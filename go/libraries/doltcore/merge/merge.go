// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")
var ErrTableDeletedAndModified = errors.New("conflict: table with same name deleted and modified ")

// ErrCantOverwriteConflicts is returned when there are unresolved conflicts
// and the merge produces new conflicts. Because we currently don't have a model
// to merge sets of conflicts together, we need to abort the merge at this
// point.
var ErrCantOverwriteConflicts = errors.New("existing unresolved conflicts would be" +
	" overridden by new conflicts produced by merge. Please resolve them and try again")

var ErrConflictsIncompatible = errors.New("the existing conflicts are of a different schema" +
	" than the conflicts generated by this merge. Please resolve them and try again")

type Merger struct {
	theirRootIsh hash.Hash
	ancRootIsh   hash.Hash
	root         *doltdb.RootValue
	mergeRoot    *doltdb.RootValue
	ancRoot      *doltdb.RootValue
	vrw          types.ValueReadWriter
}

// NewMerger creates a new merger utility object.
func NewMerger(ctx context.Context, theirRootIsh, ancRootIsh hash.Hash, root, mergeRoot, ancRoot *doltdb.RootValue, vrw types.ValueReadWriter) *Merger {
	return &Merger{theirRootIsh, ancRootIsh, root, mergeRoot, ancRoot, vrw}
}

// MergeTable merges schema and table data for the table tblName.
func (merger *Merger) MergeTable(ctx context.Context, tblName string, opts editor.Options, isCherryPick bool) (*doltdb.Table, *MergeStats, error) {
	rootHasTable, tbl, rootSchema, rootHash, err := getTableInfoFromRoot(ctx, tblName, merger.root)
	if err != nil {
		return nil, nil, err
	}

	mergeHasTable, mergeTbl, mergeSchema, mergeHash, err := getTableInfoFromRoot(ctx, tblName, merger.mergeRoot)
	if err != nil {
		return nil, nil, err
	}

	ancHasTable, ancTbl, ancSchema, ancHash, err := getTableInfoFromRoot(ctx, tblName, merger.ancRoot)
	if err != nil {
		return nil, nil, err
	}

	var ancRows durable.Index
	// only used by new storage format
	var ancIndexSet durable.IndexSet
	if ancHasTable {
		ancRows, err = ancTbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}
		ancIndexSet, err = ancTbl.GetIndexSet(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	{ // short-circuit logic

		// Nothing changed
		if rootHasTable && mergeHasTable && ancHasTable && rootHash == mergeHash && rootHash == ancHash {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// Both made identical changes
		// For keyless tables, this counts as a conflict
		if rootHasTable && mergeHasTable && rootHash == mergeHash && !schema.IsKeyless(rootSchema) {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// One or both added this table
		if !ancHasTable {
			if mergeHasTable && rootHasTable {
				if schema.SchemasAreEqual(rootSchema, mergeSchema) {
					// If both added the same table, pretend it was in the ancestor all along with no data
					// Don't touch ancHash to avoid triggering other short-circuit logic below
					ancHasTable, ancSchema, ancTbl = true, rootSchema, tbl
					ancRows, err = durable.NewEmptyIndex(ctx, merger.vrw, ancSchema)
					if err != nil {
						return nil, nil, err
					}
					ancIndexSet, err = durable.NewIndexSetWithEmptyIndexes(ctx, merger.vrw, ancSchema)
					if err != nil {
						return nil, nil, err
					}
				} else {
					return nil, nil, ErrSameTblAddedTwice
				}
			} else if rootHasTable {
				// fast-forward
				return tbl, &MergeStats{Operation: TableUnmodified}, nil
			} else {
				// fast-forward
				return mergeTbl, &MergeStats{Operation: TableAdded}, nil
			}
		}

		// Deleted in both, fast-forward
		if ancHasTable && !rootHasTable && !mergeHasTable {
			return nil, &MergeStats{Operation: TableRemoved}, nil
		}

		// Deleted in root or in merge, either a conflict (if any changes in other root) or else a fast-forward
		if ancHasTable && (!rootHasTable || !mergeHasTable) {
			if isCherryPick && rootHasTable && !mergeHasTable {
				// TODO : this is either drop table or rename table case
				// We can delete only if the table in current HEAD and parent commit contents are exact the same (same schema and same data);
				// otherwise, return ErrTableDeletedAndModified
				// We need to track renaming of a table --> the renamed table could be added as new table
				return nil, &MergeStats{Operation: TableModified}, errors.New(fmt.Sprintf("schema changes not supported: %s table was renamed or dropped in cherry-pick commit", tblName))
			}

			if (mergeHasTable && mergeHash != ancHash) ||
				(rootHasTable && rootHash != ancHash) {
				return nil, nil, ErrTableDeletedAndModified
			}
			// fast-forward
			return nil, &MergeStats{Operation: TableRemoved}, nil
		}

		// Changes only in root, table unmodified
		if mergeHash == ancHash {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// Changes only in merge root, fast-forward
		// TODO : no fast-forward when cherry-picking for now
		if !isCherryPick && rootHash == ancHash {
			ms := MergeStats{Operation: TableModified}
			if rootHash != mergeHash {
				ms, err = calcTableMergeStats(ctx, tbl, mergeTbl)
				if err != nil {
					return nil, nil, err
				}
			}
			return mergeTbl, &ms, nil
		}
	}

	if isCherryPick && !schema.SchemasAreEqual(rootSchema, mergeSchema) {
		return nil, nil, errors.New(fmt.Sprintf("schema changes not supported: %s table schema does not match in current HEAD and cherry-pick commit.", tblName))
	}

	postMergeSchema, schConflicts, err := SchemaMerge(rootSchema, mergeSchema, ancSchema, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() != 0 {
		// error on schema conflicts for now
		return nil, nil, schConflicts.AsError()
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, postMergeSchema)
	if err != nil {
		return nil, nil, err
	}

	if types.IsFormat_DOLT_1(updatedTbl.Format()) {
		updatedTbl, err = mergeTableArtifacts(ctx, tbl, mergeTbl, ancTbl, updatedTbl)
		if err != nil {
			return nil, nil, err
		}

		var stats *MergeStats
		updatedTbl, stats, err = mergeTableData(
			ctx,
			merger.vrw,
			tblName,
			postMergeSchema, rootSchema, mergeSchema, ancSchema,
			tbl, mergeTbl, updatedTbl,
			ancRows,
			ancIndexSet,
			merger.theirRootIsh,
			merger.ancRootIsh)
		if err != nil {
			return nil, nil, err
		}

		updatedTbl, err = mergeAutoIncrementValues(ctx, tbl, mergeTbl, updatedTbl)
		if err != nil {
			return nil, nil, err
		}
		return updatedTbl, stats, nil
	}

	// If any indexes were added during the merge, then we need to generate their row data to add to our updated table.
	addedIndexesSet := make(map[string]string)
	for _, index := range postMergeSchema.Indexes().AllIndexes() {
		addedIndexesSet[strings.ToLower(index.Name())] = index.Name()
	}
	for _, index := range rootSchema.Indexes().AllIndexes() {
		delete(addedIndexesSet, strings.ToLower(index.Name()))
	}
	for _, addedIndex := range addedIndexesSet {
		newIndexData, err := editor.RebuildIndex(ctx, updatedTbl, addedIndex, opts)
		if err != nil {
			return nil, nil, err
		}
		updatedTbl, err = updatedTbl.SetNomsIndexRows(ctx, addedIndex, newIndexData)
		if err != nil {
			return nil, nil, err
		}
	}

	updatedTblEditor, err := editor.NewTableEditor(ctx, updatedTbl, postMergeSchema, tblName, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeRows, err := mergeTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	resultTbl, cons, stats, err := mergeNomsTableData(ctx, merger.vrw, tblName, postMergeSchema, rows, mergeRows, durable.NomsMapFromIndex(ancRows), updatedTblEditor)
	if err != nil {
		return nil, nil, err
	}

	if cons.Len() > 0 {
		resultTbl, err = setConflicts(ctx, durable.ConflictIndexFromNomsMap(cons, merger.vrw), tbl, mergeTbl, ancTbl, resultTbl)
		if err != nil {
			return nil, nil, err
		}
	}

	resultTbl, err = mergeAutoIncrementValues(ctx, tbl, mergeTbl, resultTbl)
	if err != nil {
		return nil, nil, err
	}

	return resultTbl, stats, nil
}

func setConflicts(ctx context.Context, cons durable.ConflictIndex, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	ancSch, err := ancTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	cs := conflict.NewConflictSchema(ancSch, sch, mergeSch)

	tableToUpdate, err = tableToUpdate.SetConflicts(ctx, cs, cons)
	if err != nil {
		return nil, err
	}

	return tableToUpdate, nil
}

func getTableInfoFromRoot(ctx context.Context, tblName string, root *doltdb.RootValue) (
	ok bool,
	table *doltdb.Table,
	sch schema.Schema,
	h hash.Hash,
	err error,
) {
	table, ok, err = root.GetTable(ctx, tblName)
	if err != nil {
		return false, nil, nil, hash.Hash{}, err
	}

	if ok {
		h, err = table.HashOf()
		if err != nil {
			return false, nil, nil, hash.Hash{}, err
		}
		sch, err = table.GetSchema(ctx)
		if err != nil {
			return false, nil, nil, hash.Hash{}, err
		}
	}

	return ok, table, sch, h, nil
}

func calcTableMergeStats(ctx context.Context, tbl *doltdb.Table, mergeTbl *doltdb.Table) (MergeStats, error) {
	ms := MergeStats{Operation: TableModified}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.Summary(ctx, ch, rows, mergeRows, sch, mergeSch)

		ae.SetIfError(err)
	}()

	for p := range ch {
		if ae.IsSet() {
			break
		}

		ms.Adds += int(p.Adds)
		ms.Deletes += int(p.Removes)
		ms.Modifications += int(p.Changes)
	}

	if err := ae.Get(); err != nil {
		return MergeStats{}, err
	}

	return ms, nil
}

type rowMergeResult struct {
	mergedRow    types.Value
	didCellMerge bool
	isConflict   bool
}

type rowMerger func(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (rowMergeResult, error)

type applicator func(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error

func mergeNomsTableData(ctx context.Context, vrw types.ValueReadWriter, tblName string, sch schema.Schema, rows, mergeRows, ancRows types.Map, tblEdit editor.TableEditor) (*doltdb.Table, types.Map, *MergeStats, error) {
	var rowMerge rowMerger
	var applyChange applicator
	if schema.IsKeyless(sch) {
		rowMerge = keylessRowMerge
		applyChange = applyKeylessChange
	} else {
		rowMerge = nomsPkRowMerge
		applyChange = applyNomsPkChange
	}

	changeChan, mergeChangeChan := make(chan types.ValueChanged, 32), make(chan types.ValueChanged, 32)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer close(changeChan)
		return rows.Diff(ctx, ancRows, changeChan)
	})
	eg.Go(func() error {
		defer close(mergeChangeChan)
		return mergeRows.Diff(ctx, ancRows, mergeChangeChan)
	})

	conflictValChan := make(chan types.Value)
	sm := types.NewStreamingMap(ctx, vrw, conflictValChan)
	stats := &MergeStats{Operation: TableModified}

	eg.Go(func() error {
		defer close(conflictValChan)

		var change, mergeChange types.ValueChanged
		for {
			// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is
			// complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value.
			// Generally, though, this allows us to proceed through both diffs in (key) order, considering
			// the "current" change from both diffs at the same time.
			if change.Key == nil {
				select {
				case change = <-changeChan:
					break
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if mergeChange.Key == nil {
				select {
				case mergeChange = <-mergeChangeChan:
					break
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			key, mergeKey := change.Key, mergeChange.Key

			// Both channels are producing zero values, so we're done.
			if key == nil && mergeKey == nil {
				break
			}

			var err error
			var processed bool
			if key != nil {
				mkNilOrKeyLess := mergeKey == nil
				if !mkNilOrKeyLess {
					mkNilOrKeyLess, err = key.Less(vrw.Format(), mergeKey)
					if err != nil {
						return err
					}
				}

				if mkNilOrKeyLess {
					// change will already be in the map
					// we apply changes directly to "ours"
					// instead of to ancestor
					change = types.ValueChanged{}
					processed = true
				}
			}

			if !processed && mergeKey != nil {
				keyNilOrMKLess := key == nil
				if !keyNilOrMKLess {
					keyNilOrMKLess, err = mergeKey.Less(vrw.Format(), key)
					if err != nil {
						return err
					}
				}

				if keyNilOrMKLess {
					err = applyChange(ctx, sch, tblEdit, rows, stats, mergeChange)
					if err != nil {
						return err
					}
					mergeChange = types.ValueChanged{}
					processed = true
				}
			}

			if !processed {
				r, mergeRow, ancRow := change.NewValue, mergeChange.NewValue, change.OldValue
				rowMergeResult, err := rowMerge(ctx, vrw.Format(), sch, r, mergeRow, ancRow)
				if err != nil {
					return err
				}

				if rowMergeResult.isConflict {
					conflictTuple, err := conflict.NewConflict(ancRow, r, mergeRow).ToNomsList(vrw)
					if err != nil {
						return err
					}

					err = addConflict(conflictValChan, sm.Done(), key, conflictTuple)
					if err != nil {
						return err
					}
				} else {
					vc := types.ValueChanged{ChangeType: change.ChangeType, Key: key, NewValue: rowMergeResult.mergedRow}
					if rowMergeResult.didCellMerge {
						vc.OldValue = r
					} else {
						vc.OldValue = ancRow
					}
					err = applyChange(ctx, sch, tblEdit, rows, stats, vc)
					if err != nil {
						return err
					}
				}

				change = types.ValueChanged{}
				mergeChange = types.ValueChanged{}
			}
		}

		return nil
	})

	var conflicts types.Map
	eg.Go(func() error {
		var err error
		// |sm|'s errgroup is a child of |eg|
		// so we must wait here, before |eg| finishes
		conflicts, err = sm.Wait()
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, types.EmptyMap, nil, err
	}

	mergedTable, err := tblEdit.Table(ctx)
	if err != nil {
		return nil, types.EmptyMap, nil, err
	}

	return mergedTable, conflicts, stats, nil
}

func addConflict(conflictChan chan types.Value, done <-chan struct{}, key types.Value, value types.Tuple) error {
	select {
	case conflictChan <- key:
	case <-done:
		return context.Canceled
	}
	select {
	case conflictChan <- value:
	case <-done:
		return context.Canceled
	}
	return nil
}

func applyNomsPkChange(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		newRow, err := row.FromNoms(sch, change.Key.(types.Tuple), change.NewValue.(types.Tuple))
		if err != nil {
			return err
		}
		// TODO(andy): because we apply changes to "ours" instead of ancestor
		// we have to check for duplicate primary key errors here.
		val, ok, err := rowData.MaybeGet(ctx, change.Key)
		if err != nil {
			return err
		} else if ok {
			oldRow, err := row.FromNoms(sch, change.Key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.UpdateRow(ctx, oldRow, newRow, makeDupErrHandler(ctx, tableEditor, change.Key.(types.Tuple), change.NewValue.(types.Tuple)))
			if err != nil {
				if err != nil {
					return err
				}
			}
		} else {
			err = tableEditor.InsertRow(ctx, newRow, makeDupErrHandler(ctx, tableEditor, change.Key.(types.Tuple), change.NewValue.(types.Tuple)))
			if err != nil {
				if err != nil {
					return err
				}
			}
		}
		stats.Adds++
	case types.DiffChangeModified:
		key, oldVal, newVal := change.Key.(types.Tuple), change.OldValue.(types.Tuple), change.NewValue.(types.Tuple)
		oldRow, err := row.FromNoms(sch, key, oldVal)
		if err != nil {
			return err
		}
		newRow, err := row.FromNoms(sch, key, newVal)
		if err != nil {
			return err
		}
		err = tableEditor.UpdateRow(ctx, oldRow, newRow, makeDupErrHandler(ctx, tableEditor, key, newVal))
		if err != nil {
			if err != nil {
				return err
			}
		}
		stats.Modifications++
	case types.DiffChangeRemoved:
		key := change.Key.(types.Tuple)
		value := change.OldValue.(types.Tuple)
		tv, err := row.TaggedValuesFromTupleKeyAndValue(key, value)
		if err != nil {
			return err
		}

		err = tableEditor.DeleteByKey(ctx, key, tv)
		if err != nil {
			return err
		}

		stats.Deletes++
	}

	return nil
}

func makeDupErrHandler(ctx context.Context, tableEditor editor.TableEditor, newKey, newValue types.Tuple) editor.PKDuplicateErrFunc {
	return func(keyString, indexName string, existingKey, existingValue types.Tuple, isPk bool) error {
		if isPk {
			return fmt.Errorf("duplicate key '%s'", keyString)
		}

		sch := tableEditor.Schema()
		idx := sch.Indexes().GetByName(indexName)
		m, err := makeUniqViolMeta(sch, idx)
		if err != nil {
			return err
		}
		d, err := json.Marshal(m)
		if err != nil {
			return err
		}

		return addUniqueViolation(ctx, tableEditor, existingKey, existingValue, newKey, newValue, d)
	}
}

func addUniqueViolation(ctx context.Context, tableEditor editor.TableEditor, existingKey, existingVal, newKey, newVal types.Tuple, jsonData []byte) error {
	nomsJson, err := jsonDataToNomsValue(ctx, tableEditor.ValueReadWriter(), jsonData)
	if err != nil {
		return err
	}
	cvKey, cvVal, err := toConstraintViolationRow(ctx, CvType_UniqueIndex, nomsJson, newKey, newVal)
	if err != nil {
		return err
	}
	err = tableEditor.SetConstraintViolation(ctx, cvKey, cvVal)
	if err != nil {
		return err
	}

	cvKey, cvVal, err = toConstraintViolationRow(ctx, CvType_UniqueIndex, nomsJson, existingKey, existingVal)
	if err != nil {
		return err
	}
	err = tableEditor.SetConstraintViolation(ctx, cvKey, cvVal)
	if err != nil {
		return err
	}

	return nil
}

func applyKeylessChange(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, _ types.Map, stats *MergeStats, change types.ValueChanged) (err error) {
	apply := func(ch types.ValueChanged) error {
		switch ch.ChangeType {
		case types.DiffChangeAdded:
			newRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.NewValue.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.InsertRow(ctx, newRow, nil)
			if err != nil {
				return err
			}
			stats.Adds++
		case types.DiffChangeModified:
			oldRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.OldValue.(types.Tuple))
			if err != nil {
				return err
			}
			newRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.NewValue.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.UpdateRow(ctx, oldRow, newRow, nil)
			if err != nil {
				return err
			}
			stats.Modifications++
		case types.DiffChangeRemoved:
			key := change.Key.(types.Tuple)
			value := change.OldValue.(types.Tuple)
			tv, err := row.TaggedValuesFromTupleKeyAndValue(key, value)
			if err != nil {
				return err
			}

			err = tableEditor.DeleteByKey(ctx, key, tv)
			if err != nil {
				return err
			}

			stats.Deletes++
		}
		return nil
	}

	var card uint64
	change, card, err = convertValueChanged(change)
	if err != nil {
		return err
	}

	for card > 0 {
		if err = apply(change); err != nil {
			return err
		}
		card--
	}
	return nil
}

func convertValueChanged(vc types.ValueChanged) (types.ValueChanged, uint64, error) {
	var oldCard uint64
	if vc.OldValue != nil {
		v, err := vc.OldValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return vc, 0, err
		}
		oldCard = uint64(v.(types.Uint))
	}

	var newCard uint64
	if vc.NewValue != nil {
		v, err := vc.NewValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return vc, 0, err
		}
		newCard = uint64(v.(types.Uint))
	}

	switch vc.ChangeType {
	case types.DiffChangeRemoved:
		return vc, oldCard, nil

	case types.DiffChangeAdded:
		return vc, newCard, nil

	case types.DiffChangeModified:
		delta := int64(newCard) - int64(oldCard)
		if delta > 0 {
			vc.ChangeType = types.DiffChangeAdded
			vc.OldValue = nil
			return vc, uint64(delta), nil
		} else if delta < 0 {
			vc.ChangeType = types.DiffChangeRemoved
			vc.NewValue = nil
			return vc, uint64(-delta), nil
		} else {
			panic(fmt.Sprintf("diff with delta = 0 for key: %s", vc.Key.HumanReadableString()))
		}
	default:
		return vc, 0, fmt.Errorf("unexpected DiffChange type %d", vc.ChangeType)
	}
}

// pkRowMerge returns the merged value, if a cell-wise merge was performed, and whether a conflict occurred
func nomsPkRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (rowMergeResult, error) {
	var baseVals row.TaggedValues
	if baseRow == nil {
		if r.Equals(mergeRow) {
			// same row added to both
			return rowMergeResult{r, false, false}, nil
		}
	} else if r == nil && mergeRow == nil {
		// same row removed from both
		return rowMergeResult{nil, false, false}, nil
	} else if r == nil || mergeRow == nil {
		// removed from one and modified in another
		return rowMergeResult{nil, false, true}, nil
	} else {
		var err error
		baseVals, err = row.ParseTaggedValues(baseRow.(types.Tuple))

		if err != nil {
			return rowMergeResult{}, err
		}
	}

	rowVals, err := row.ParseTaggedValues(r.(types.Tuple))
	if err != nil {
		return rowMergeResult{}, err
	}

	mergeVals, err := row.ParseTaggedValues(mergeRow.(types.Tuple))
	if err != nil {
		return rowMergeResult{}, err
	}

	var didMerge bool
	processTagFunc := func(tag uint64) (resultVal types.Value, isConflict bool) {
		baseVal, _ := baseVals.Get(tag)
		val, _ := rowVals.Get(tag)
		mergeVal, _ := mergeVals.Get(tag)

		if valutil.NilSafeEqCheck(val, mergeVal) {
			return val, false
		} else {
			modified := !valutil.NilSafeEqCheck(val, baseVal)
			mergeModified := !valutil.NilSafeEqCheck(mergeVal, baseVal)
			switch {
			case modified && mergeModified:
				return nil, true
			case modified:
				didMerge = true
				return val, false
			default:
				didMerge = true
				return mergeVal, false
			}
		}

	}

	resultVals := make(row.TaggedValues)

	var isConflict bool
	err = sch.GetNonPKCols().Iter(func(tag uint64, _ schema.Column) (stop bool, err error) {
		var val types.Value
		val, isConflict = processTagFunc(tag)
		resultVals[tag] = val

		return isConflict, nil
	})

	if err != nil {
		return rowMergeResult{}, err
	}

	if isConflict {
		return rowMergeResult{nil, false, true}, nil
	}

	tpl := resultVals.NomsTupleForNonPKCols(nbf, sch.GetNonPKCols())
	v, err := tpl.Value(ctx)

	if err != nil {
		return rowMergeResult{}, err
	}

	return rowMergeResult{v, didMerge, false}, nil
}

func keylessRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, val, mergeVal, ancVal types.Value) (rowMergeResult, error) {
	// both sides of the merge produced a diff for this key,
	// so we always throw a conflict
	return rowMergeResult{nil, false, true}, nil
}

func mergeAutoIncrementValues(ctx context.Context, tbl, otherTbl, resultTbl *doltdb.Table) (*doltdb.Table, error) {
	// only need to check one table, no PK changes yet
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	if !schema.HasAutoIncrement(sch) {
		return resultTbl, nil
	}

	autoVal, err := tbl.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	mergeAutoVal, err := otherTbl.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	if autoVal < mergeAutoVal {
		autoVal = mergeAutoVal
	}
	return resultTbl.SetAutoIncrementValue(ctx, autoVal)
}

func MergeCommits(ctx context.Context, commit, mergeCommit *doltdb.Commit, opts editor.Options) (*doltdb.RootValue, map[string]*MergeStats, error) {
	ancCommit, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)
	if err != nil {
		return nil, nil, err
	}

	ourRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancCmHash, err := ancCommit.HashOf()
	if err != nil {
		return nil, nil, err
	}

	theirCmHash, err := mergeCommit.HashOf()
	if err != nil {
		return nil, nil, err
	}

	theirRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancRoot, err := ancCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	return MergeRoots(ctx, theirCmHash, ancCmHash, ourRoot, theirRoot, ancRoot, opts, false)
}

// MergeRoots three-way merges |ourRoot|, |theirRoot|, and |ancRoot| and returns
// the merged root. If any conflicts or constraint violations are produced they
// are stored in the merged root. If |ourRoot| already contains conflicts they
// are stashed before the merge is performed. We abort the merge if the stash
// contains conflicts and we produce new conflicts. We currently don't have a
// model to merge conflicts together.
//
// Constraint violations that exist in ancestor are stashed and merged with the
// violations we detect when we diff the ancestor and the newly merged root.
//
// |theirRootIsh| is the hash of their's working set or commit. It is used to
// key any artifacts generated by this merge. |ancRootIsh| is similar and is
// used to retrieve the base value for a conflict.
func MergeRoots(ctx context.Context, theirRootIsh, ancRootIsh hash.Hash, ourRoot, theirRoot, ancRoot *doltdb.RootValue, opts editor.Options, isCherryPick bool) (*doltdb.RootValue, map[string]*MergeStats, error) {
	var conflictStash *conflictStash
	var violationStash *violationStash
	var err error
	if !types.IsFormat_DOLT_1(ourRoot.VRW().Format()) {
		ourRoot, conflictStash, err = stashConflicts(ctx, ourRoot)
		if err != nil {
			return nil, nil, err
		}
		ancRoot, violationStash, err = stashViolations(ctx, ancRoot)
		if err != nil {
			return nil, nil, err
		}
	}

	tblNames, err := doltdb.UnionTableNames(ctx, ourRoot, theirRoot)

	if err != nil {
		return nil, nil, err
	}

	tblToStats := make(map[string]*MergeStats)

	mergedRoot := ourRoot

	optsWithFKChecks := opts
	optsWithFKChecks.ForeignKeyChecksDisabled = true

	// Merge tables one at a time. This is done based on name, so will work badly for things like table renames.
	// TODO: merge based on a more durable table identity that persists across renames
	merger := NewMerger(ctx, theirRootIsh, ancRootIsh, ourRoot, theirRoot, ancRoot, ourRoot.VRW())
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName, opts, isCherryPick)
		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats

			mergedRoot, err = mergedRoot.PutTable(ctx, tblName, mergedTable)
			if err != nil {
				return nil, nil, err
			}
			continue
		}

		newRootHasTable, err := mergedRoot.HasTable(ctx, tblName)
		if err != nil {
			return nil, nil, err
		}

		if newRootHasTable {
			// Merge root deleted this table
			tblToStats[tblName] = &MergeStats{Operation: TableRemoved}

			mergedRoot, err = mergedRoot.RemoveTables(ctx, false, false, tblName)
			if err != nil {
				return nil, nil, err
			}

		} else {
			// This is a deleted table that the merge root still has
			if stats.Operation != TableRemoved {
				panic(fmt.Sprintf("Invalid merge state for table %s. This is a bug.", tblName))
			}
			// Nothing to update, our root already has the table deleted
		}
	}

	mergedFKColl, conflicts, err := ForeignKeysMerge(ctx, mergedRoot, ourRoot, theirRoot, ancRoot)
	if err != nil {
		return nil, nil, err
	}
	if len(conflicts) > 0 {
		return nil, nil, fmt.Errorf("foreign key conflicts")
	}

	mergedRoot, err = mergedRoot.PutForeignKeyCollection(ctx, mergedFKColl)
	if err != nil {
		return nil, nil, err
	}

	mergedRoot, err = mergedRoot.UpdateSuperSchemasFromOther(ctx, tblNames, theirRoot)
	if err != nil {
		return nil, nil, err
	}

	mergedRoot, _, err = AddForeignKeyViolations(ctx, mergedRoot, ancRoot, nil, theirRootIsh)
	if err != nil {
		return nil, nil, err
	}

	if types.IsFormat_DOLT_1(ourRoot.VRW().Format()) {
		err = getConflictAndConstraintViolationStats(ctx, mergedRoot, tblToStats)
		if err != nil {
			return nil, nil, err
		}

		return mergedRoot, tblToStats, nil
	}

	mergedRoot, err = mergeCVsWithStash(ctx, mergedRoot, violationStash)
	if err != nil {
		return nil, nil, err
	}

	err = getConflictAndConstraintViolationStats(ctx, mergedRoot, tblToStats)
	if err != nil {
		return nil, nil, err
	}

	mergedHasConflicts := checkForConflicts(tblToStats)
	if !conflictStash.Empty() && mergedHasConflicts {
		return nil, nil, ErrCantOverwriteConflicts
	} else if !conflictStash.Empty() {
		mergedRoot, err = applyConflictStash(ctx, conflictStash.Stash, mergedRoot)
		if err != nil {
			return nil, nil, err
		}
	}

	return mergedRoot, tblToStats, nil
}

// mergeCVsWithStash merges the table constraint violations in |stash| with |root|.
// Returns an updated root with all the merged CVs.
func mergeCVsWithStash(ctx context.Context, root *doltdb.RootValue, stash *violationStash) (*doltdb.RootValue, error) {
	updatedRoot := root
	for name, stashed := range stash.Stash {
		tbl, ok, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			// the table with the CVs was deleted
			continue
		}
		curr, err := tbl.GetConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		unioned, err := types.UnionMaps(ctx, curr, stashed, func(key types.Value, currV types.Value, stashV types.Value) (types.Value, error) {
			if !currV.Equals(stashV) {
				panic(fmt.Sprintf("encountered conflict when merging constraint violations, conflicted key: %v\ncurrent value: %v\nstashed value: %v\n", key, currV, stashV))
			}
			return currV, nil
		})
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.SetConstraintViolations(ctx, unioned)
		if err != nil {
			return nil, err
		}
		updatedRoot, err = root.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}
	return updatedRoot, nil
}

// checks if a conflict occurred during the merge
func checkForConflicts(tblToStats map[string]*MergeStats) bool {
	for _, stat := range tblToStats {
		if stat.Conflicts > 0 {
			return true
		}
	}
	return false
}

// populates tblToStats with violation statistics
func getConflictAndConstraintViolationStats(ctx context.Context, root *doltdb.RootValue, tblToStats map[string]*MergeStats) error {
	for tblName, stats := range tblToStats {
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if ok {
			n, err := tbl.NumRowsInConflict(ctx)
			if err != nil {
				return err
			}
			stats.Conflicts = int(n)

			n2, err := tbl.NumConstraintViolations(ctx)
			if err != nil {
				return err
			}
			stats.ConstraintViolations = int(n2)
		}
	}
	return nil
}

// MayHaveConstraintViolations returns whether the given roots may have constraint violations. For example, a fast
// forward merge that does not involve any tables with foreign key constraints or check constraints will not be able
// to generate constraint violations. Unique key constraint violations would be caught during the generation of the
// merged root, therefore it is not a factor for this function.
func MayHaveConstraintViolations(ctx context.Context, ancestor, merged *doltdb.RootValue) (bool, error) {
	ancTables, err := ancestor.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	mergedTables, err := merged.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	fkColl, err := merged.GetForeignKeyCollection(ctx)
	if err != nil {
		return false, err
	}
	tablesInFks := fkColl.Tables()
	for tblName := range tablesInFks {
		if ancHash, ok := ancTables[tblName]; !ok {
			// If a table used in a foreign key is new then it's treated as a change
			return true, nil
		} else if mergedHash, ok := mergedTables[tblName]; !ok {
			return false, fmt.Errorf("foreign key uses table '%s' but no hash can be found for this table", tblName)
		} else if !ancHash.Equal(mergedHash) {
			return true, nil
		}
	}
	return false, nil
}

func GetTablesInConflict(ctx context.Context, roots doltdb.Roots) (
	workingInConflict, stagedInConflict, headInConflict []string,
	err error,
) {
	headInConflict, err = roots.Head.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedInConflict, err = roots.Staged.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingInConflict, err = roots.Working.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingInConflict, stagedInConflict, headInConflict, err
}

func GetTablesWithConstraintViolations(ctx context.Context, roots doltdb.Roots) (
	workingViolations, stagedViolations, headViolations []string,
	err error,
) {
	headViolations, err = roots.Head.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedViolations, err = roots.Staged.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingViolations, err = roots.Working.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingViolations, stagedViolations, headViolations, err
}

func GetDocsInConflict(ctx context.Context, workingRoot *doltdb.RootValue, docs doltdocs.Docs) (*diff.DocDiffs, error) {
	return diff.NewDocDiffs(ctx, workingRoot, nil, docs)
}

func MergeWouldStompChanges(ctx context.Context, roots doltdb.Roots, mergeCommit *doltdb.Commit) ([]string, map[string]hash.Hash, error) {
	mergeRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := roots.Head.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := roots.Working.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mergeRoot.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergedHeadDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergedHeadDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}
