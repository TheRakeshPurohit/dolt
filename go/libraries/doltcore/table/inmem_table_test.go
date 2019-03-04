package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"io"
	"testing"
)

const (
	nameTag uint64 = iota
	ageTag
	titleTag
	greatTag
)

var fields, _ = schema.NewColCollection(
	schema.Column{"name", nameTag, types.StringKind, true, nil},
	schema.Column{"age", ageTag, types.UintKind, true, nil},
	schema.Column{"title", titleTag, types.StringKind, true, nil},
	schema.Column{"is_great", greatTag, types.BoolKind, true, nil},
)

var rowSch = schema.SchemaFromCols(fields)
var rows = []row.Row{
	row.New(rowSch, row.TaggedValues{
		nameTag:  types.String("Bill Billerson"),
		ageTag:   types.Uint(32),
		titleTag: types.String("Senior Dufus"),
		greatTag: types.Bool(true),
	}),
	row.New(rowSch, row.TaggedValues{
		nameTag:  types.String("Rob Robertson"),
		ageTag:   types.Uint(25),
		titleTag: types.String("Dufus"),
		greatTag: types.Bool(false),
	}),
	row.New(rowSch, row.TaggedValues{
		nameTag:  types.String("John Johnson"),
		ageTag:   types.Uint(21),
		titleTag: types.String("Intern Dufus"),
		greatTag: types.Bool(true),
	}),
}

func TestInMemTable(t *testing.T) {
	imt := NewInMemTable(rowSch)

	func() {
		var wr TableWriteCloser
		wr = NewInMemTableWriter(imt)
		defer wr.Close()

		for _, row := range rows {
			err := wr.WriteRow(row)

			if err != nil {
				t.Fatal("Failed to write row")
			}
		}
	}()

	func() {
		var r TableReadCloser
		r = NewInMemTableReader(imt)
		defer r.Close()

		for _, expectedRow := range rows {
			actualRow, err := r.ReadRow()

			if err != nil {
				t.Error("Unexpected read error")
			} else if !row.AreEqual(expectedRow, actualRow, rowSch) {
				t.Error("Unexpected row value")
			}
		}

		_, err := r.ReadRow()

		if err != io.EOF {
			t.Error("Should have reached the end.")
		}
	}()
}

func TestPipeRows(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)
	imtt2 := NewInMemTable(rowSch)

	var err error
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close()
		wr := NewInMemTableWriter(imtt2)
		defer wr.Close()
		_, _, err = PipeRows(rd, wr, false)
	}()

	if err != nil {
		t.Error("Error piping rows from reader to writer", err)
	}

	if imt.NumRows() != imtt2.NumRows() {
		t.Error("Row counts should match")
	}

	for i := 0; i < imt.NumRows(); i++ {
		r1, err1 := imt.GetRow(i)
		r2, err2 := imtt2.GetRow(i)

		if err1 != nil || err2 != nil {
			t.Fatal("Couldn't Get row.")
		}

		if !row.AreEqual(r1, r2, rowSch) {
			t.Error("Rows sholud be the same.", row.Fmt(r1, rowSch), "!=", row.Fmt(r2, rowSch))
		}
	}
}

func TestReadAllRows(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)

	var err error
	var numBad int
	var results []row.Row
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close()
		results, numBad, err = ReadAllRows(rd, true)
	}()

	if err != nil {
		t.Fatal("Error reading rows")
	}

	if len(rows) != len(results) {
		t.Error("Unexpected count.")
	}

	if numBad != 0 {
		t.Error("Unexpected BadRow Count")
	}

	for i := 0; i < len(rows); i++ {
		if !row.AreEqual(rows[i], results[i], rowSch) {
			t.Error(row.Fmt(rows[i], rowSch), "!=", row.Fmt(results[i], rowSch))
		}
	}
}

/*
func TestReadAllRowsToMap(t *testing.T) {
	imt := NewInMemTableWithData(rowSch, rows)
	greatIndex := rowSch.GetFieldIndex("is_great")

	var err error
	var numBad int
	var results map[types.Value][]row.Row
	func() {
		rd := NewInMemTableReader(imt)
		defer rd.Close()
		results, numBad, err = ReadAllRowsToMap(rd, greatIndex, true)
	}()

	if err != nil {
		t.Fatal("Error reading rows")
	}

	if numBad != 0 {
		t.Error("Unexpected BadRow Count")
	}

	if len(results) != 2 {
		t.Error("Unexpected count.")
	}

	if len(results[types.Bool(true)]) != 2 || len(results[types.Bool(false)]) != 1 {
		t.Error("Unexpected count for one or more values of is_great")
	}

	for _, great := range []types.Bool{types.Bool(true), types.Bool(false)} {
		for i, j := 0, 0; i < len(rows); i++ {
			rowIsGreat, _ := rows[i].CurrData().GetField(greatIndex)

			if rowIsGreat == great {
				if !RowsEqualIgnoringSchema(rows[i], results[great][j]) {
					t.Error(RowFmt(rows[i]), "!=", RowFmt(results[great][j]))
				}
				j++
			}
		}
	}
}
*/
