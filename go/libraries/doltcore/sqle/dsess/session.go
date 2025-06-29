// Copyright 2020 Dolthub, Inc.
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

package dsess

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	DbRevisionDelimiter = "/"
)

var ErrSessionNotPersistable = errors.New("session is not persistable")

// DoltSession is the sql.Session implementation used by dolt. It is accessible through a *sql.Context instance
type DoltSession struct {
	sql.Session
	DoltgresSessObj       any   // This is used by Doltgres to persist objects in the session. This is not used by Dolt.
	notices               []any // This is used by Doltgres to store notices. This is not used by Dolt.
	username              string
	email                 string
	dbStates              map[string]*DatabaseSessionState
	dbCache               *DatabaseCache
	provider              DoltDatabaseProvider
	tempTables            map[string][]sql.Table
	globalsConf           config.ReadWriteConfig
	branchController      *branch_control.Controller
	statsProv             sql.StatsProvider
	mu                    *sync.Mutex
	fs                    filesys.Filesys
	writeSessProv         WriteSessFunc
	gcSafepointController *gcctx.GCSafepointController

	// If non-nil, this will be returned from ValidateSession.
	// Used by sqle/cluster to put a session into a terminal err state.
	validateErr error
}

var _ sql.Session = (*DoltSession)(nil)
var _ sql.PersistableSession = (*DoltSession)(nil)
var _ sql.TransactionSession = (*DoltSession)(nil)
var _ branch_control.ContextConvertible = (*DoltSession)(nil)

// DefaultSession creates a DoltSession with default values
func DefaultSession(pro DoltDatabaseProvider, sessFunc WriteSessFunc) *DoltSession {
	return &DoltSession{
		Session:          sql.NewBaseSession(),
		username:         "",
		email:            "",
		dbStates:         make(map[string]*DatabaseSessionState),
		dbCache:          newDatabaseCache(),
		provider:         pro,
		tempTables:       make(map[string][]sql.Table),
		globalsConf:      config.NewMapConfig(make(map[string]string)),
		branchController: branch_control.CreateDefaultController(context.TODO()), // Default sessions are fine with the default controller
		mu:               &sync.Mutex{},
		fs:               pro.FileSystem(),
		writeSessProv:    sessFunc,
	}
}

// NewDoltSession creates a DoltSession object from a standard sql.Session and 0 or more Database objects.
func NewDoltSession(
	sqlSess *sql.BaseSession,
	pro DoltDatabaseProvider,
	conf config.ReadWriteConfig,
	branchController *branch_control.Controller,
	statsProvider sql.StatsProvider,
	writeSessProv WriteSessFunc,
	gcSafepointController *gcctx.GCSafepointController,
) (*DoltSession, error) {
	username := conf.GetStringOrDefault(config.UserNameKey, "")
	email := conf.GetStringOrDefault(config.UserEmailKey, "")
	globals := config.NewPrefixConfig(conf, env.SqlServerGlobalsPrefix)

	sess := &DoltSession{
		Session:               sqlSess,
		username:              username,
		email:                 email,
		dbStates:              make(map[string]*DatabaseSessionState),
		dbCache:               newDatabaseCache(),
		provider:              pro,
		tempTables:            make(map[string][]sql.Table),
		globalsConf:           globals,
		branchController:      branchController,
		statsProv:             statsProvider,
		mu:                    &sync.Mutex{},
		fs:                    pro.FileSystem(),
		writeSessProv:         writeSessProv,
		gcSafepointController: gcSafepointController,
	}

	return sess, nil
}

// Provider returns the RevisionDatabaseProvider for this session.
func (d *DoltSession) Provider() DoltDatabaseProvider {
	return d.provider
}

// StatsProvider returns the sql.StatsProvider for this session.
func (d *DoltSession) StatsProvider() sql.StatsProvider {
	return d.statsProv
}

// DSessFromSess retrieves a dolt session from a standard sql.Session
func DSessFromSess(sess sql.Session) *DoltSession {
	return sess.(*DoltSession)
}

// lookupDbState is the private version of LookupDbState, returning a struct that has more information available than
// the interface returned by the public method.
func (d *DoltSession) lookupDbState(ctx *sql.Context, dbName string) (*branchState, bool, error) {
	dbName = strings.ToLower(dbName)

	var baseName, rev string
	baseName, rev = SplitRevisionDbName(dbName)

	d.mu.Lock()
	dbState, dbStateFound := d.dbStates[baseName]
	d.mu.Unlock()

	if dbStateFound {
		// If we got an unqualified name, use the current working set head
		if rev == "" {
			rev = dbState.checkedOutRevSpec
		}

		branchState, ok := dbState.heads[strings.ToLower(rev)]

		if ok {
			if dbState.Err != nil {
				return nil, false, dbState.Err
			}

			return branchState, ok, nil
		}
	}

	// No state for this db / branch combination yet, look it up from the provider. We use the unqualified DB name (no
	// branch) if the current DB has not yet been loaded into this session. It will resolve to that DB's default branch
	// in that case.
	revisionQualifiedName := dbName
	if rev != "" {
		revisionQualifiedName = RevisionDbName(baseName, rev)
	}

	database, ok, err := d.provider.SessionDatabase(ctx, revisionQualifiedName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// Add the initial state to the session for future reuse
	if err := d.addDB(ctx, database); err != nil {
		return nil, false, err
	}

	d.mu.Lock()
	dbState, dbStateFound = d.dbStates[baseName]
	d.mu.Unlock()
	if !dbStateFound {
		// should be impossible
		return nil, false, sql.ErrDatabaseNotFound.New(dbName)
	}

	return dbState.heads[strings.ToLower(database.Revision())], true, nil
}

// RevisionDbName returns the name of the revision db for the base name and revision string given
func RevisionDbName(baseName string, rev string) string {
	return baseName + DbRevisionDelimiter + rev
}

func SplitRevisionDbName(dbName string) (string, string) {
	var baseName, rev string
	parts := strings.SplitN(dbName, DbRevisionDelimiter, 2)
	baseName = parts[0]
	if len(parts) > 1 {
		rev = parts[1]
	}
	return baseName, rev
}

// LookupDbState returns the session state for the database named. Unqualified database names, e.g. `mydb` get resolved
// to the currently checked out HEAD, which could be a branch, a commit, a tag, etc. Revision-qualified database names,
// e.g. `mydb/branch1` get resolved to the session state for the revision named.
// A note on unqualified database names: unqualified names will resolve to a) the head last checked out with
// `dolt_checkout`, or b) the database's default branch, if this session hasn't called `dolt_checkout` yet.
// Also returns a bool indicating whether the database was found, and an error if one occurred.
func (d *DoltSession) LookupDbState(ctx *sql.Context, dbName string) (SessionState, bool, error) {
	s, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return s, true, nil
}

// RemoveDbState invalidates any cached db state in this session, for example, if a database is dropped.
func (d *DoltSession) RemoveDbState(_ *sql.Context, dbName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.dbStates, strings.ToLower(dbName))
	// also clear out any db-level caches for this db
	d.dbCache.Clear()
	return nil
}

// RemoveBranchState removes the session state for a branch, for example, if a branch is deleted.
func (d *DoltSession) RemoveBranchState(ctx *sql.Context, dbName string, branchName string) error {
	baseName, _ := SplitRevisionDbName(dbName)

	checkedOutState, ok, err := d.lookupDbState(ctx, baseName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(baseName)
	}

	d.mu.Lock()
	delete(checkedOutState.dbState.heads, strings.ToLower(branchName))
	d.mu.Unlock()

	db, ok := d.provider.BaseDatabase(ctx, baseName)
	if !ok {
		return sql.ErrDatabaseNotFound.New(baseName)
	}

	defaultHead, err := DefaultHead(ctx, baseName, db)
	if err != nil {
		return err
	}

	checkedOutState.dbState.checkedOutRevSpec = defaultHead

	// also clear out any db-level caches for this db
	d.dbCache.Clear()
	return nil
}

// RenameBranchState replaces all references to a renamed branch with its new name
func (d *DoltSession) RenameBranchState(ctx *sql.Context, dbName string, oldBranchName, newBranchName string) error {
	baseName, _ := SplitRevisionDbName(dbName)

	checkedOutState, ok, err := d.lookupDbState(ctx, baseName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(baseName)
	}

	d.mu.Lock()
	branch, ok := checkedOutState.dbState.heads[strings.ToLower(oldBranchName)]

	if !ok {
		// nothing to rename
		d.mu.Unlock()
		return nil
	}

	delete(checkedOutState.dbState.heads, strings.ToLower(oldBranchName))
	branch.head = strings.ToLower(newBranchName)
	checkedOutState.dbState.heads[strings.ToLower(newBranchName)] = branch

	d.mu.Unlock()

	// also clear out any db-level caches for this db
	d.dbCache.Clear()
	return nil
}

// SetValidateErr sets an error on this session to be returned from every call
// to ValidateSession. This is effectively a way to disable a session.
//
// Used by sql/cluster logic to make sessions on a server which has
// transitioned roles terminally error.
func (d *DoltSession) SetValidateErr(err error) {
	d.validateErr = err
}

// ValidateSession validates a working set if there are a valid sessionState with non-nil working set.
// If there is no sessionState or its current working set not defined, then no need for validation,
// so no error is returned.
func (d *DoltSession) ValidateSession(ctx *sql.Context) error {
	return d.validateErr
}

// Notices returns the set of notices currently queued in this session. Notices are specific to Doltgres sessions
// and are not used by Dolt sessions.
func (d *DoltSession) Notices() []any {
	return d.notices
}

// Notice adds a notice to the queue of the current notices in this session that have not been sent to the client yet.
// Notices are specific to Doltgres sessions and are not used by Dolt sessions.
func (d *DoltSession) Notice(notice any) {
	d.notices = append(d.notices, notice)
}

// ClearNotices clears the queued notices in this session. Notices are specific to Doltgres sessions and are not
// used by Dolt sessions.
func (d *DoltSession) ClearNotices() {
	d.notices = nil
}

// StartTransaction refreshes the state of this session and starts a new transaction.
func (d *DoltSession) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	// TODO: this is only necessary to support filter-branch, which needs to set a root directly and not have the
	//  session state altered when a transaction begins
	if TransactionsDisabled(ctx) {
		return DisabledTransaction{}, nil
	}

	// New transaction, clear all session state
	d.clear()

	// Take a snapshot of the current noms root for every database under management
	doltDatabases := d.provider.DoltDatabases()
	txDbs := make([]SqlDatabase, 0, len(doltDatabases))
	for _, db := range doltDatabases {
		// TODO: this nil check is only necessary to support UserSpaceDatabase and clusterDatabase, come up with a better set of
		//  interfaces to capture these capabilities
		ddb := db.DbData().Ddb
		if ddb != nil {
			rrd, ok := db.(RemoteReadReplicaDatabase)
			if ok && rrd.ValidReplicaState(ctx) {
				err := rrd.PullFromRemote(ctx)
				if err != nil && !IgnoreReplicationErrors() {
					return nil, fmt.Errorf("replication error: %w", err)
				} else if err != nil {
					WarnReplicationError(ctx, err)
				}
			}

			// TODO: this check is relatively expensive, we should cache this value when it changes instead of looking it
			//  up on each transaction start
			if _, v, ok := sql.SystemVariables.GetGlobal(ReadReplicaRemote); ok && v != "" {
				err := ddb.Rebase(ctx)
				if err != nil && !IgnoreReplicationErrors() {
					return nil, err
				} else if err != nil {
					WarnReplicationError(ctx, err)
				}
			}

			txDbs = append(txDbs, db)
		}
	}

	tx, err := NewDoltTransaction(ctx, txDbs, tCharacteristic)
	if err != nil {
		return nil, err
	}

	// The engine sets the transaction after this call as well, but since we begin accessing data below, we need to set
	// this now to avoid seeding the session state with stale data in some cases. The duplication is harmless since the
	// code below cannot error. Additionally we clear any state that was cached by replication updates in the block above.
	d.clear()
	ctx.SetTransaction(tx)

	// Set session vars for every DB in this session using their current branch head
	for _, db := range doltDatabases {
		// faulty settings can make it impossible to load particular DB branch states, so we ignore any errors in this
		// loop and just decline to set the session vars. Throwing an error on transaction start in these cases makes it
		// impossible for the user to correct any problems.
		bs, ok, err := d.lookupDbState(ctx, db.Name())
		if err != nil || !ok {
			continue
		}

		_ = d.setDbSessionVars(ctx, bs, false)
	}

	return tx, nil
}

// clear clears all DB state for this session
func (d *DoltSession) clear() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, dbState := range d.dbStates {
		for head := range dbState.heads {
			delete(dbState.heads, head)
		}
	}
}

func (d *DoltSession) newWorkingSetForHead(ctx *sql.Context, wsRef ref.WorkingSetRef, dbName string) (*doltdb.WorkingSet, error) {
	dbData, _ := d.GetDbData(nil, dbName)

	headSpec, _ := doltdb.NewCommitSpec("HEAD")
	headRef, err := wsRef.ToHeadRef()
	if err != nil {
		return nil, err
	}

	optCmt, err := dbData.Ddb.Resolve(ctx, headSpec, headRef)
	if err != nil {
		return nil, err
	}
	headCommit, ok := optCmt.ToCommit()
	if !ok {
		return nil, doltdb.ErrGhostCommitEncountered
	}

	headRoot, err := headCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	return doltdb.EmptyWorkingSet(wsRef).WithWorkingRoot(headRoot).WithStagedRoot(headRoot), nil
}

// CommitTransaction commits the in-progress transaction. Depending on session settings, this may write only a new
// working set, or may additionally create a new dolt commit for the current HEAD. If more than one branch head has
// changes, the transaction is rejected.
func (d *DoltSession) CommitTransaction(ctx *sql.Context, tx sql.Transaction) (err error) {
	// Any non-error path must set the ctx's transaction to nil even if no work was done, because the engine only clears
	// out transaction state in some cases. Changes to only branch heads (creating a new branch, reset, etc.) have no
	// changes to commit visible to the transaction logic, but they still need a new transaction on the next statement.
	// See comment in |commitBranchState|
	defer func() {
		if err == nil {
			ctx.SetTransaction(nil)
		}
	}()

	if TransactionsDisabled(ctx) {
		return nil
	}

	dirties := d.dirtyWorkingSets()
	if len(dirties) == 0 {
		return nil
	}

	if len(dirties) > 1 {
		return ErrDirtyWorkingSets
	}

	performDoltCommitVar, err := d.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommit)
	if err != nil {
		return err
	}

	peformDoltCommitInt, ok := performDoltCommitVar.(int8)
	if !ok {
		return fmt.Errorf("Unexpected type for var %s: %T", DoltCommitOnTransactionCommit, performDoltCommitVar)
	}

	dirtyBranchState := dirties[0]
	if peformDoltCommitInt == 1 {
		// if the dirty working set doesn't belong to the currently checked out branch, that's an error
		err = d.validateDoltCommit(ctx, dirtyBranchState)
		if err != nil {
			return err
		}

		message := "Transaction commit"
		doltCommitMessageVar, err := d.Session.GetSessionVariable(ctx, DoltCommitOnTransactionCommitMessage)
		if err != nil {
			return err
		}

		doltCommitMessageString, ok := doltCommitMessageVar.(string)
		if !ok && doltCommitMessageVar != nil {
			return fmt.Errorf("Unexpected type for var %s: %T", DoltCommitOnTransactionCommitMessage, doltCommitMessageVar)
		}

		trimmedString := strings.TrimSpace(doltCommitMessageString)
		if strings.TrimSpace(doltCommitMessageString) != "" {
			message = trimmedString
		}

		var pendingCommit *doltdb.PendingCommit
		pendingCommit, err = d.PendingCommitAllStaged(ctx, dirtyBranchState, actions.CommitStagedProps{
			Message:    message,
			Date:       ctx.QueryTime(),
			AllowEmpty: false,
			Force:      false,
			Name:       d.Username(),
			Email:      d.Email(),
		})
		if err != nil {
			return err
		}

		// Nothing to stage, so fall back to CommitWorkingSet logic instead
		if pendingCommit == nil {
			return d.commitWorkingSet(ctx, dirtyBranchState, tx)
		}

		_, err = d.DoltCommit(ctx, ctx.GetCurrentDatabase(), tx, pendingCommit)
		return err
	} else {
		return d.commitWorkingSet(ctx, dirtyBranchState, tx)
	}
}

func (d *DoltSession) validateDoltCommit(ctx *sql.Context, dirtyBranchState *branchState) error {
	currDb := ctx.GetCurrentDatabase()
	if currDb == "" {
		return fmt.Errorf("cannot dolt_commit with no database selected")
	}
	currDbBaseName, rev := SplitRevisionDbName(currDb)
	dirtyDbBaseName := dirtyBranchState.dbState.dbName

	if !strings.EqualFold(currDbBaseName, dirtyDbBaseName) {
		return fmt.Errorf("no changes to dolt_commit on database %s", currDbBaseName)
	}

	d.mu.Lock()
	dbState, ok := d.dbStates[strings.ToLower(currDbBaseName)]
	d.mu.Unlock()

	if !ok {
		return fmt.Errorf("no database state found for %s", currDbBaseName)
	}

	if rev == "" {
		rev = dbState.checkedOutRevSpec
	}

	if !strings.EqualFold(rev, dirtyBranchState.head) {
		return fmt.Errorf("no changes to dolt_commit on branch %s", rev)
	}

	return nil
}

var ErrDirtyWorkingSets = errors.New("Cannot commit changes on more than one branch / database")

// dirtyWorkingSets returns all dirty working sets for this session
func (d *DoltSession) dirtyWorkingSets() []*branchState {
	var dirtyStates []*branchState
	for _, state := range d.dbStates {
		for _, branchState := range state.heads {
			if branchState.dirty {
				dirtyStates = append(dirtyStates, branchState)
			}
		}
	}

	return dirtyStates
}

// DirtyDatabases returns the names of databases who have outstanding changes in this session and need to be committed
// in a SQL transaction before they are visible to other sessions.
func (d *DoltSession) DirtyDatabases() []string {
	var dbNames []string
	for _, dbState := range d.dbStates {
		for _, branchState := range dbState.heads {
			if branchState.dirty {
				dbNames = append(dbNames, dbState.dbName)
				break
			}
		}
	}
	return dbNames
}

// CommitWorkingSet commits the working set for the transaction given, without creating a new dolt commit.
// Clients should typically use CommitTransaction, which performs additional checks, instead of this method.
func (d *DoltSession) CommitWorkingSet(ctx *sql.Context, dbName string, tx sql.Transaction) error {
	commitFunc := func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error) {
		ws, err := dtx.Commit(ctx, workingSet, dbName)
		return ws, nil, err
	}

	_, err := d.commitCurrentHead(ctx, dbName, tx, commitFunc)
	return err
}

// commitWorkingSet commits the working set for the branch state given, without creating a new dolt commit.
func (d *DoltSession) commitWorkingSet(ctx *sql.Context, branchState *branchState, tx sql.Transaction) error {
	commitFunc := func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error) {
		ws, err := dtx.Commit(ctx, workingSet, branchState.RevisionDbName())
		return ws, nil, err
	}

	_, err := d.commitBranchState(ctx, branchState, tx, commitFunc)
	return err
}

// DoltCommit commits the working set and a new dolt commit with the properties given.
// Clients should typically use CommitTransaction, which performs additional checks, instead of this method.
func (d *DoltSession) DoltCommit(
	ctx *sql.Context,
	dbName string,
	tx sql.Transaction,
	commit *doltdb.PendingCommit,
) (*doltdb.Commit, error) {
	commitFunc := func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error) {
		ws, commit, err := dtx.DoltCommit(
			ctx,
			workingSet.WithWorkingRoot(commit.Roots.Working).WithStagedRoot(commit.Roots.Staged),
			commit,
			dbName)
		if err != nil {
			return nil, nil, err
		}

		return ws, commit, err
	}

	return d.commitCurrentHead(ctx, dbName, tx, commitFunc)
}

// doCommitFunc is a function to write to the database, which involves updating the working set and potentially
// updating HEAD with a new commit
type doCommitFunc func(ctx *sql.Context, dtx *DoltTransaction, workingSet *doltdb.WorkingSet) (*doltdb.WorkingSet, *doltdb.Commit, error)

// commitBranchState performs a commit for the branch state given, using the doCommitFunc provided
func (d *DoltSession) commitBranchState(
	ctx *sql.Context,
	branchState *branchState,
	tx sql.Transaction,
	commitFunc doCommitFunc,
) (*doltdb.Commit, error) {
	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return nil, fmt.Errorf("expected a DoltTransaction")
	}

	_, newCommit, err := commitFunc(ctx, dtx, branchState.WorkingSet())
	if err != nil {
		return nil, err
	}

	// Anything that commits a transaction needs its current transaction state cleared so that the next statement starts
	// a new transaction. This should in principle be done by the engine, but it currently only understands explicit
	// COMMIT statements. Any other statements that commit a transaction, including stored procedures, needs to do this
	// themselves.
	ctx.SetTransaction(nil)
	return newCommit, nil
}

// commitCurrentHead commits the current HEAD for the database given, using the doCommitFunc provided
func (d *DoltSession) commitCurrentHead(ctx *sql.Context, dbName string, tx sql.Transaction, commitFunc doCommitFunc) (*doltdb.Commit, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return d.commitBranchState(ctx, branchState, tx, commitFunc)
}

// PendingCommitAllStaged returns a pending commit with all tables staged. Returns nil if there are no changes to stage.
func (d *DoltSession) PendingCommitAllStaged(ctx *sql.Context, branchState *branchState, props actions.CommitStagedProps) (*doltdb.PendingCommit, error) {
	roots := branchState.roots()

	var err error
	roots, err = actions.StageAllTables(ctx, roots, true)
	if err != nil {
		return nil, err
	}

	return d.newPendingCommit(ctx, branchState, roots, props)
}

// NewPendingCommit returns a new |doltdb.PendingCommit| for the database named, using the roots given, adding any
// merge parent from an in progress merge as appropriate. The session working set is not updated with these new roots,
// but they are set in the returned |doltdb.PendingCommit|. If there are no changes staged, this method returns nil.
func (d *DoltSession) NewPendingCommit(
	ctx *sql.Context,
	dbName string,
	roots doltdb.Roots,
	props actions.CommitStagedProps,
) (*doltdb.PendingCommit, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("session state for database %s not found", dbName)
	}

	return d.newPendingCommit(ctx, branchState, roots, props)
}

// newPendingCommit returns a new |doltdb.PendingCommit| for the database and head named by |branchState|
// See NewPendingCommit
func (d *DoltSession) newPendingCommit(ctx *sql.Context, branchState *branchState, roots doltdb.Roots, props actions.CommitStagedProps) (*doltdb.PendingCommit, error) {
	headCommit := branchState.headCommit

	if branchState.WorkingSet() == nil {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}

	var mergeParentCommits []*doltdb.Commit
	if branchState.WorkingSet().MergeCommitParents() {
		mergeParentCommits = []*doltdb.Commit{branchState.WorkingSet().MergeState().Commit()}
	} else if props.Amend {
		numParentsHeadForAmend := headCommit.NumParents()

		for i := 0; i < numParentsHeadForAmend; i++ {
			optCmt, err := headCommit.GetParent(ctx, i)
			if err != nil {
				return nil, err
			}
			parentCommit, ok := optCmt.ToCommit()
			if !ok {
				return nil, doltdb.ErrGhostCommitEncountered
			}

			mergeParentCommits = append(mergeParentCommits, parentCommit)
		}

		// If the commit message isn't set and we're amending the previous commit,
		// go ahead and set the commit message from the current HEAD
		if props.Message == "" && props.Amend {
			meta, err := headCommit.GetCommitMeta(ctx)
			if err != nil {
				return nil, err
			}
			props.Message = meta.Description
		}
	}

	pendingCommit, err := actions.GetCommitStaged(ctx, roots, branchState.WorkingSet(), mergeParentCommits, branchState.dbData.Ddb, props)
	if err != nil {
		// Special case for nothing staged, which is not an error
		if _, ok := err.(actions.NothingStaged); !ok {
			return nil, err
		}
	}

	return pendingCommit, nil
}

// Rollback rolls the given transaction back
func (d *DoltSession) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	// Nothing to do here, we just throw away all our work and let a new transaction begin next statement
	d.clear()
	return nil
}

// As part of GC, ongoing *DoltSessions are asked to make their roots available to the GC process.
// A *DoltSession has the following roots:
// 1) All of the branchStates for the database.
// 2) If there is an active transaction, the initial root for that transaction and any roots for any savepoints of that transaction.
// 3) Working set roots in any writeSession.
func (d *DoltSession) VisitGCRoots(ctx context.Context, dbName string, keep func(hash.Hash) bool) error {
	dbName = strings.ToLower(dbName)
	dbName, _ = SplitRevisionDbName(dbName)

	d.mu.Lock()
	dbState, dbStateFound := d.dbStates[dbName]
	d.mu.Unlock()

	if dbStateFound {
		for _, head := range dbState.heads {
			if head.headRoot != nil {
				h, err := head.headRoot.HashOf()
				if err != nil {
					return err
				}
				if keep(h) {
					panic("gc safepoint establishment found inconsistent state; process could not guarantee it would be able to keep a chunk if we continue")
				}
			} else if head.headCommit != nil {
				h, err := head.headCommit.HashOf()
				if err != nil {
					return err
				}
				if keep(h) {
					panic("gc safepoint establishment found inconsistent state; process could not guarantee it would be able to keep a chunk if we continue")
				}
			} else if head.workingSet != nil {
				hashes, err := head.dbData.Ddb.WorkingSetHashes(ctx, head.workingSet)
				if err != nil {
					return err
				}
				for _, h := range hashes {
					if keep(h) {
						panic("gc safepoint establishment found inconsistent state; process could not guarantee it would be able to keep a chunk if we continue")
					}
				}
			}
			if head.writeSession != nil {
				ws := head.writeSession.GetWorkingSet()
				hashes, err := head.dbData.Ddb.WorkingSetHashes(ctx, ws)
				if err != nil {
					return err
				}
				for _, h := range hashes {
					if keep(h) {
						panic("gc safepoint establishment found inconsistent state; process could not guarantee it would be able to keep a chunk if we continue")
					}
				}
			}
		}
	}

	tx := d.GetTransaction()
	if tx == nil {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		// weird...
		return nil
	}

	h, has := dtx.GetInitialRoot(dbName)
	if has && keep(h) {
		panic("gc safepoint establishment found inconsistent state; process could not guarantee it could would be able to keep a chunk if we continue")
	}
	for _, savepoint := range dtx.savepoints {
		rv, ok := savepoint.roots[dbName]
		if ok {
			h, err := rv.HashOf()
			if err != nil {
				return err
			}
			if keep(h) {
				panic("gc safepoint establishment found inconsistent state; process could not guarantee it could would be able to keep a chunk if we continue")
			}
		}
	}

	return nil
}

// CreateSavepoint creates a new savepoint for this transaction with the name given. A previously created savepoint
// with the same name will be overwritten.
func (d *DoltSession) CreateSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	if TransactionsDisabled(ctx) {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	roots := make(map[string]doltdb.RootValue)
	for _, db := range d.provider.DoltDatabases() {
		// TODO: See TODO in CreateTransaction about needing to skip clusterDatabase and UserSpaceDatabases here :-/. (aaron@, 2025/03)
		ddb := db.DbData().Ddb
		if ddb != nil {
			branchState, ok, err := d.lookupDbState(ctx, db.Name())
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("session state for database %s not found", db.Name())
			}
			baseName, _ := SplitRevisionDbName(db.Name())
			roots[strings.ToLower(baseName)] = branchState.WorkingSet().WorkingRoot()
		}
	}

	dtx.CreateSavepoint(savepointName, roots)
	return nil
}

// RollbackToSavepoint sets this session's root to the one saved in the savepoint name. It's an error if no savepoint
// with that name exists.
func (d *DoltSession) RollbackToSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	if TransactionsDisabled(ctx) {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	roots := dtx.RollbackToSavepoint(savepointName)
	if roots == nil {
		return sql.ErrSavepointDoesNotExist.New(savepointName)
	}

	for dbName, root := range roots {
		err := d.SetWorkingRoot(ctx, dbName, root)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReleaseSavepoint removes the savepoint name from the transaction. It's an error if no savepoint with that name
// exists.
func (d *DoltSession) ReleaseSavepoint(ctx *sql.Context, tx sql.Transaction, savepointName string) error {
	if TransactionsDisabled(ctx) {
		return nil
	}

	dtx, ok := tx.(*DoltTransaction)
	if !ok {
		return fmt.Errorf("expected a DoltTransaction")
	}

	existed := dtx.ClearSavepoint(savepointName)
	if !existed {
		return sql.ErrSavepointDoesNotExist.New(savepointName)
	}

	return nil
}

// GetDoltDB returns the *DoltDB for a given database by name
func (d *DoltSession) GetDoltDB(ctx *sql.Context, dbName string) (*doltdb.DoltDB, bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, false
	}
	if !ok {
		return nil, false
	}

	return branchState.dbData.Ddb, true
}

func (d *DoltSession) GetDbData(ctx *sql.Context, dbName string) (env.DbData[*sql.Context], bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return env.DbData[*sql.Context]{}, false
	}
	if !ok {
		return env.DbData[*sql.Context]{}, false
	}

	return branchState.dbData, true
}

// GetRoots returns the current roots for a given database associated with the session
func (d *DoltSession) GetRoots(ctx *sql.Context, dbName string) (doltdb.Roots, bool) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return doltdb.Roots{}, false
	}
	if !ok {
		return doltdb.Roots{}, false
	}

	return branchState.roots(), true
}

// ResolveRootForRef returns the root value for the ref given, which refers to either a commit spec or is one of the
// special identifiers |WORKING| or |STAGED|
// Returns the root value associated with the identifier given, its commit time and its hash string. The hash string
// for special identifiers |WORKING| or |STAGED| would be itself, 'WORKING' or 'STAGED', respectively.
func (d *DoltSession) ResolveRootForRef(ctx *sql.Context, dbName, refStr string) (doltdb.RootValue, *types.Timestamp, string, error) {
	if refStr == doltdb.Working || refStr == doltdb.Staged {
		// TODO: get from working set / staged update time
		now := types.Timestamp(time.Now())
		// TODO: no current database
		roots, _ := d.GetRoots(ctx, ctx.GetCurrentDatabase())
		if refStr == doltdb.Working {
			return roots.Working, &now, refStr, nil
		} else if refStr == doltdb.Staged {
			return roots.Staged, &now, refStr, nil
		}
	}

	var root doltdb.RootValue
	var commitTime *types.Timestamp
	cs, err := doltdb.NewCommitSpec(refStr)
	if err != nil {
		return nil, nil, "", err
	}

	dbData, ok := d.GetDbData(ctx, dbName)
	if !ok {
		return nil, nil, "", sql.ErrDatabaseNotFound.New(dbName)
	}

	headRef, err := d.CWBHeadRef(ctx, dbName)
	if err == doltdb.ErrOperationNotSupportedInDetachedHead {
		// leave head ref nil, we may not need it (commit hash)
	} else if err != nil {
		return nil, nil, "", err
	}

	optCmt, err := dbData.Ddb.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, nil, "", err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, nil, "", doltdb.ErrGhostCommitRuntimeFailure
	}

	root, err = cm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, "", err
	}

	meta, err := cm.GetCommitMeta(ctx)
	if err != nil {
		return nil, nil, "", err
	}

	t := meta.Time()
	commitTime = (*types.Timestamp)(&t)

	commitHash, err := cm.HashOf()
	if err != nil {
		return nil, nil, "", err
	}

	return root, commitTime, commitHash.String(), nil
}

// SetWorkingRoot sets a new root value for the session for the database named. This is the primary mechanism by which data
// changes are communicated to the engine and persisted back to disk. All data changes should be followed by a call to
// update the session's root value via this method.
// The dbName given should generally be a revision-qualified database name.
// Data changes contained in the |newRoot| aren't persisted until this session is committed.
func (d *DoltSession) SetWorkingRoot(ctx *sql.Context, dbName string, newRoot doltdb.RootValue) error {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	if branchState.WorkingSet() == nil {
		return doltdb.ErrOperationNotSupportedInDetachedHead
	}

	if rootsEqual(branchState.roots().Working, newRoot) {
		return nil
	}

	existingWorkingSet := branchState.WorkingSet()

	if branchState.readOnly {
		return fmt.Errorf("cannot set root on read-only session")
	}
	return d.SetWorkingSet(ctx, dbName, existingWorkingSet.WithWorkingRoot(newRoot))
}

// SetStagingRoot sets the staging root for the session's current database. This is useful when editing the staged
// table without messing with the HEAD or working trees.
func (d *DoltSession) SetStagingRoot(ctx *sql.Context, dbName string, newRoot doltdb.RootValue) error {
	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	existingWorkingSet := branchState.WorkingSet()
	if existingWorkingSet == nil {
		return doltdb.ErrOperationNotSupportedInDetachedHead
	}
	if rootsEqual(branchState.roots().Staged, newRoot) {
		return nil
	}

	if branchState.readOnly {
		return fmt.Errorf("cannot set root on read-only session")
	}
	return d.SetWorkingSet(ctx, dbName, existingWorkingSet.WithStagedRoot(newRoot))
}

// SetRoots sets new roots for the session for the database named. Typically, clients should only set the working root,
// via setRoot. This method is for clients that need to update more of the session state, such as the dolt_ functions.
// Unlike setting the working root, this method always marks the database state dirty.
func (d *DoltSession) SetRoots(ctx *sql.Context, dbName string, roots doltdb.Roots) error {
	sessionState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	if sessionState.WorkingSet() == nil {
		return doltdb.ErrOperationNotSupportedInDetachedHead
	}

	workingSet := sessionState.WorkingSet().WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged)
	return d.SetWorkingSet(ctx, dbName, workingSet)
}

func (d *DoltSession) ResetGlobals(ctx *sql.Context, dbName string, root doltdb.RootValue) error {
	sessionState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}

	tracker, err := sessionState.dbState.globalState.AutoIncrementTracker(ctx)
	if err != nil {
		return err
	}

	err = tracker.InitWithRoots(ctx, root)
	if err != nil {
		return err
	}

	return nil
}

func (d *DoltSession) SetFileSystem(fs filesys.Filesys) {
	d.fs = fs
}

func (d *DoltSession) GetFileSystem() filesys.Filesys {
	return d.fs
}

// SetWorkingSet sets the working set for this session.
func (d *DoltSession) SetWorkingSet(ctx *sql.Context, dbName string, ws *doltdb.WorkingSet) error {
	if ws == nil {
		panic("attempted to set a nil working set for the session")
	}

	branchState, _, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return err
	}
	if ws.Ref() != branchState.WorkingSet().Ref() {
		return fmt.Errorf("must switch working sets with SwitchWorkingSet")
	}
	branchState.workingSet = ws

	err = d.setDbSessionVars(ctx, branchState, true)
	if err != nil {
		return err
	}

	if branchState.writeSession != nil {
		err = branchState.writeSession.SetWorkingSet(ctx, ws)
		if err != nil {
			return err
		}
	}

	branchState.dirty = true
	return nil
}

// SwitchWorkingSet switches to a new working set for this session. Unlike SetWorkingSet, this method expresses no
// intention to eventually persist any uncommitted changes. Rather, this method only changes the in memory state of
// this session. It's equivalent to starting a new session with the working set reference provided. If the current
// session is dirty, this method returns an error. Clients can only switch branches with a clean working set, and so
// must either commit or rollback any changes before attempting to switch working sets.
func (d *DoltSession) SwitchWorkingSet(
	ctx *sql.Context,
	dbName string,
	wsRef ref.WorkingSetRef,
) error {
	headRef, err := wsRef.ToHeadRef()
	if err != nil {
		return err
	}

	d.mu.Lock()

	baseName, _ := SplitRevisionDbName(dbName)
	dbState, ok := d.dbStates[strings.ToLower(baseName)]
	if !ok {
		d.mu.Unlock()
		return sql.ErrDatabaseNotFound.New(dbName)
	}
	dbState.checkedOutRevSpec = headRef.GetPath()

	d.mu.Unlock()

	// bootstrap the db state as necessary
	branchState, ok, err := d.lookupDbState(ctx, baseName+DbRevisionDelimiter+headRef.GetPath())
	if err != nil {
		return err
	}

	if !ok {
		return sql.ErrDatabaseNotFound.New(dbName)
	}

	ctx.SetCurrentDatabase(baseName)

	return d.setDbSessionVars(ctx, branchState, false)
}

func (d *DoltSession) WorkingSet(ctx *sql.Context, dbName string) (*doltdb.WorkingSet, error) {
	// TODO: need to make sure we use a revision qualified DB name here
	sessionState, _, err := d.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if sessionState.WorkingSet() == nil {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}
	return sessionState.WorkingSet(), nil
}

// GetHeadCommit returns the parent commit of the current session.
func (d *DoltSession) GetHeadCommit(ctx *sql.Context, dbName string) (*doltdb.Commit, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return branchState.headCommit, nil
}

// SetSessionVariable is defined on sql.Session. We intercept it here to interpret the special semantics of the system
// vars that we define. Otherwise we pass it on to the base implementation.
func (d *DoltSession) SetSessionVariable(ctx *sql.Context, key string, value interface{}) error {
	if ok, db := IsHeadRefKey(key); ok {
		v, ok := value.(string)
		if !ok {
			return doltdb.ErrInvalidBranchOrHash
		}
		return d.setHeadRefSessionVar(ctx, db, v)
	}
	if IsReadOnlyVersionKey(key) {
		return sql.ErrSystemVariableReadOnly.New(key)
	}

	if strings.EqualFold(key, "foreign_key_checks") {
		return d.setForeignKeyChecksSessionVar(ctx, key, value)
	}

	return d.Session.SetSessionVariable(ctx, key, value)
}

func (d *DoltSession) setHeadRefSessionVar(ctx *sql.Context, db, value string) error {
	headRef, err := ref.Parse(value)
	if err != nil {
		return err
	}

	ws, err := ref.WorkingSetRefForHead(headRef)
	if err != nil {
		return err
	}
	err = d.SwitchWorkingSet(ctx, db, ws)
	if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		return fmt.Errorf("%w; %s: '%s'", doltdb.ErrBranchNotFound, err, value)
	}
	return err
}

func (d *DoltSession) setForeignKeyChecksSessionVar(ctx *sql.Context, key string, value interface{}) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	convertedVal, _, err := sqltypes.Int64.Convert(ctx, value)
	if err != nil {
		return err
	}
	intVal := int64(0)
	if convertedVal != nil {
		intVal = convertedVal.(int64)
	}

	if intVal == 0 {
		for _, dbState := range d.dbStates {
			for _, branchState := range dbState.heads {
				if ws := branchState.WriteSession(); ws != nil {
					opts := ws.GetOptions()
					opts.ForeignKeyChecksDisabled = true
					ws.SetOptions(opts)
				}
			}
		}
	} else if intVal == 1 {
		for _, dbState := range d.dbStates {
			for _, branchState := range dbState.heads {
				if ws := branchState.WriteSession(); ws != nil {
					opts := ws.GetOptions()
					opts.ForeignKeyChecksDisabled = false
					ws.SetOptions(opts)
				}
			}
		}
	} else {
		return sql.ErrInvalidSystemVariableValue.New("foreign_key_checks", intVal)
	}

	return d.Session.SetSessionVariable(ctx, key, value)
}

// addDB adds the database given to this session. This establishes a starting root value for this session, as well as
// other state tracking metadata.
func (d *DoltSession) addDB(ctx *sql.Context, db SqlDatabase) error {
	revisionQualifiedName := strings.ToLower(db.RevisionQualifiedName())
	baseName, _ := SplitRevisionDbName(revisionQualifiedName)

	DefineSystemVariablesForDB(baseName)

	tx, usingDoltTransaction := d.GetTransaction().(*DoltTransaction)

	d.mu.Lock()
	defer d.mu.Unlock()
	sessionState, sessionStateExists := d.dbStates[baseName]

	// Before computing initial state for the DB, check to see if we have it in the cache
	var dbState InitialDbState
	var dbStateCached bool
	if usingDoltTransaction {
		nomsRoot, ok := tx.GetInitialRoot(baseName)
		if ok && sessionStateExists {
			dbState, dbStateCached = d.dbCache.GetCachedInitialDbState(doltdb.DataCacheKey{Hash: nomsRoot}, revisionQualifiedName)
		}
	}

	if !dbStateCached {
		var err error
		dbState, err = db.InitialDBState(ctx)
		if err != nil {
			return err
		}
	}

	if !sessionStateExists {
		sessionState = newEmptyDatabaseSessionState()
		d.dbStates[baseName] = sessionState

		var err error
		sessionState.tmpFileDir, err = dbState.DbData.Rsw.TempTableFilesDir()
		if err != nil {
			if errors.Is(err, env.ErrDoltRepositoryNotFound) {
				return env.ErrFailedToAccessDB.New(dbState.Db.Name())
			}
			return err
		}

		sessionState.dbName = baseName

		baseDb, ok := d.provider.BaseDatabase(ctx, baseName)
		if !ok {
			return fmt.Errorf("unable to find database %s, this is a bug", baseName)
		}

		// The checkedOutRevSpec should be the checked out branch of the database if available, or the revision
		// string otherwise
		sessionState.checkedOutRevSpec, err = DefaultHead(ctx, baseName, baseDb)
		if err != nil {
			return err
		}
	}

	if !dbStateCached && usingDoltTransaction {
		nomsRoot, ok := tx.GetInitialRoot(baseName)
		if ok {
			d.dbCache.CacheInitialDbState(doltdb.DataCacheKey{Hash: nomsRoot}, revisionQualifiedName, dbState)
		}
	}

	branchState := sessionState.NewEmptyBranchState(db.Revision(), db.RevisionType())

	// TODO: get rid of all repo state reader / writer stuff. Until we do, swap out the reader with one of our own, and
	//  the writer with one that errors out
	// TODO: this no longer gets called at session creation time, so the error handling below never occurs when a
	//  database is deleted out from under a running server
	branchState.dbData = dbState.DbData
	adapter := NewSessionStateAdapter(d, db.Name(), dbState.Remotes, dbState.Branches, dbState.Backups)
	branchState.dbData.Rsr = adapter
	branchState.dbData.Rsw = adapter
	branchState.readOnly = dbState.ReadOnly

	// TODO: figure out how to cast this to dsqle.SqlDatabase without creating import cycles
	// Or better yet, get rid of EditOptions from the database, it's a session setting
	nbf := types.Format_Default
	if branchState.dbData.Ddb != nil {
		nbf = branchState.dbData.Ddb.Format()
	}
	editOpts := db.(interface{ EditOptions() editor.Options }).EditOptions()

	if dbState.Err != nil {
		sessionState.Err = dbState.Err
	} else if dbState.WorkingSet != nil {
		branchState.workingSet = dbState.WorkingSet

		// TODO: this is pretty clunky, there is a silly dependency between InitialDbState and globalstate.StateProvider
		//  that's hard to express with the current types
		stateProvider, ok := db.(globalstate.GlobalStateProvider)
		if !ok {
			return fmt.Errorf("database does not contain global state store")
		}
		sessionState.globalState = stateProvider.GetGlobalState()

		tracker, err := sessionState.globalState.AutoIncrementTracker(ctx)
		if err != nil {
			return err
		}
		branchState.writeSession = d.writeSessProv(nbf, branchState.WorkingSet(), tracker, editOpts)
	}

	// WorkingSet is nil in the case of a read only, detached head DB
	if dbState.HeadCommit != nil {
		headRoot, err := dbState.HeadCommit.GetRootValue(ctx)
		if err != nil {
			return err
		}
		branchState.headRoot = headRoot
	} else if dbState.HeadRoot != nil {
		branchState.headRoot = dbState.HeadRoot
	}

	branchState.headCommit = dbState.HeadCommit
	return nil
}

func (d *DoltSession) DatabaseCache(ctx *sql.Context) *DatabaseCache {
	return d.dbCache
}

func (d *DoltSession) AddTemporaryTable(ctx *sql.Context, db string, tbl sql.Table) {
	d.tempTables[strings.ToLower(db)] = append(d.tempTables[strings.ToLower(db)], tbl)
}

func (d *DoltSession) DropTemporaryTable(ctx *sql.Context, db, name string) {
	tables := d.tempTables[strings.ToLower(db)]
	for i, tbl := range d.tempTables[strings.ToLower(db)] {
		if strings.EqualFold(tbl.Name(), name) {
			tables = append(tables[:i], tables[i+1:]...)
			break
		}
	}
	d.tempTables[strings.ToLower(db)] = tables
}

func (d *DoltSession) GetTemporaryTable(ctx *sql.Context, db, name string) (sql.Table, bool) {
	for _, tbl := range d.tempTables[strings.ToLower(db)] {
		if strings.EqualFold(tbl.Name(), name) {
			return tbl, true
		}
	}
	return nil, false
}

// GetAllTemporaryTables returns all temp tables for this session.
func (d *DoltSession) GetAllTemporaryTables(ctx *sql.Context, db string) ([]sql.Table, error) {
	return d.tempTables[strings.ToLower(db)], nil
}

// CWBHeadRef returns the branch ref for this session HEAD for the database named
func (d *DoltSession) CWBHeadRef(ctx *sql.Context, dbName string) (ref.DoltRef, error) {
	branchState, ok, err := d.lookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	if branchState.revisionType != RevisionTypeBranch {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}

	return ref.NewBranchRef(branchState.head), nil
}

// CurrentHead returns the current head for the db named, which must be unqualified. Used for bootstrap resolving the
// correct session head when a database name from the client is unqualified.
func (d *DoltSession) CurrentHead(ctx *sql.Context, dbName string) (string, bool, error) {
	baseName := strings.ToLower(dbName)

	d.mu.Lock()
	dbState, ok := d.dbStates[baseName]
	d.mu.Unlock()

	if ok {
		return dbState.checkedOutRevSpec, true, nil
	}

	return "", false, nil
}

func (d *DoltSession) Username() string {
	return d.username
}

func (d *DoltSession) Email() string {
	return d.email
}

// setDbSessionVars updates the three session vars that track the value of the session root hashes
func (d *DoltSession) setDbSessionVars(ctx *sql.Context, state *branchState, force bool) error {
	// This check is important even when we are forcing an update, because it updates the idea of staleness
	varsStale := d.dbSessionVarsStale(ctx, state)
	if !varsStale && !force {
		return nil
	}

	baseName := state.dbState.dbName

	// Different DBs have different requirements for what state is set, so we are maximally permissive on what's expected
	// in the state object here
	if state.WorkingSet() != nil {
		headRef, err := state.WorkingSet().Ref().ToHeadRef()
		if err != nil {
			return err
		}

		err = d.Session.SetSessionVariable(ctx, HeadRefKey(baseName), headRef.String())
		if err != nil {
			return err
		}
	}

	roots := state.roots()

	if roots.Working != nil {
		h, err := roots.Working.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, WorkingKey(baseName), h.String())
		if err != nil {
			return err
		}
	}

	if roots.Staged != nil {
		h, err := roots.Staged.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, StagedKey(baseName), h.String())
		if err != nil {
			return err
		}
	}

	if state.headCommit != nil {
		h, err := state.headCommit.HashOf()
		if err != nil {
			return err
		}
		err = d.Session.SetSessionVariable(ctx, HeadKey(baseName), h.String())
		if err != nil {
			return err
		}
	}

	return nil
}

// dbSessionVarsStale returns whether the session vars for the database with the state provided need to be updated in
// the session
func (d *DoltSession) dbSessionVarsStale(ctx *sql.Context, state *branchState) bool {
	dtx, ok := ctx.GetTransaction().(*DoltTransaction)
	if !ok {
		return true
	}

	return d.dbCache.CacheSessionVars(state, dtx)
}

func (d *DoltSession) WithGlobals(conf config.ReadWriteConfig) *DoltSession {
	nd := *d
	nd.globalsConf = conf
	return &nd
}

// PersistGlobal implements sql.PersistableSession
func (d *DoltSession) PersistGlobal(ctx *sql.Context, sysVarName string, value interface{}) error {
	if d.globalsConf == nil {
		return ErrSessionNotPersistable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return setPersistedValue(d.globalsConf, sysVar.GetName(), value)
}

// RemovePersistedGlobal implements sql.PersistableSession
func (d *DoltSession) RemovePersistedGlobal(sysVarName string) error {
	if d.globalsConf == nil {
		return ErrSessionNotPersistable
	}

	sysVar, _, err := validatePersistableSysVar(sysVarName)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.globalsConf.Unset([]string{sysVar.GetName()})
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (d *DoltSession) RemoveAllPersistedGlobals() error {
	if d.globalsConf == nil {
		return ErrSessionNotPersistable
	}

	allVars := make([]string, d.globalsConf.Size())
	i := 0
	d.globalsConf.Iter(func(k, v string) bool {
		allVars[i] = k
		i++
		return false
	})

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.globalsConf.Unset(allVars)
}

// GetPersistedValue implements sql.PersistableSession
func (d *DoltSession) GetPersistedValue(k string) (interface{}, error) {
	if d.globalsConf == nil {
		return nil, ErrSessionNotPersistable
	}

	return getPersistedValue(d.globalsConf, k)
}

// SystemVariablesInConfig returns a list of System Variables associated with the session
func (d *DoltSession) SystemVariablesInConfig() ([]sql.SystemVariable, error) {
	if d.globalsConf == nil {
		return nil, ErrSessionNotPersistable
	}
	sysVars, _, err := SystemVariablesInConfig(d.globalsConf)
	if err != nil {
		return nil, err
	}
	return sysVars, nil
}

// GetBranch implements the interface branch_control.Context.
func (d *DoltSession) GetBranch(ctx *sql.Context) (string, error) {
	currentDb := d.Session.GetCurrentDatabase()

	// no branch if there's no current db
	if currentDb == "" {
		return "", nil
	}

	bs, _, err := d.LookupDbState(ctx, currentDb)
	if err != nil {
		return "", err
	}

	if bs != nil && bs.WorkingSet() != nil {
		branchRef, err := bs.WorkingSet().Ref().ToHeadRef()
		if err != nil {
			return "", err
		}
		return branchRef.GetPath(), nil
	}
	// A nil working set probably means that we're not on a branch (like we may be on a commit), so we return an empty string
	return "", nil
}

// GetUser implements the interface branch_control.Context.
func (d *DoltSession) GetUser() string {
	return d.Session.Client().User
}

// GetHost implements the interface branch_control.Context.
func (d *DoltSession) GetHost() string {
	return d.Session.Client().Address
}

// GetController implements the interface branch_control.Context.
func (d *DoltSession) GetController() *branch_control.Controller {
	return d.branchController
}

// Implement sql.LifecycleAwareSession, allowing for GC safepoints to be aware of
// outstanding SQL operations.
func (d *DoltSession) CommandBegin() error {
	if d.gcSafepointController != nil {
		return d.gcSafepointController.SessionCommandBegin(d)
	}
	return nil
}

func (d *DoltSession) CommandEnd() {
	if d.gcSafepointController != nil {
		d.gcSafepointController.SessionCommandEnd(d)
	}
}

func (d *DoltSession) SessionEnd() {
	if d.gcSafepointController != nil {
		d.gcSafepointController.SessionEnd(d)
	}
}

func (d *DoltSession) Validate() {
	// If this gets called, valctx context validation is enabled
	// and the purpose is to validate that this session is
	// registered with an open command on our current
	// gcSafepointController.
	if d.gcSafepointController == nil {
		panic("DoltSession.Validate called. Expected to have a gcSafepointController but did not.")
	}
	d.gcSafepointController.Validate(d)
}

// dolt_gc accesses the safepoint controller for the current
// sql engine through here.
func (d *DoltSession) GCSafepointController() *gcctx.GCSafepointController {
	return d.gcSafepointController
}

// validatePersistedSysVar checks whether a system variable exists and is dynamic
func validatePersistableSysVar(name string) (sql.SystemVariable, interface{}, error) {
	sysVar, val, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return nil, nil, sql.ErrUnknownSystemVariable.New(name)
	}
	if sysVar.IsReadOnly() {
		return nil, nil, sql.ErrSystemVariableReadOnly.New(name)
	}
	return sysVar, val, nil
}

// getPersistedValue reads and converts a config value to the associated MysqlSystemVariable type
func getPersistedValue(conf config.ReadableConfig, k string) (interface{}, error) {
	v, err := conf.GetString(k)
	if err != nil {
		return nil, err
	}

	_, value, err := validatePersistableSysVar(k)
	if err != nil {
		return nil, err
	}

	var res interface{}
	switch value.(type) {
	case int8:
		v = asIntIfBoolValue(v)
		var tmp int64
		tmp, err = strconv.ParseInt(v, 10, 8)
		res = int8(tmp)
	case int, int16, int32, int64:
		v = asIntIfBoolValue(v)
		res, err = strconv.ParseInt(v, 10, 64)
	case uint, uint8, uint16, uint32, uint64:
		v = asIntIfBoolValue(v)
		res, err = strconv.ParseUint(v, 10, 64)
	case float32, float64:
		res, err = strconv.ParseFloat(v, 64)
	case bool:
		return nil, sql.ErrInvalidType.New(value)
	case string:
		return v, nil
	default:
		return nil, sql.ErrInvalidType.New(value)
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

func asIntIfBoolValue(v string) string {
	lower := strings.ToLower(v)
	if lower == "true" {
		return "1"
	} else if lower == "false" {
		return "0"
	}

	return v
}

// setPersistedValue casts and persists a key value pair assuming thread safety
func setPersistedValue(conf config.WritableConfig, key string, value interface{}) error {
	switch v := value.(type) {
	case int:
		return config.SetInt(conf, key, int64(v))
	case int8:
		return config.SetInt(conf, key, int64(v))
	case int16:
		return config.SetInt(conf, key, int64(v))
	case int32:
		return config.SetInt(conf, key, int64(v))
	case int64:
		return config.SetInt(conf, key, v)
	case uint:
		return config.SetUint(conf, key, uint64(v))
	case uint8:
		return config.SetUint(conf, key, uint64(v))
	case uint16:
		return config.SetUint(conf, key, uint64(v))
	case uint32:
		return config.SetUint(conf, key, uint64(v))
	case uint64:
		return config.SetUint(conf, key, v)
	case float32:
		return config.SetFloat(conf, key, float64(v))
	case float64:
		return config.SetFloat(conf, key, v)
	case decimal.Decimal:
		f64, _ := v.Float64()
		return config.SetFloat(conf, key, f64)
	case string:
		return config.SetString(conf, key, v)
	case bool:
		if v {
			return config.SetInt(conf, key, 1)
		} else {
			return config.SetInt(conf, key, 0)
		}
	default:
		return sql.ErrInvalidType.New(v)
	}
}

// SystemVariablesInConfig returns system variables from the persisted config
// and a list of persisted keys that have no corresponding definition in
// |sql.SystemVariables|.
func SystemVariablesInConfig(conf config.ReadableConfig) ([]sql.SystemVariable, []string, error) {
	allVars := make([]sql.SystemVariable, conf.Size())
	var missingKeys []string
	i := 0
	var err error
	var def interface{}
	conf.Iter(func(k, v string) bool {
		def, err = getPersistedValue(conf, k)
		if err != nil {
			if sql.ErrUnknownSystemVariable.Is(err) {
				err = nil
				missingKeys = append(missingKeys, k)
				return false
			}
			err = fmt.Errorf("key: '%s'; %w", k, err)
			return true
		}
		// getPersistedVal already checked for errors
		sysVar, _, _ := sql.SystemVariables.GetGlobal(k)
		sysVar.SetDefault(def)
		allVars[i] = sysVar
		i++
		return false
	})
	if err != nil {
		return nil, nil, err
	}
	return allVars, missingKeys, nil
}

var initMu = sync.Mutex{}

// InitPersistedSystemVars loads all persisted global variables from disk and initializes the corresponding
// SQL system variables with their values.
func InitPersistedSystemVars(dEnv *env.DoltEnv) error {
	initMu.Lock()
	defer initMu.Unlock()

	// Find all the persisted global vars and load their values into sql.SystemVariables
	persistedGlobalVars, err := findPersistedGlobalVars(dEnv)
	if err != nil {
		return err
	}
	sql.SystemVariables.AddSystemVariables(persistedGlobalVars)
	return nil
}

// PersistSystemVarDefaults persists any SQL system variables that have non-deterministic default values, and
// must have their generated default value persisted to disk. If the system variable is already persisted to disk,
// then no changes are made. Currently, the only SQL system variable that requires persisting its default value
// is @@server_uuid, since we need a consistent value used each time the server is started.
func PersistSystemVarDefaults(dEnv *env.DoltEnv) error {
	initMu.Lock()
	defer initMu.Unlock()

	// Find all the persisted global vars and load their values into sql.SystemVariables
	persistedGlobalVars, err := findPersistedGlobalVars(dEnv)
	if err != nil {
		return err
	}

	// Ensure the @@server_uuid value is persisted
	var globalConfig config.ReadWriteConfig
	if localConf, ok := dEnv.Config.GetConfig(env.LocalConfig); ok {
		globalConfig = config.NewPrefixConfig(localConf, env.SqlServerGlobalsPrefix)
	} else if globalConf, ok := dEnv.Config.GetConfig(env.GlobalConfig); ok {
		globalConfig = config.NewPrefixConfig(globalConf, env.SqlServerGlobalsPrefix)
	} else {
		return fmt.Errorf("unable to find local or global Dolt configuration")
	}
	return persistServerUuid(persistedGlobalVars, globalConfig)
}

// findPersistedGlobalVars searches the local and global configuration for the specified Dolt environment |dEnv| and
// finds all global persisted system variables. Since global system vars can be persisted in either the local or
// global configuration stores, this function searches both.
func findPersistedGlobalVars(dEnv *env.DoltEnv) (persistedGlobalVars []sql.SystemVariable, err error) {
	foundConfig := false
	if localConf, ok := dEnv.Config.GetConfig(env.LocalConfig); ok {
		foundConfig = true
		localConfig := config.NewPrefixConfig(localConf, env.SqlServerGlobalsPrefix)
		globalVars, missingKeys, err := SystemVariablesInConfig(localConfig)
		if err != nil {
			return nil, err
		}

		persistedGlobalVars = append(persistedGlobalVars, globalVars...)
		for _, k := range missingKeys {
			cli.Printf("warning: persisted system variable %s was not loaded since its definition does not exist.\n", k)
		}
	}

	if globalConf, ok := dEnv.Config.GetConfig(env.GlobalConfig); ok {
		foundConfig = true
		globalConfig := config.NewPrefixConfig(globalConf, env.SqlServerGlobalsPrefix)
		globalVars, missingKeys, err := SystemVariablesInConfig(globalConfig)
		if err != nil {
			return nil, err
		}

		persistedGlobalVars = append(persistedGlobalVars, globalVars...)
		for _, k := range missingKeys {
			cli.Printf("warning: persisted system variable %s was not loaded since its definition does not exist.\n", k)
		}
	}

	if !foundConfig {
		cli.Println("warning: no local or global Dolt configuration found; session is not persistable")
	}

	return persistedGlobalVars, nil
}

// persistServerUuid searches the set of persisted global variables from |persistedGlobalVars| to see if the
// global @@server_uuid variable has been persisted already. If not, it is persisted to the Dolt configuration
// file specified by |globalConfig|.
//
// The @@server_uuid system variable is unique in that it has a non-deterministic default value, so unlike other
// system variables, we need to persist the default so that the value is stable across invocations. This could
// be generalized more in the GMS layer by adding a "DefaultPersisted" property to the @@server_uuid system var
// definition, but since this is the only system variable that currently needs this, we just handle it here.
func persistServerUuid(persistedGlobalVars []sql.SystemVariable, globalConfig config.ReadWriteConfig) error {
	foundServerUuidSysVar := false
	for _, persistedGlobalVar := range persistedGlobalVars {
		if persistedGlobalVar == nil {
			continue
		}
		if persistedGlobalVar.GetName() == "server_uuid" {
			foundServerUuidSysVar = true
		}
	}

	// if @@server_uuid hasn't been persisted yet, then we need to persist its generated default value
	if !foundServerUuidSysVar {
		_, value, ok := sql.SystemVariables.GetGlobal("server_uuid")
		if !ok {
			return fmt.Errorf("unable to find @@server_uuid system variable definition")
		}
		return setPersistedValue(globalConfig, "server_uuid", value)
	}

	return nil
}

// TransactionRoot returns the noms root for the given database in the current transaction
func TransactionRoot(ctx *sql.Context, db SqlDatabase) (hash.Hash, error) {
	tx, ok := ctx.GetTransaction().(*DoltTransaction)
	// We don't have a real transaction in some cases (esp. PREPARE), in which case we need to use the tip of the data
	if !ok {
		return db.DbData().Ddb.NomsRoot(ctx)
	}

	nomsRoot, ok := tx.GetInitialRoot(db.Name())
	if !ok {
		return hash.Hash{}, fmt.Errorf("could not resolve initial root for database %s", db.Name())
	}

	return nomsRoot, nil
}

// DefaultHead returns the head for the database given when one isn't specified
func DefaultHead(ctx *sql.Context, baseName string, db SqlDatabase) (string, error) {
	head := ""

	// First check the global variable for the default branch
	_, val, ok := sql.SystemVariables.GetGlobal(DefaultBranchKey(baseName))
	if ok {
		head = val.(string)
		branchRef, err := ref.Parse(head)
		if err == nil {
			head = branchRef.GetPath()
		} else {
			head = ""
			// continue to below
		}
	}

	// Fall back to the database's initially checked out branch
	if head == "" {
		rsr := db.DbData().Rsr
		if rsr != nil {
			headRef, err := rsr.CWBHeadRef(ctx)
			if err != nil {
				return "", err
			}
			head = headRef.GetPath()
		}
	}

	if head == "" {
		head = db.Revision()
	}

	return head, nil
}

// WriteSessFunc is a constructor that session builders use to
// create fresh table editors.
// The indirection avoids a writer/dsess package import cycle.
type WriteSessFunc func(nbf *types.NomsBinFormat, ws *doltdb.WorkingSet, aiTracker globalstate.AutoIncrementTracker, opts editor.Options) WriteSession
