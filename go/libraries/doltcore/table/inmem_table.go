package table

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"io"
)

// InMemTable holds a simple list of rows that can be retrieved, or appended to.  It is meant primarily for testing.
type InMemTable struct {
	sch  schema.Schema
	rows []row.Row
}

// NewInMemTable creates an empty Table with the expectation that any rows added will have the given
// Schema
func NewInMemTable(sch schema.Schema) *InMemTable {
	return NewInMemTableWithData(sch, []row.Row{})
}

// NewInMemTableWithData creates a Table with the riven rows
func NewInMemTableWithData(sch schema.Schema, rows []row.Row) *InMemTable {
	return NewInMemTableWithDataAndValidationType(sch, rows)
}

func NewInMemTableWithDataAndValidationType(sch schema.Schema, rows []row.Row) *InMemTable {
	return &InMemTable{sch, rows}
}

// AppendRow appends a row.  Appended rows must be valid for the table's schema
func (imt *InMemTable) AppendRow(r row.Row) error {
	if !row.IsValid(r, imt.sch) {
		return ErrInvalidRow
	}

	imt.rows = append(imt.rows, r)

	return nil
}

// GetRow gets a row by index
func (imt *InMemTable) GetRow(index int) (row.Row, error) {
	return imt.rows[index], nil
}

// GetSchema gets the table's schema
func (imt *InMemTable) GetSchema() schema.Schema {
	return imt.sch
}

// NumRows returns the number of rows in the table
func (imt *InMemTable) NumRows() int {
	return len(imt.rows)
}

// InMemTableReader is an implementation of a TableReader for an InMemTable
type InMemTableReader struct {
	tt      *InMemTable
	current int
}

// NewInMemTableReader creates an instance of a TableReader from an InMemTable
func NewInMemTableReader(imt *InMemTable) *InMemTableReader {
	return &InMemTableReader{imt, 0}
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (rd *InMemTableReader) ReadRow() (row.Row, error) {
	numRows := rd.tt.NumRows()

	if rd.current < numRows {
		r := rd.tt.rows[rd.current]
		rd.current++

		return r, nil
	}

	return nil, io.EOF
}

// Close should release resources being held
func (rd *InMemTableReader) Close() error {
	rd.current = -1
	return nil
}

// GetSchema gets the schema of the rows that this reader will return
func (rd *InMemTableReader) GetSchema() schema.Schema {
	return rd.tt.sch
}

// InMemTableWriter is an implementation of a TableWriter for an InMemTable
type InMemTableWriter struct {
	tt *InMemTable
}

// NewInMemTableWriter creates an instance of a TableWriter from an InMemTable
func NewInMemTableWriter(imt *InMemTable) *InMemTableWriter {
	return &InMemTableWriter{imt}
}

// WriteRow will write a row to a table
func (w *InMemTableWriter) WriteRow(r row.Row) error {
	return w.tt.AppendRow(r)
}

// Close should flush all writes, release resources being held
func (w *InMemTableWriter) Close() error {
	return nil
}

// GetSchema gets the schema of the rows that this writer writes
func (w *InMemTableWriter) GetSchema() schema.Schema {
	return w.tt.sch
}
