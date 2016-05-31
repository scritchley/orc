package orc

type RowReader struct {
	columns []*ColumnReader
}

// NewRowReader returns a new RowReader that reads from the given
// ORCColumns.
func NewRowReader(columns ...*ColumnReader) (*RowReader, error) {
	return &RowReader{
		columns: columns,
	}, nil
}

// Seek moves the cursor to the row at the provided offset. If
// the offset is out of bounds an error is returned.
func (rr *RowReader) Seek(offset int64) error {
	return nil
}

// Close closes any readers and returns any error
// that occurs.
func (rr *RowReader) Close() error {
	return nil
}

// Columns returns a slice of strings containing the column
// names or an error if one occurs whilst reading the columns.
func (rr *RowReader) Columns() ([]string, error) {
	return nil, nil
}

// Err returns the last error to occur when reading the rows.
func (rr *RowReader) Err() error {
	return nil
}

// Next returns true if there are rows available otherwise
// it returns false.
func (rr *RowReader) Next() bool {
	return false
}

// Scan copies the columns in the current row into the values
// pointed at by dest. It will return an error if the underlying
// type of the column does not match the type of the underlying
// interface value.
func (rr *RowReader) Scan(dest ...interface{}) error {
	return nil
}
