package orc

// Cursor is used for iterating through the stripes and
// rows within the ORC file.
type Cursor struct {
	*Reader
	streams  streamMap
	columns  []*TypeDescription
	included []int
	readers  []TreeReader
	err      error
}

// Select determines the columns that will be read from the ORC file.
// Only streams for the selected columns will be loaded into memory.
func (c *Cursor) Select(fields ...string) *Cursor {
	var columns []*TypeDescription
	var included []int
	for _, field := range fields {
		column, err := c.Reader.schema.GetField(field)
		if err != nil {
			c.err = err
			return c
		}
		columns = append(columns, column)
		included = append(included, column.getID())
		included = append(included, column.getChildrenIDs()...)
	}
	c.columns = columns
	c.included = included
	return c
}

// prepareStreamReaders prepares TreeReaders for each of the columns
// that will be read.
func (c *Cursor) prepareStreamReaders() error {
	var readers []TreeReader
	// fmt.Println(c.streams)
	for _, column := range c.columns {
		reader, err := createTreeReader(column, c.streams, c.Reader)
		if err != nil {
			return err
		}
		readers = append(readers, reader)
	}
	c.readers = readers
	return nil
}

// prepareNextStripe retrieves the stream information for the next stripe.
func (c *Cursor) prepareNextStripe() error {
	// Prepare the next stripe by loading it into memory
	// and creating the required readers for each of the
	// required columns.
	var err error
	c.streams, err = c.Reader.getStreams(c.included...)
	if err != nil {
		return err
	}
	return nil
}

// Next returns true if another set of records are available.
func (c *Cursor) Next() bool {
	// If readers have values available return true.
	if c.next() {
		return true
	}
	// If there is an error (such as EOF) return false.
	if c.Err() != nil {
		return false
	}
	// Otherwise, attempt to prepare the next set of streams for reading.
	err := c.prepareStreamReaders()
	if err != nil {
		c.err = err
		return false
	}
	return true
}

// next returns true if all readers return that another row is available.
func (c *Cursor) next() bool {
	// If there are no readers then return false.
	if len(c.readers) == 0 {
		return false
	}
	// Check all readers have values available. Assumes all readers
	// will always have the same number of values per stripe.
	for _, reader := range c.readers {
		if !reader.Next() {
			return false
		}
	}
	return true
}

// Row returns the next row of values.
func (c *Cursor) Row() []interface{} {
	s := make([]interface{}, len(c.readers), len(c.readers))
	for i, reader := range c.readers {
		s[i] = reader.Value()
	}
	return s
}

// Err returns the last error to have occurred.
func (c *Cursor) Err() error {
	// Check whether there is already an error.
	if c.err != nil {
		return c.err
	}
	// Otherwise, return the first error returned by the readers.
	for _, reader := range c.readers {
		if err := reader.Err(); err != nil {
			return err
		}
	}
	return nil
}

// Stripes prepares the next stripe for reading, returning true once its ready. It
// returns false if an error occurs whilst preparing the stripe.
func (c *Cursor) Stripes() bool {
	// Prepare the next stripe for reading.
	err := c.prepareNextStripe()
	if err != nil {
		c.err = err
		return false
	}
	return true
}
