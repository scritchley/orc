package orc

import (
	"reflect"
	"testing"

	"github.com/crphang/orc/proto"
)

func TestCursor(t *testing.T) {

	r, err := Open("./examples/demo-11-zlib.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Log the schema
	t.Log(r.Schema())

	// Select a single column from the file.
	c := r.Select("_col0")

	// Call Stripes to trigger reading the first stripe.
	s := c.Stripes()
	if !s {
		t.Errorf("Test failed, expected true, got false")
	}

	// Call Next to initialise the readers.
	n := c.Next()
	if !n {
		t.Errorf("Test failed, expected true, got false")
	}

	// There should be a data stream available for reading.
	stream := c.Stripe.get(streamName{1, proto.Stream_DATA})
	if stream == nil {
		t.Errorf("Test failed, got nil stream")
	}

	// There should also be a row index.
	stream = c.Stripe.get(streamName{1, proto.Stream_ROW_INDEX})
	if stream == nil {
		t.Errorf("Test failed, got nil stream")
	}

	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

}

func TestCursorResets(t *testing.T) {

	r, err := Open("./examples/demo-11-zlib.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Select a single column from the file.
	c := r.Select("_col0")

	// Call Stripes to trigger reading the first stripe.
	var values []interface{}
	c.Stripes()
	for c.Next() {
		vals := c.Row()
		values = append(values, vals...)
	}
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	// Select a single column from the file.
	c = r.Select("_col0")

	// Call Stripes to trigger reading the first stripe.
	var valuesAgain []interface{}
	c.Stripes()
	for c.Next() {
		vals := c.Row()
		valuesAgain = append(valuesAgain, vals...)
	}
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(values, valuesAgain) {
		t.Errorf("Test failed, expected values to be equal")
	}

}

func TestCursorSelectError(t *testing.T) {

	r, err := Open("./examples/demo-11-zlib.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Try to select a column that doesn't exist.
	c := r.Select("notfound")

	var hasNext bool
	for c.Next() {
		hasNext = true
	}

	if hasNext {
		t.Errorf("Next returned true, expected false")
	}

	err = c.Err()
	if err == nil {
		t.Errorf("Expected error")
	}
	if err.Error() != "no field with name: notfound" {
		t.Errorf("Unexpected error: %s", err.Error())
	}

}

func TestCursorSelectStripe(t *testing.T) {
	r, err := Open("./examples/demo-11-none.orc")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	expectedStripes := 385

	numStripes, err := r.NumStripes()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if numStripes != expectedStripes {
		t.Fatalf("Expected %d stripes, got %d", expectedStripes, numStripes)
	}

	cols := r.Schema().Columns()
	c := r.Select(cols...)

	err = c.SelectStripe(numStripes - 1)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var row []interface{}
	for c.Next() {
		row = c.Row()
	}

	expectedLastRow := []interface{}{
		int64(1920800),
		"F",
		"U",
		"Unknown",
		int64(10000),
		"Unknown",
		int64(6),
		int64(6),
		int64(6),
	}

	if len(row) != len(expectedLastRow) {
		t.Fatalf("Expected %d elements, got %d", len(expectedLastRow), len(row))
	}

	for idx, e := range expectedLastRow {
		if e != row[idx] {
			t.Fatalf("Expected %v, got %v", e, row[idx])
		}
	}
}
