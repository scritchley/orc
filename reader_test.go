package orc

import (
	"testing"
)

func TestReadNullAtEnd(t *testing.T) {
	r, err := Open("examples/nulls-at-end-snappy.orc")
	if err != nil {
		t.Fatal(err)
	}

	defer r.Close()
	var row []interface{}
	rows := 0
	c := r.Select(r.schema.Columns()...)
	for c.Stripes() {
		for c.Next() {
			row = c.Row()
			rows++
		}
	}

	if c.Err() != nil {
		t.Fatal(err)
	}

	if rows != 70000 {
		t.Errorf("Expected 70000 rows, got %d", rows)
	}

	expectedLastRow := []interface{}{nil, int64(-12769), nil, nil, nil, nil, nil}

	for i, v := range row {
		if expectedLastRow[i] != v {
			t.Errorf("Expected item %d of the last row to be %v got %v", i, expectedLastRow[i], v)
		}
	}
}

func TestNumStripes(t *testing.T) {
	expectedStripes := 385

	r, err := Open("examples/demo-11-none.orc")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer r.Close()

	n, err := r.NumStripes()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if n != expectedStripes {
		t.Fatalf("Expected %d stripes, got %d", expectedStripes, n)
	}
}
