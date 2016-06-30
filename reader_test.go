package orc

import (
	"io"
	"testing"
)

func TestReader(t *testing.T) {

	r, err := Open("./examples/orc-file-11-format.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := r.Select("boolean1")
	row := 0
	for c.Stripes() {
		for c.Next() {
			c.Row()
			row++
		}
	}
	t.Log(row)

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

}

// func TestReader2(t *testing.T) {

// 	r, err := Open("./examples/performanceanalyticshistorical.orc")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer r.Close()

// 	t.Log(r.schema.String())

// 	c := r.Select("_col42")

// 	rows := 0

// 	tacticImps := make(map[interface{}]int64)
// 	for c.Stripes() {

// 		for c.Next() {

// 			t.Log(rows, c.Row())
// 			// row := c.Row()
// 			// imps := row[1].(int64)
// 			// if v, ok := tacticImps[row[0]]; ok {
// 			// 	tacticImps[row[0]] = v + imps
// 			// } else {
// 			// 	tacticImps[row[0]] = imps
// 			// }
// 			rows++

// 		}

// 	}

// 	for k, v := range tacticImps {
// 		t.Log(k, v)
// 	}
// 	t.Log("records:", rows)

// 	if err := c.Err(); err != nil && err != io.EOF {
// 		t.Fatal(err)
// 	}

// }

// func TestReader3(t *testing.T) {

// 	r, err := Open("./examples/impressions.orc")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer r.Close()

// 	t.Log(r.schema.String())

// 	c := r.Select("uuid")

// 	for c.Stripes() {

// 		for c.Next() {

// 			t.Log(c.Row())

// 		}

// 	}

// 	if err := c.Err(); err != nil && err != io.EOF {
// 		t.Fatal(err)
// 	}

// }
