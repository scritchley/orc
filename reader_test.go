package orc

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"testing"
)

func TestReader(t *testing.T) {

	expectedFile, err := os.Open("./examples/expected/orc-file-11-format.jsn")
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(expectedFile)

	r, err := Open("./examples/orc-file-11-format.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	c := r.Select("*")
	for c.Stripes() {
		for c.Next() {
			var expected map[string]interface{}
			err := dec.Decode(&expected)
			if err != nil {
				t.Fatal(err)
			}
			actual := c.Row()[0]
			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("Test failed, expected %v to equal %v", actual, expected)
			}
		}
	}
	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

}

func TestReader2(t *testing.T) {

	r, err := Open("./examples/testorc")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	c := r.Select("*")
	for c.Stripes() {
		for c.Next() {
			// t.Log(c.Row())
		}
	}
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
