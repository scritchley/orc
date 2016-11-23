package orc

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"
)

func TestReadExamples(t *testing.T) {

	testCases := []struct {
		expected string
		example  string
	}{
		{
			expected: "decimal.jsn.gz",
			example:  "decimal.orc",
		},
		{
			expected: "TestOrcFile.test1.jsn.gz",
			example:  "TestOrcFile.test1.orc",
		},
		{
			expected: "orc_split_elim.jsn.gz",
			example:  "orc_split_elim.orc",
		},
		{
			expected: "orc-file-11-format.jsn.gz",
			example:  "orc-file-11-format.orc",
		},
	}

	for _, tc := range testCases {

		t.Run(fmt.Sprintf("comparing %s to %s", tc.expected, tc.example), func(t *testing.T) {

			e, err := loadExpected(tc.expected)
			if err != nil {
				t.Fatal(err)
			}

			r, err := Open(path.Join("./examples/", tc.example))
			if err != nil {
				t.Fatal(err)
			}

			t.Log(r.Schema())

			c := r.Select("*")

			var rowNum int
			for c.Stripes() {
				for c.Next() {
					rowData := c.Row()[0].(map[string]interface{})
					// We have to perform some coercion so that the values match
					// the JSON values in the terribly formatted example files.
					for i := range rowData {
						switch v := rowData[i].(type) {
						case int64:
							rowData[i] = float64(v)
						case time.Time:
							rowData[i] = v.UTC().Format("2006-01-02 15:04:05.0") // Java JSON Serde timestamp format
						case []byte:
							values := make([]uint, len(v))
							for j := range v {
								values[j] = uint(v[j])
							}
							rowData[i] = values
						}
					}
					row, err := json.Marshal(rowData)
					if err != nil {
						t.Fatal(err)
					}
					var m map[string]interface{}
					err = json.Unmarshal(row, &m)
					if err != nil {
						t.Fatal(err)
					}
					for col, val := range e[rowNum] {
						if actualVal, ok := m[col]; ok {
							if !reflect.DeepEqual(val, actualVal) {
								t.Fatalf("Test failed on row %v column `%s`, expected %v (%T) got %v (%T)", rowNum, col, val, val, actualVal, actualVal)
							}
						} else {
							t.Fatalf("Test failed, column %s expected but not present", col)
						}
					}
					rowNum++
				}
			}

		})

	}

}

func loadExpected(filename string) ([]map[string]interface{}, error) {

	f, err := os.Open(path.Join("./examples/expected", filename))
	if err != nil {
		return nil, err
	}

	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(r)

	var results []map[string]interface{}

	for d.More() {
		var row map[string]interface{}
		err := d.Decode(&row)
		if err != nil {
			return nil, err
		}
		results = append(results, row)
	}

	return results, nil

}
