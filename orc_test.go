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
		{
			expected: "TestOrcFile.emptyFile.jsn.gz",
			example:  "TestOrcFile.emptyFile.orc",
		},
		{
			expected: "nulls-at-end-snappy.jsn.gz",
			example:  "nulls-at-end-snappy.orc",
		},
		// {
		// 	expected: "TestOrcFile.testUnionAndTimestamp.jsn.gz",
		// 	example:  "TestOrcFile.testUnionAndTimestamp.orc",
		// },
		{
			expected: "TestOrcFile.testSnappy.jsn.gz",
			example:  "TestOrcFile.testSnappy.orc",
		},
		// {
		// 	expected: "TestOrcFile.testDate2038.jsn.gz",
		// 	example:  "TestOrcFile.testDate2038.orc",
		// },
		// {
		// 	expected: "TestOrcFile.testDate1900.jsn.gz",
		// 	example:  "TestOrcFile.testDate1900.orc",
		// },
		{
			expected: "TestOrcFile.columnProjection.jsn.gz",
			example:  "TestOrcFile.columnProjection.orc",
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

			// Log out the schema description, helps in the event of a test failure.
			t.Log(r.Schema().String())

			c := r.Select("*")

			var rowNum int
			for c.Stripes() {
				for c.Next() {
					rowData := c.Row()[0].(Struct)
					// We have to perform some coercion so that the values match
					// the JSON values in the formatted example files.
					for col, val := range rowData {
						switch ty := val.(type) {
						case Date:
							rowData[col] = ty.UTC().Format("2006-01-02")
						case time.Time:
							rowData[col] = ty.UTC().Format("2006-01-02 15:04:05.0")
						case []byte:
							values := make([]uint, len(ty))
							for j := range ty {
								values[j] = uint(ty[j])
							}
							rowData[col] = values
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
