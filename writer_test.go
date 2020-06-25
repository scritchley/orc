package orc

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

type bytesSizedReaderAt struct {
	*bytes.Buffer
}

func (b *bytesSizedReaderAt) Size() int64 {
	return int64(b.Len())
}

func (b *bytesSizedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	copy(p, b.Bytes()[off:off+int64(len(p))])
	return len(p), nil
}

func TestWriter(t *testing.T) {
	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename) // clean up
	defer f.Close()

	schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1478123411, 99).UTC()
	timeIncrease := 5*time.Second + 10001*time.Nanosecond
	length := 1000
	var intSum int64
	for i := 0; i < length; i++ {
		string1 := fmt.Sprintf("%x", rand.Int63n(1000))
		timestamp1 := now.Add(time.Duration(i) * timeIncrease)
		int1 := rand.Int63n(10000)
		intSum += int1
		boolean1 := int1 > 4444
		double1 := rand.Float64()
		nested := []interface{}{
			rand.Float64(),
			[]interface{}{
				rand.Int63n(10000),
			},
		}
		err = w.Write(string1, timestamp1, int1, boolean1, double1, nested)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read the writer output
	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	var compareIntSum int64
	var previousTimestamp time.Time
	c := r.Select("int1", "timestamp1")
	row := 0
	for c.Stripes() {
		for c.Next() {
			compareIntSum += c.Row()[0].(int64)
			timestamp, ok := c.Row()[1].(time.Time)
			if !ok {
				t.Fatalf("Row %d: Expected a time.Time but got %T", row, c.Row()[1])
			}
			if row == 0 {
				if timestamp != now {
					t.Fatalf("Row %d: Expected a timestamp %s got %s. Difference: %s", row, now, timestamp, now.Sub(timestamp))
				}
			} else {
				d := timestamp.Sub(previousTimestamp)
				if d != timeIncrease {
					t.Fatalf("Row %d: Expected a time increase of %s but got %s", row, timeIncrease, d)
				}
			}
			previousTimestamp = timestamp
			row++
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if intSum != compareIntSum {
		t.Errorf("Test failed, expected %v sum got %v", intSum, compareIntSum)
	}

	if row != length {
		t.Errorf("Test failed, expected %v rows got %v", length, row)
	}

}

func TestWriterWithCompression(t *testing.T) {
	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename) // clean up
	defer f.Close()

	schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema), SetCompression(CompressionZlib{Level: flate.DefaultCompression}))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1478123411, 99).UTC()
	timeIncrease := 5*time.Second + 10001*time.Nanosecond
	length := 10001
	var intSum int64
	for i := 0; i < length; i++ {
		string1 := fmt.Sprintf("%x", rand.Int63n(1000))
		timestamp1 := now.Add(time.Duration(i) * timeIncrease)
		int1 := rand.Int63n(10000)
		intSum += int1
		boolean1 := int1 > 4444
		double1 := rand.Float64()
		nested := []interface{}{
			rand.Float64(),
			[]interface{}{
				rand.Int63n(10000),
			},
		}
		err = w.Write(string1, timestamp1, int1, boolean1, double1, nested)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read the writer output
	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	var compareIntSum int64
	var previousTimestamp time.Time
	c := r.Select("int1", "timestamp1")
	row := 0
	for c.Stripes() {
		for c.Next() {
			compareIntSum += c.Row()[0].(int64)
			timestamp, ok := c.Row()[1].(time.Time)
			if !ok {
				t.Fatalf("Row %d: Expected a time.Time but got %T", row, c.Row()[1])
			}
			if row == 0 {
				if timestamp != now {
					t.Fatalf("Row %d: Expected a timestamp %s got %s. Difference: %s", row, now, timestamp, now.Sub(timestamp))
				}
			} else {
				d := timestamp.Sub(previousTimestamp)
				if d != timeIncrease {
					t.Fatalf("Row %d: Expected a time increase of %s but got %s", row, timeIncrease, d)
				}
			}
			previousTimestamp = timestamp
			row++
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if intSum != compareIntSum {
		t.Errorf("Test failed, expected %v sum got %v", intSum, compareIntSum)
	}

	if row != length {
		t.Errorf("Test failed, expected %v rows got %v", length, row)
	}

}

func TestWriteNil(t *testing.T) {
	f, err := ioutil.TempFile("", "testwritenil")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer f.Close()
	defer os.Remove(filename)

	schema, err := ParseSchema("struct<string1:string,int1:int,double1:double,timestamp1:timestamp,boolean1:boolean>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema))
	if err != nil {
		t.Fatal(err)
	}

	err = w.Write(nil, nil, nil, nil, nil)
	if err != nil {
		t.Errorf("Test failed, expected no error, got %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	c := r.Select("string1", "int1", "double1", "timestamp1", "boolean1")
	expected := []interface{}{nil, nil, nil, nil, nil}
	for c.Stripes() {
		for c.Next() {
			actual := c.Row()
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Test failed, expected %v, got %v", expected, actual)
			}
		}
	}

}

func TestWriterWithCompressionSingleColumn(t *testing.T) {
	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename) // clean up
	defer f.Close()

	schema, err := ParseSchema("struct<int1:int>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema), SetCompression(CompressionZlib{Level: flate.DefaultCompression}))
	if err != nil {
		t.Fatal(err)
	}

	length := 10
	var intSum int64
	for i := 0; i < length; i++ {
		int1 := int64(10)
		intSum += int1
		err = w.Write(int1)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read the writer output
	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	var compareIntSum int64
	c := r.Select("int1")
	row := 0
	for c.Stripes() {
		for c.Next() {
			compareIntSum += c.Row()[0].(int64)
			row++
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if intSum != compareIntSum {
		t.Errorf("Test failed, expected %v sum got %v", intSum, compareIntSum)
	}

	if row != length {
		t.Errorf("Test failed, expected %v rows got %v", length, row)
	}

}

func TestWriterWithCompressionRecompress(t *testing.T) {
	t.Skip()

	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename) // clean up
	defer f.Close()

	original, err := Open("examples/demo-12-zlib.orc")
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()

	w, err := NewWriter(f, SetSchema(original.schema), SetCompression(CompressionZlib{Level: flate.DefaultCompression}))
	if err != nil {
		t.Fatal(err)
	}

	c := original.Select(original.schema.Columns()...)
	for c.Stripes() {
		for c.Next() {
			err = w.Write(c.Row()...)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterWithNils(t *testing.T) {
	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename)
	defer f.Close()

	schema, err := ParseSchema("struct<int1:int>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	numValues := 100
	values := make([]interface{}, numValues)

	for i := 0; i < numValues; i++ {
		if i%5 == 0 {
			values[i] = nil
		} else {
			values[i] = int64(i)
		}
		err := w.Write(values[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read the writer output
	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	c := r.Select("int1")

	var row int
	for c.Stripes() {
		for c.Next() {
			val := c.Row()[0]
			if !reflect.DeepEqual(values[row], val) {
				t.Errorf("Test failed, expected %v, got %v", values[row], val)
			}
			row++
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if row != numValues {
		t.Errorf("Test failed, expected %v, got %v", numValues, row)
	}
}

func TestBooleanWriterAlternating(t *testing.T) {
	// Test was added to address issue: https://github.com/scritchley/orc/issues/61
	// Involves writing alternative true, nil, false values to a boolean column.

	f, err := ioutil.TempFile("", "testorc")
	if err != nil {
		t.Fatal(err)
	}

	filename := f.Name()
	defer os.Remove(filename)
	defer f.Close()

	schema, err := ParseSchema("struct<boolean1:boolean>")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(f, SetSchema(schema))
	if err != nil {
		t.Fatal(err)
	}

	length := 20000
	expected := make([]interface{}, length)

	for i := 0; i < length; i++ {
		if i%2 == 0 {
			err = w.Write(true)
			expected[i] = true

		} else if i%3 == 0 {
			err = w.Write(nil)
			expected[i] = nil
		} else {
			err = w.Write(false)
			expected[i] = false
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Read the writer output
	r, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	c := r.Select("boolean1")
	row := 0
	got := make([]interface{}, length)
	for c.Stripes() {
		for c.Next() {
			got[row] = c.Row()[0]
			row++
		}
	}

	if len(got) != len(expected) {
		t.Errorf("Test failed, got length %v expected %v", len(got), len(expected))
	}

	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	for i := range expected {
		if !reflect.DeepEqual(expected[i], got[i]) {
			t.Errorf("Test failed for row %v, expected %v got %v", i, expected[i], got[i])
		}
	}

}
