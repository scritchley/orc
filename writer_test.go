package orc

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
	// "encoding/json"
	"math/rand"
	"testing"
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

	// schema, err := ParseSchema("struct<string1:string,int1:int,boolean1:boolean>")
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
	length := 1000000
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
		// err = w.Write(string1, int1, boolean1)
		// err = w.Write(string1)
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
