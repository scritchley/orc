package orc

import (
	"bytes"
	"fmt"
	"io"
	// "encoding/json"
	"math/rand"
	"os"
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

	filename := "./testorc"

	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	schema, err := ParseSchema("struct<string1:string,int1:int,boolean1:boolean>")
	// schema, err := ParseSchema("struct<string1:string,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(f, SetSchema(schema))
	if err != nil {
		t.Fatal(err)
	}

	length := 100000
	for i := 0; i < length; i++ {
		string1 := fmt.Sprintf("%x", rand.Int63n(1000))
		int1 := rand.Int63n(10000)
		boolean1 := int1 > 4444
		// double1 := rand.Float64()
		// nested := []interface{}{
		// 	rand.Float64(),
		// 	[]interface{}{
		// 		rand.Int63n(10000),
		// 	},
		// }
		// err = w.Write(string1, int1, boolean1, double1, nested)
		err = w.Write(string1, int1, boolean1)
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

	c := r.Select("*")
	row := 0
	for c.Stripes() {
		for c.Next() {
			// t.Log(c.Row())
			row++
		}
	}

	if err := c.Err(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if row != length {
		t.Errorf("Test failed, expected %v rows got %v", length, row)
	}

}
