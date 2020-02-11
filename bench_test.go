package orc

import (
	"bytes"
	"compress/flate"
	"math/rand"
	"fmt"
	"testing"
	"time"
)

func testWrite(writer *Writer) error {
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
		err := writer.Write(string1, timestamp1, int1, boolean1, double1, nested)
		if err != nil {
			return err
		}
	}

	err := writer.Close()
	return err
}

// BenchmarkWrite/write-12         	      31	  36687418 ns/op	10831782 B/op	  455401 allocs/op
func BenchmarkWrite(b *testing.B) {
	buf := &bytes.Buffer{}

	schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		b.Fatal(err)
	}

	w, err := NewWriter(buf, SetSchema(schema))
	if err != nil {
		b.Fatal(err)
	}


	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = testWrite(w)
		}
	})
}

// BenchmarkWriteSnappy/write-12         	      60	  19407485 ns/op	 7242888 B/op	  192506 allocs/op
func BenchmarkWriteSnappy(b *testing.B) {
	buf := &bytes.Buffer{}

	schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		b.Fatal(err)
	}

	w, err := NewWriter(buf, SetSchema(schema), SetCompression(CompressionSnappy{}))
	if err != nil {
		b.Fatal(err)
	}

	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = testWrite(w)
		}
	})
}


// BenchmarkWriteZlib/write-12         	      39	  29554911 ns/op	29303990 B/op	  191425 allocs/op
func BenchmarkWriteZlib(b *testing.B) {
	buf := &bytes.Buffer{}

	schema, err := ParseSchema("struct<string1:string,timestamp1:timestamp,int1:int,boolean1:boolean,double1:double,nested:struct<double2:double,nested:struct<int2:int>>>")
	if err != nil {
		b.Fatal(err)
	}

	w, err := NewWriter(buf, SetSchema(schema), SetCompression(CompressionZlib{Level: flate.DefaultCompression}))
	if err != nil {
		b.Fatal(err)
	}


	// Run the actual benchmark
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = testWrite(w)
		}
	})
}