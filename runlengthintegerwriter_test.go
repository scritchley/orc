package orc

import (
	"bytes"
	"math/rand"
	// "reflect"
	"testing"
)

func TestRunLengthIntegerWriter(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []int64
		expect func([]byte)
	}{
	// {
	// 	signed: false,
	// 	input:  makeInt64Slice(7, nil, 100),
	// 	expect: func(output []byte) {
	// 		expected := []byte{0x61, 0xff, 0x64}
	// 		if !reflect.DeepEqual(output, expected) {
	// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
	// 		}
	// 	},
	// },
	// {
	// 	signed: false,
	// 	input:  []int64{2, 3, 4, 7, 11},
	// 	expect: func(output []byte) {
	// 		expected := []byte{0xfb, 0x02, 0x03, 0x04, 0x07, 0xb}
	// 		if !reflect.DeepEqual(output, expected) {
	// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
	// 		}
	// 	},
	// },
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		w := NewRunLengthIntegerWriter(&buf, tc.signed)
		for i := range tc.input {
			err := w.WriteInt(tc.input[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		tc.expect(buf.Bytes())
	}
}

func TestWriteReadRunLengthIntegerWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewRunLengthIntegerWriter(&buf, true)
	var input []int64
	for i := 0; i < 1000000; i++ {
		b := rand.Int63n(1000000)
		input = append(input, b)
		err := w.WriteInt(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunLengthIntegerReader(&buf, true)
	var index int
	for r.Next() {
		b := r.Int()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}

func TestWriteReadRunLengthIntegerWriterRun(t *testing.T) {
	var buf bytes.Buffer
	w := NewRunLengthIntegerWriter(&buf, true)
	var input []int64
	for i := 0; i < 1000000; i++ {
		b := rand.Int63n(2)
		input = append(input, b)
		err := w.WriteInt(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunLengthIntegerReader(&buf, true)
	var index int
	for r.Next() {
		b := r.Int()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}
