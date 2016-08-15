package orc

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestRunLengthIntegerWriterV2(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []int64
		expect func([]byte)
	}{
		// {
		// 	// Patched Base (Unsigned)
		// 	signed: false,
		// 	input:  []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090},
		// 	expect: func(output []byte) {
		// 		expected := []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		{
			// Direct
			signed: false,
			input:  []int64{23713, 43806, 57005, 48879},
			expect: func(output []byte) {
				expected := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Direct
			signed: false,
			input:  []int64{23713, 43806, 57005, 48879},
			expect: func(output []byte) {
				expected := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Delta
			signed: false,
			input:  []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29},
			expect: func(output []byte) {
				expected := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Short Repeat
			signed: false,
			input:  []int64{10000, 10000, 10000, 10000, 10000},
			expect: func(output []byte) {
				expected := []byte{0x0a, 0x27, 0x10}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Short Repeat
			signed: false,
			input:  []int64{1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1},
			expect: func(output []byte) {
				expected := []byte{2, 1, 64, 5, 80, 1, 1}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Short Repeat
			signed: true,
			input: []int64{3551,
				51,
				7320,
				6148,
				9449,
				9287,
				8836,
				2873,
				4091,
				3331,
				9956,
				5637,
				1109,
				1650,
				8971,
				4443,
				8459,
				1739,
			},
			expect: func(output []byte) {
				expected := []byte{2, 1, 64, 5, 80, 1, 1}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		w := NewRunLengthIntegerWriterV2(&buf, tc.signed)
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

func TestWriteReadRunLengthIntegerWriterV2(t *testing.T) {
	var buf bytes.Buffer
	w := NewRunLengthIntegerWriterV2(&buf, true)
	var input []int64
	for i := 0; i < 1000000; i++ {
		b := rand.Int63()
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
	r := NewRunLengthIntegerReaderV2(&buf, true, false)
	var index int
	for r.Next() {
		b := r.Int()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
			break
		}
		index++
	}
}

func TestWriteReadRunLengthIntegerWriterV2Run(t *testing.T) {
	var buf bytes.Buffer
	w := NewRunLengthIntegerWriterV2(&buf, false)
	var input []int64
	for i := 0; i < 1000000; i++ {
		b := rand.Int63()
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
	r := NewRunLengthIntegerReaderV2(&buf, false, false)
	var index int
	for r.Next() {
		b := r.Int()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
			break
		}
		index++
	}
}
