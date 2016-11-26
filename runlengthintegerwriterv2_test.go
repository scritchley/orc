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
		{
			// Patched Base (Unsigned)
			signed: false,
			input: []int64{20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
				3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
				1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
				52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
				2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -13, 1, 2, 3,
				13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
				141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
				13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
				1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
				2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
				1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
				2, 16},
			expect: func(output []byte) {
				expected := []byte{144, 109, 4, 164, 141, 16, 131, 194, 0, 240, 112, 64, 60, 84, 24, 3, 193, 201, 128, 120, 60, 33, 4, 244, 3, 193, 192, 224, 128, 56, 32, 15, 22, 131, 129, 225, 0, 112, 84, 86, 14, 8, 106, 193, 192, 228, 160, 64, 32, 14, 213, 131, 193, 192, 240, 121, 124, 30, 18, 9, 132, 67, 0, 224, 120, 60, 28, 14, 32, 132, 65, 192, 240, 160, 56, 61, 91, 7, 3, 193, 192, 240, 120, 76, 29, 23, 7, 3, 220, 192, 240, 152, 60, 52, 15, 7, 131, 129, 225, 0, 144, 56, 30, 14, 44, 140, 129, 194, 224, 120, 0, 28, 15, 8, 6, 129, 198, 144, 128, 104, 36, 27, 11, 38, 131, 33, 48, 224, 152, 60, 111, 6, 183, 3, 112, 0, 1, 78, 5, 46, 2, 1, 1, 141, 3, 1, 1, 138, 22, 0, 65, 1, 4, 0, 225, 16, 209, 192, 4, 16, 8, 36, 16, 3, 48, 1, 3, 13, 33, 0, 176, 0, 1, 94, 18, 0, 68, 0, 33, 1, 143, 0, 1, 7, 93, 0, 25, 0, 5, 0, 2, 0, 4, 0, 1, 0, 1, 0, 2, 0, 16, 0, 1, 11, 150, 0, 3, 0, 1, 0, 1, 99, 157, 0, 1, 140, 54, 0, 162, 1, 130, 0, 16, 112, 67, 66, 0, 2, 4, 0, 0, 224, 0, 1, 0, 16, 64, 16, 91, 198, 1, 2, 0, 32, 144, 64, 0, 12, 2, 8, 24, 0, 64, 0, 1, 0, 0, 8, 48, 51, 128, 0, 2, 12, 16, 32, 32, 71, 128, 19, 76}
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
		reader := NewRunLengthIntegerReaderV2(&buf, tc.signed, false)
		var vals []int64
		for reader.Next() {
			vals = append(vals, reader.Int())
		}
		if !reflect.DeepEqual(tc.input, vals) {
			t.Fatalf("Test failed, expected %v got %v", tc.input, vals)
		}
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
