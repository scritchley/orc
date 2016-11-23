package orc

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestRunLengthIntegerReaderV2(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []byte
		expect func([]int64)
	}{
		// {
		// 	signed: false,
		// 	input:  []byte{2, 1, 64, 5, 80, 1, 1},
		// 	expect: func(output []int64) {
		// 		expected := []int64{1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		{
			// Patched Base
			signed: false,
			input:  []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8},
			expect: func(output []int64) {
				expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		// {
		// 	// Patched Base
		// 	signed: false,
		// 	input:  []byte{110, 9, 0, 7, 238, 0, 7, 208, 0, 7, 228, 15, 66, 64, 0, 7, 248, 0, 8, 2, 0, 8, 12, 0, 8, 22, 0, 8, 32, 0, 8, 42},
		// 	expect: func(output []int64) {
		// 		expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	// Direct
		// 	signed: false,
		// 	input:  []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef},
		// 	expect: func(output []int64) {
		// 		expected := []int64{23713, 43806, 57005, 48879}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	// Delta
		// 	signed: false,
		// 	input:  []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46},
		// 	expect: func(output []int64) {
		// 		expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	// Short Repeat
		// 	signed: false,
		// 	input:  []byte{0x0a, 0x27, 0x10},
		// 	expect: func(output []int64) {
		// 		expected := []int64{10000, 10000, 10000, 10000, 10000}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	signed: false,
		// 	input:  []byte{102, 9, 0, 126, 224, 7, 208, 0, 126, 79, 66, 64, 0, 127, 128, 8, 2, 0, 128, 192, 8, 22, 0, 130, 0, 8, 42},
		// 	expect: func(output []int64) {
		// 		expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	signed: false,
		// 	input:  []byte{196, 9, 2, 2, 74, 40, 166},
		// 	expect: func(output []int64) {
		// 		expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	signed: false,
		// 	input:  []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46},
		// 	expect: func(output []int64) {
		// 		expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
		// {
		// 	signed: false,
		// 	input:  []byte{7, 1},
		// 	expect: func(output []int64) {
		// 		expected := []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
		// 		if !reflect.DeepEqual(output, expected) {
		// 			t.Errorf("Test failed, expected %v to equal %v", output, expected)
		// 		}
		// 	},
		// },
	}

	for _, tc := range testCases {
		r := NewRunLengthIntegerReaderV2(bytes.NewReader(tc.input), tc.signed, false)
		var output []int64
		for r.Next() {
			v := r.Int()
			output = append(output, v)
		}
		if err := r.Err(); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		tc.expect(output)
	}

}
