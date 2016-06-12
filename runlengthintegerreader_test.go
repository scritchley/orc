package orc

import (
	"bytes"
	"reflect"
	"testing"
)

func progression(add int64) func(prev int64) int64 {
	return func(prev int64) int64 {
		return prev + add
	}
}

func makeInt64Slice(start int64, fn func(prev int64) int64, l int) []int64 {
	s := make([]int64, l)
	var prev int64
	prev, s[0] = start, start
	for i := range s[1:] {
		if fn != nil {
			prev = fn(prev)
			s[i+1] = prev
		} else {
			s[i+1] = start
		}
	}
	return s
}

func TestRunLengthIntegerReader(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []byte
		expect func([]int64)
	}{
		{
			signed: false,
			input:  []byte{0x61, 0x00, 0x07},
			expect: func(output []int64) {
				expected := makeInt64Slice(7, nil, 100)
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			signed: false,
			input:  []byte{0x61, 0xff, 0x64},
			expect: func(output []int64) {
				expected := makeInt64Slice(100, progression(-1), 100)
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			signed: false,
			input:  []byte{0xfb, 0x02, 0x03, 0x04, 0x07, 0xb},
			expect: func(output []int64) {
				expected := []int64{2, 3, 4, 7, 11}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		r := NewRunLengthIntegerReader(bytes.NewReader(tc.input), tc.signed)
		var output []int64
		for r.HasNext() {
			v := r.NextInt()
			output = append(output, v)
		}
		tc.expect(output)
	}
}
