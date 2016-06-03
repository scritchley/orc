package orc

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestBooleanReader(t *testing.T) {
	testCases := []struct {
		input  []byte
		expect func([]bool)
	}{
		{
			input: []byte{0xff, 0x80},
			expect: func(output []bool) {
				expected := []bool{true, false, false, false, false, false, false, false}
				if !reflect.DeepEqual(expected, output) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		r := NewBooleanReader(bytes.NewReader(tc.input))
		var output []bool
		for r.HasNext() {
			b := r.NextBool()
			output = append(output, b)
		}
		if err := r.Error(); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		tc.expect(output)
	}

}
