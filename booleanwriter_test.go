package orc

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestBooleanWriter(t *testing.T) {
	testCases := []struct {
		input  []bool
		expect func([]byte)
	}{
		{
			input: []bool{true, false, false, false, false, false, false, false},
			expect: func(output []byte) {
				expected := []byte{0xff, 0x80}
				if !reflect.DeepEqual(expected, output) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		w := NewBooleanWriter(&buf)
		for i := range tc.input {
			err := w.WriteBool(tc.input[i])
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

func TestWriteReadBools(t *testing.T) {
	var buf bytes.Buffer
	w := NewBooleanWriter(&buf)
	var input []bool
	for i := 0; i < 100000; i++ {
		var b bool
		if rand.Intn(2) == 1 {
			b = true
		}
		input = append(input, b)
		err := w.WriteBool(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r := NewBooleanReader(&buf)
	var index int
	for r.HasNext() {
		b := r.NextBool()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}
