package orc

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestRunLengthByteWriter(t *testing.T) {
	testCases := []struct {
		input  []byte
		expect func([]byte)
	}{
		{
			input: []byte{0x44, 0x45},
			expect: func(output []byte) {
				expected := []byte{0xfe, 0x44, 0x45}
				if !reflect.DeepEqual(expected, output) {
					t.Errorf("Test failed, got %v expected %v", output, expected)
				}
			},
		},
		{
			input: []byte{0x01, 0x01, 0x01, 0x01},
			expect: func(output []byte) {
				expected := []byte{0x01, 0x01}
				if !reflect.DeepEqual(expected, output) {
					t.Errorf("Test failed, got %v expected %v", output, expected)
				}
			},
		},
		{
			input: make([]byte, 100),
			expect: func(output []byte) {
				expected := []byte{0x61, 0x00}
				if !reflect.DeepEqual(expected, output) {
					t.Errorf("Test failed, got %v expected %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		w := NewRunLengthByteWriter(&buf)
		for i := range tc.input {
			err := w.WriteByte(tc.input[i])
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

func TestWriteReadBytes(t *testing.T) {
	var buf bytes.Buffer
	w := NewRunLengthByteWriter(&buf)
	var input []byte
	for i := 0; i < 10000; i++ {
		b := uint8(rand.Intn(1))
		input = append(input, b)
		err := w.WriteByte(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunLengthByteReader(&buf)
	var index int
	for r.HasNext() {
		b := r.NextByte()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}
