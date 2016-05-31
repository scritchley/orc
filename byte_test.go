package orc

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"math/rand"
)

func TestByteStreamReader(t *testing.T) {
	testCases := []struct {
		input  []byte
		expect func([]byte)
	}{
		{
			input: []byte{0x61, 0x00},
			expect: func(output []byte) {
				if len(output) != 100 {
					t.Errorf("Test failed, expected len 100 but got %v", len(output))
				}
				for _, val := range output {
					if val != 0 {
						t.Errorf("Test failed, expected %v to equal %v", val, 0)
					}
				}
			},
		},
		{
			input: []byte{0x01, 0x01},
			expect: func(output []byte) {
				if len(output) != 4 {
					t.Errorf("Test failed, expected len 100 but got %v", len(output))
				}
				for _, val := range output {
					if val != 1 {
						t.Errorf("Test failed, expected %v to equal %v", val, 0)
					}
				}
			},
		},
		{
			input: []byte{0xfe, 0x44, 0x45},
			expect: func(output []byte) {
				if !reflect.DeepEqual([]byte{0x44, 0x45}, output) {
					t.Errorf("Test failed, got %v", output)
				}
			},
		},
	}

	for _, tc := range testCases {
		bs := NewByteStreamReader(bytes.NewReader(tc.input))
		var output []byte
		for bs.HasNext() {
			b, ok := bs.NextByte()
			if ok {
				output = append(output, b)
			}
		}
		if err := bs.Error(); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		tc.expect(output)
	}

}

func TestByteStreamWriter(t *testing.T) {
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
		w := NewByteStreamWriter(&buf)
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
	w := NewByteStreamWriter(&buf)
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
	r := NewByteStreamReader(&buf)
	var index int
	for r.HasNext() {
		b, ok := r.NextByte()
		if ok {
			if input[index] != b {
				t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
			}
			index++
		} else {
			t.Fatal("Invalid bytes returned")
		}
	}
}

func BenchmarkByteStreamReader(b *testing.B) {
	input := bytes.Repeat([]byte{0x61, 0x00}, b.N)
	bs := NewByteStreamReader(bytes.NewReader(input))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if bs.HasNext() {
			bs.NextByte()
		}
		break
	}
}
