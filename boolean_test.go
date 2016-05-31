package orc

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"
)

func TestBooleanStreamReader(t *testing.T) {
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
		r := NewBooleanStreamReader(bytes.NewReader(tc.input))
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

func TestBooleanStreamWriter(t *testing.T) {
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
		w := NewBooleanStreamWriter(&buf)
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
	w := NewBooleanStreamWriter(&buf)
	var input []bool
	for i := 0; i < 10000; i++ {
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
	r := NewBooleanStreamReader(&buf)
	var index int
	for r.HasNext() {
		b := r.NextBool()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}

func BenchmarkBooleanStreamReader(b *testing.B) {
	input := bytes.Repeat([]byte{0xff, 0x80}, b.N)
	bs := NewBooleanStreamReader(bytes.NewReader(input))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if bs.HasNext() {
			bs.NextBool()
		}
		break
	}
}
