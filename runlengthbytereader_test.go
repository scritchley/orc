package orc

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestRunLengthByteReader(t *testing.T) {
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
		bs := NewRunLengthByteReader(bytes.NewReader(tc.input))
		var output []byte
		for bs.HasNext() {
			b := bs.NextByte()
			output = append(output, b)
		}
		if err := bs.Error(); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		tc.expect(output)
	}

}

func BenchmarkRunLengthByteReader(b *testing.B) {
	input := bytes.Repeat([]byte{0x61, 0x00}, b.N)
	bs := NewRunLengthByteReader(bytes.NewReader(input))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if bs.HasNext() {
			bs.NextByte()
		}
		break
	}
}
