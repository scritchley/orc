package orc

import (
	"bytes"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestZigZagEncoder(t *testing.T) {
	ints := []int64{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if int(zigzagEncode(v)) != i {
			t.Errorf("Test failed, expected %v to equal %v", v, i)
		}
	}
}

func TestZigZagDecode(t *testing.T) {
	ints := []int64{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if zigzagDecode(uint64(i)) != v {
			t.Errorf("Test failed, expected %v to equal %v", i, v)
		}
	}
}

func TestIntStreamReaderV2(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []byte
		expect func([]int64)
	}{
		{
			// Patched Base (Unsigned)
			signed: false,
			input:  []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8},
			expect: func(output []int64) {
				expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Direct
			signed: false,
			input:  []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef},
			expect: func(output []int64) {
				expected := []int64{23713, 43806, 57005, 48879}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Delta
			signed: false,
			input:  []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46},
			expect: func(output []int64) {
				expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			// Short Repeat
			signed: false,
			input:  []byte{0x0a, 0x27, 0x10},
			expect: func(output []int64) {
				expected := []int64{10000, 10000, 10000, 10000, 10000}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			signed: false,
			input:  []byte{102, 9, 0, 126, 224, 7, 208, 0, 126, 79, 66, 64, 0, 127, 128, 8, 2, 0, 128, 192, 8, 22, 0, 130, 0, 8, 42},
			expect: func(output []int64) {
				expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			signed: false,
			input:  []byte{200, 9, 16, 202, 117, 182, 51, 191, 64},
			expect: func(output []int64) {
				expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
		{
			signed: false,
			input:  []byte{196, 9, 2, 2, 74, 40, 166},
			expect: func(output []int64) {
				expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
				if !reflect.DeepEqual(output, expected) {
					t.Errorf("Test failed, expected %v to equal %v", output, expected)
				}
			},
		},
	}

	for _, tc := range testCases {
		r := NewIntStreamReaderV2(bytes.NewReader(tc.input), tc.signed)
		var output []int64
		for r.HasNext() {
			output = append(output, r.NextInt())
		}
		tc.expect(output)
	}

}

func TestIntStreamWriterV2(t *testing.T) {
	testCases := []struct {
		signed bool
		input  []int64
		expect func([]byte)
	}{
		{
			// Patched Base (Unsigned)
			signed: false,
			input:  []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090},
			expect: func(output []byte) {
				expected := []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8}
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
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		w := NewIntStreamWriterV2(&buf, tc.signed)
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

func TestWriteReadInts(t *testing.T) {
	var buf bytes.Buffer
	w := NewIntStreamWriterV2(&buf, true)
	var input []int64
	for i := 0; i < 1000; i++ {
		b := rand.Int63n(10)
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
	t.Log(len(input))
	t.Log(buf.Len())
	r := NewIntStreamReaderV2(&buf, true)
	var index int
	for r.HasNext() {
		b := r.NextInt()
		if input[index] != b {
			t.Errorf("Test failed, %v does not equal %v at index %v", b, input[index], index)
		}
		index++
	}
}

func BenchmarkzigzagEncodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagEncode(int64(i))
	}
}

func BenchmarkzigzagDecodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagDecode(uint64(i))
	}
}

func BenchmarkReadIntShortRepeat(b *testing.B) {
	input := []byte{0x0a, 0x27, 0x10}
	r := bytes.NewReader(input)
	for i := 0; i < b.N; i++ {
		readIntValues(r, false)
	}
}

func BenchmarkReadIntPatchedBase(b *testing.B) {
	input := []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8}
	r := bytes.NewReader(input)
	for i := 0; i < b.N; i++ {
		readIntValues(r, false)
	}
}

func BenchmarkReadIntDelta(b *testing.B) {
	input := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	r := bytes.NewReader(input)
	for i := 0; i < b.N; i++ {
		readIntValues(r, false)
	}
}

func BenchmarkReadIntDirect(b *testing.B) {
	input := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef}
	r := bytes.NewReader(input)
	for i := 0; i < b.N; i++ {
		readIntValues(r, false)
	}
}

func BenchmarkIntStreamReader(b *testing.B) {
	input := []byte(strings.Repeat(string([]byte{0x0a, 0x27, 0x10}), b.N))
	is := NewIntStreamReaderV2(bytes.NewReader(input), true)
	for i := 0; i < b.N; i++ {
		if is.HasNext() {
			is.NextInt()
		}
		break
	}
}
