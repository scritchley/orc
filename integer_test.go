package orc

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestZigZagEncodeeger(t *testing.T) {
	ints := []int{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if int(zigzagEncode(v)) != i {
			t.Errorf("Test failed, expected %v to equal %v", v, i)
		}
	}
}

func TestZigZagDecode(t *testing.T) {
	ints := []int{0, -1, 1, -2, 2, -3, 3, -4, 4, -5}
	for i, v := range ints {
		if zigzagDecode(uint(i)) != v {
			t.Errorf("Test failed, expected %v to equal %v", i, v)
		}
	}
}

func TestReadIntShortRepeat(t *testing.T) {
	expected := []int64{10000, 10000, 10000, 10000, 10000}
	input := []byte{0x0a, 0x27, 0x10}
	output, err := readIntValues(bytes.NewReader(input), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Test failed, expected %v to equal %v", output, expected)
	}
}

func TestReadIntDelta(t *testing.T) {
	expected := []int64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
	input := []byte{0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46}
	output, err := readIntValues(bytes.NewReader(input), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Test failed, expected %v to equal %v", output, expected)
	}
}

func TestReadIntDirect(t *testing.T) {
	expected := []int64{23713, 43806, 57005, 48879}
	input := []byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef}
	output, err := readIntValues(bytes.NewReader(input), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Test failed, expected %v to equal %v", output, expected)
	}
}

func TestReadIntPatchedBase(t *testing.T) {
	expected := []int64{2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090}
	input := []byte{0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8}
	output, err := readIntValues(bytes.NewReader(input), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Test failed, expected %v to equal %v", output, expected)
	}
}

func BenchmarkzigzagEncodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagEncode(i)
	}
}

func BenchmarkzigzagDecodeeger(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zigzagDecode(uint(i))
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
	input := []byte(strings.Repeat(string([]byte{0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef, 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46, 0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8}), b.N))
	is := NewIntStreamReader(bytes.NewReader(input), true)
	for i := 0; i < b.N; i++ {
		if is.Next() {
			is.Int()
		}
		break
	}
}
