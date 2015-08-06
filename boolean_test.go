package orc

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestBooleanStreamReader(t *testing.T) {

	expected := []bool{true, false, false, false, false, false, false, false}

	input := []byte{0xff, 0x80}

	bs := NewBooleanStreamReader(bytes.NewReader(input))

	var output []bool

	for bs.Next() {
		output = append(output, bs.Bool())
	}

	if err := bs.Error(); err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(output, expected) {
		t.Errorf("Test failed, expected %v to equal %v", output, expected)
	}

}

func BenchmarkBooleanStreamReader(b *testing.B) {
	input := []byte(strings.Repeat(string([]byte{0xff, 0x80}), b.N))
	bs := NewBooleanStreamReader(bytes.NewReader(input))
	for i := 0; i < b.N; i++ {
		if bs.Next() {
			bs.Bool()
		}
		break
	}
}
