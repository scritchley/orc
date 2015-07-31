package orc

import (
	"bytes"
	"io"
	"reflect"
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
