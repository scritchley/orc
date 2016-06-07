package orc

import (
	"os"
	"testing"

	"code.simon-critchley.co.uk/orc/proto"
)

func TestReader(t *testing.T) {

	orcFile, err := os.Open("./examples/orc-file-11-format.orc")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(FileReader{orcFile})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// t.Log(r.streams)

	stream, err := r.streams.get(streamName{1, proto.Stream_DATA})
	if err != nil {
		t.Fatal(err)
	}

	// i := NewRunLengthIntegerReaderV2(&stream, true, false)
	// for i.HasNext() {
	// 	t.Log(i.NextInt())
	// }

	row := 0
	i := NewBooleanReader(&stream)
	for i.HasNext() {
		t.Log(row, i.NextBool())
		row++
	}

}
