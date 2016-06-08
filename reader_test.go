package orc

import (
	"os"
	"testing"

	"code.simon-critchley.co.uk/orc/proto"
)

func TestReader(t *testing.T) {

	orcFile, err := os.Open("./examples/TestOrcFile.columnProjection.orc")
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(FileReader{orcFile})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// t.Log(r)
	// present, err := r.streams.get(streamName{2, proto.Stream_PRESENT})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	data, err := r.streams.get(streamName{2, proto.Stream_DATA})
	if err != nil {
		t.Fatal(err)
	}
	// dictionary, err := r.streams.get(streamName{2, proto.Stream_DICTIONARY_DATA})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	length, err := r.streams.get(streamName{2, proto.Stream_LENGTH})
	if err != nil {
		t.Fatal(err)
	}

	sr, err := NewStringTreeReader(nil, &data, &length, nil, r.columns[2])
	if err != nil {
		t.Fatal(err)
	}

	for sr.HasNext() {
		t.Log(sr.Next())
	}

	if err := sr.Err(); err != nil {
		t.Fatal(err)
	}

}
