package orc

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
)

var (
	_ CompressionCodec = (*CompressionNone)(nil)
	_ CompressionCodec = (*CompressionSnappy)(nil)
	_ CompressionCodec = (*CompressionZlib)(nil)

	_ io.WriteCloser = (*CompressionZlibEncoder)(nil)
	_ io.WriteCloser = (*CompressionZlibEncoder)(nil)
	_ io.WriteCloser = (*CompressionZlibEncoder)(nil)
)

func TestCompressionHeader(t *testing.T) {
	testcases := []struct {
		chunkSize  int
		isOriginal bool
		expected   []byte
		isError    bool
	}{
		{9000000, false, []byte{}, true},
		{100000, false, []byte{0x40, 0x0d, 0x03}, false},
		{5, true, []byte{0x0b, 0x00, 0x00}, false},
	}

	for _, v := range testcases {
		header, err := compressionHeader(v.chunkSize, v.isOriginal)
		if err != nil && !v.isError {
			t.Error(err)
			continue
		}
		if err == nil && v.isError {
			t.Errorf("On input: Length %d and isOriginal %t -> Expected an error, but got none.", v.chunkSize, v.isOriginal)
		}
		if bytes.Compare(header, v.expected) != 0 {
			t.Errorf("On input: Length %d and isOriginal %t -> Expected header %x got %x", v.chunkSize, v.isOriginal, v.expected, header)
		}
	}
}

func TestCompressionZlib(t *testing.T) {
	c := CompressionZlib{}

	buf := make([]byte, 1<<17)
	_, err := rand.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	w := &bytes.Buffer{}
	r := w

	enc := c.Encoder(w)
	dec := c.Decoder(r)

	n, err := enc.Write(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Errorf("Buffer underflow. Expected to write %d, wrote %d", len(buf), n)
	}
	err = enc.Close()
	if err != nil {
		t.Fatal(err)
	}

	got, err := ioutil.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(buf, got) != 0 {
		t.Errorf("Input and output don't match: %v vs %v", buf, got)
	}
}
