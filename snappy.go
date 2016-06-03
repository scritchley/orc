package orc

import (
	"bufio"
	"io"

	"github.com/golang/snappy"
)

func SnappyEncoder(w io.Writer) *bufio.Writer {
	return bufio.NewWriter(snappy.NewWriter(w))
}

func SnappyDecoder(r io.Reader) *bufio.Reader {
	return bufio.NewReader(snappy.NewReader(r))
}
