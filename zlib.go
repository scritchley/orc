package orc

import (
	"bufio"
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

const (
	headerLength = 3
)

func zlibDecoder(r io.Reader) (io.Reader, error) {

	br := bufio.NewReader(r)

	b0, err := br.ReadByte()
	b1, err := br.ReadByte()
	b2, err := br.ReadByte()

	if err != nil {
		return nil, err
	}

	isUncompressed := bool((b0 & 0x01) == 1)

	chunkLength := (int(b2) << 15) | (int(b1) << 7) | int((uint(b0) >> 1))

	originalChunk := make([]byte, chunkLength)

	n, err := br.Read(originalChunk)
	if err != nil {
		return nil, err
	}
	if n != chunkLength {
		return nil, fmt.Errorf("read unexpected number of bytes, got %v, expected %v", n, chunkLength)
	}

	chunkReader := bytes.NewReader(originalChunk)

	if isUncompressed {
		return chunkReader, nil
	}

	return flate.NewReader(chunkReader), nil
}
