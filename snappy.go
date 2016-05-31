package orc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

func snappyDecoder(r io.Reader) (io.Reader, error) {

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

	if isUncompressed {
		return bytes.NewReader(originalChunk), nil
	}

	var uncompressedChunk []byte

	uncompressedChunk, err = snappy.Decode(uncompressedChunk, originalChunk)

	return bytes.NewReader(uncompressedChunk), err

}
