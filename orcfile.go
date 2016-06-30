package orc

import (
	"os"
)

type EncodingStrategy int

const (
	EncodingStrategySpeed       EncodingStrategy = 0
	EncodingStrategyCompression EncodingStrategy = 1
)

type CompressionStrategy int

const (
	CompressionStrategySpeed       CompressionStrategy = 0
	CompressionStrategyCompression CompressionStrategy = 1
)

type Version struct {
	name  string
	major uint32
	minor uint32
}

var (
	Version0_11 = Version{"0.11", 0, 11}
	Version0_12 = Version{"0.12", 0, 12}
)

type fileReader struct {
	*os.File
}

func (f fileReader) Size() int64 {
	stats, err := f.Stat()
	if err != nil {
		return 0
	}
	return stats.Size()
}

func Open(name string) (*Reader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return NewReader(fileReader{f})
}
