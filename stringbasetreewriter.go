package orc

import (
	"io"
)

const (
	InitialDictionarySize = 4096
)

type StringBaseTreeWriter struct {
	stringOutput       io.ByteWriter
	lengthOutput       IntegerWriter
	rowOutput          IntegerWriter
	dictionary         *StringRedBlackTree
	rows               *DynamicIntSlice
	directLengthOutput IntegerWriter
}

func NewStringBaseTreeWriter(columnID int, schema TypeDescription, writer StreamFactory, nullable bool) (*StringBaseTreeWriter, error) {
	sbtw := &StringBaseTreeWriter{}

	return sbtw, nil
}
