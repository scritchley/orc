package orc

import (
	"io"
)

type RunLengthIntegerWriter struct {
	w io.ByteWriter
}

func NewRunLengthIntegerWriter(w io.ByteWriter, signed bool) *RunLengthIntegerWriter {
	return &RunLengthIntegerWriter{
		w:      w,
		signed: signed,
	}
}

func (w *RunLengthIntegerWriter) writeValues() error {

}
