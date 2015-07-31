package orc

import "io"

type ByteStreamReader struct {
	r   io.ByteReader
	buf []byte
	err error
}

func NewByteStreamReader(r io.ByteReader) *ByteStreamReader {
	return &ByteStreamReader{
		r: r,
	}
}

func (b *ByteStreamReader) Next() bool {
	if len(b.buf) == 0 {
		nb, err := readBytes(b.r)
		if err != nil {
			b.err = err
			return false
		}
		b.buf = nb
	}
	return true
}

func (b *ByteStreamReader) Byte() (byte, bool) {
	l := len(b.buf)
	if l > 0 {
		r := b.buf[0]
		if l == 1 {
			b.buf = nil
		} else {
			b.buf = b.buf[1:]
		}
		return r, true
	}
	return 0x00, false
}

func (b *ByteStreamReader) Error() error {
	return b.err
}

// readBytes reads the control byte from r to extract run length, it
// then reads the remaining bytes for the run and returns them as a byte
// slice along with any errors that occur.
func readBytes(r io.ByteReader) ([]byte, error) {
	// Read the header byte
	b0, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	// Extract the run length
	length := int8(b0)
	if length < 0 {
		length = -length
	} else {
		length += MinRepeatSize
	}
	// Create a slice of bytes to return
	literals := make([]byte, length)
	// Populate the buffer with the next set of values
	for i := 0; i < int(length); i++ {
		b, err := r.ReadByte()
		if err != nil {
			return literals, err
		}
		literals[i] = b
	}
	return literals, nil
}
