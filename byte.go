package orc

import "io"

// ByteStreamReader reads a byte run length encoded stream from ByteReader r.
type ByteStreamReader struct {
	r    io.ByteReader
	data []byte
	err  error
}

func NewByteStreamReader(r io.ByteReader) *ByteStreamReader {
	return &ByteStreamReader{
		r: r,
	}
}

func (b *ByteStreamReader) HasNext() bool {
	if len(b.data) == 0 {
		nb, err := readBytes(b.r)
		if err != nil {
			b.err = err
			return false
		}
		b.data = nb
	}
	if len(b.data) == 0 {
		return false
	}
	return true
}

func (b *ByteStreamReader) NextByte() (byte, bool) {
	if len(b.data) > 0 {
		byt := b.data[0]
		b.data = b.data[1:]
		return byt, true
	}
	return 0x00, false
}

func (b *ByteStreamReader) Next() interface{} {
	v, ok := b.NextByte()
	if !ok {
		return nil
	}
	return v
}

func (b *ByteStreamReader) Error() error {
	return b.err
}

// readBytes reads the control byte from r to extract either the literal length
// or the run length, it then reads the remaining bytes and returns them as a byte
// slice along with any errors that occur.
func readBytes(r io.ByteReader) ([]byte, error) {
	// Read the header byte
	b0, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	// Extract the run length.
	length := int8(b0)
	if length >= 0 {
		// A length between 0 and 127 indicates a run.
		return readRun(r, length+MinRepeatSize)
	}
	// A length between -1 and -128 indicates a set of
	// literal values.
	return readLiteral(r, -length)
}

func readLiteral(r io.ByteReader, length int8) ([]byte, error) {
	literal := make([]byte, length, length)
	for i := 0; i < int(length); i++ {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		literal[i] = b
	}
	return literal, nil
}

func readRun(r io.ByteReader, length int8) ([]byte, error) {
	run := make([]byte, length, length)
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(length); i++ {
		run[i] = b
	}
	return run, nil
}

type ByteStreamWriter struct {
	io.ByteWriter
	prev *byte
	run  int8
	data []byte
}

func NewByteStreamWriter(w io.ByteWriter) *ByteStreamWriter {
	return &ByteStreamWriter{
		ByteWriter: w,
	}
}

func (b *ByteStreamWriter) WriteByte(c byte) error {
	if b.prev == nil {
		b.prev = &c
		b.data = []byte{c}
		return nil
	}
	switch {
	case *b.prev != c:
		if b.run > 0 {
			err := b.writeRun(*b.prev, b.run)
			if err != nil {
				return err
			}
			b.data = []byte{c}
			b.run = 0
		} else {
			b.data = append(b.data, c)
		}
	case *b.prev == c:
		b.run++
		if len(b.data) > 1 {
			err := b.writeLiteral(b.data[:len(b.data)-1]...)
			if err != nil {
				return err
			}
			b.data = b.data[len(b.data)-1:]
		}
	}
	b.prev = &c
	if len(b.data) >= 127 {
		return b.Flush()
	} else if b.run >= 127 {
		return b.Flush()
	}
	return nil
}

func (b *ByteStreamWriter) Flush() error {
	if b.run > 0 {
		err := b.writeRun(*b.prev, b.run)
		if err != nil {
			return err
		}
	} else {
		err := b.writeLiteral(b.data...)
		if err != nil {
			return err
		}
	}
	b.prev = nil
	b.run = 0
	b.data = []byte{}
	return nil
}

func (b *ByteStreamWriter) Close() error {
	return b.Flush()
}

func (b *ByteStreamWriter) writeRun(c byte, l int8) error {
	if l+1 < MinRepeatSize {
		var literal []byte
		for i := 0; i < int(l+1); i++ {
			literal = append(literal, c)
		}
		return b.writeLiteral(literal...)
	}
	err := b.ByteWriter.WriteByte(uint8(l + 1 - MinRepeatSize))
	if err != nil {
		return err
	}
	err = b.ByteWriter.WriteByte(c)
	if err != nil {
		return err
	}
	return nil
}

func (b *ByteStreamWriter) writeLiteral(c ...byte) error {
	l := int8(len(c))
	h := uint8(-l)
	err := b.ByteWriter.WriteByte(h)
	if err != nil {
		return err
	}
	for i := range c {
		err := b.ByteWriter.WriteByte(c[i])
		if err != nil {
			return err
		}
	}
	return nil
}
