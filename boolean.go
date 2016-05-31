package orc

import "io"

type BooleanStreamReader struct {
	*ByteStreamReader
	bitsInData int
	data       byte
	err        error
	val        bool
}

func NewBooleanStreamReader(r io.ByteReader) *BooleanStreamReader {
	return &BooleanStreamReader{
		ByteStreamReader: NewByteStreamReader(r),
	}
}

func (b *BooleanStreamReader) HasNext() bool {
	// read more data if necessary
	if b.bitsInData == 0 {
		if b.ByteStreamReader.HasNext() {
			byt, ok := b.ByteStreamReader.NextByte()
			if !ok {
				return false
			}
			b.data = byt
			b.bitsInData = 8
		} else {
			return false
		}
	}
	b.val = (b.data & 0x80) != 0
	// mark bit consumed
	b.data <<= 1
	b.bitsInData--
	return true
}

func (b *BooleanStreamReader) NextBool() bool {
	return b.val
}

func (b *BooleanStreamReader) Value() interface{} {
	return b.NextBool()
}

type BooleanStreamWriter struct {
	*ByteStreamWriter
	bitsInData int
	data       byte
}

func NewBooleanStreamWriter(w io.ByteWriter) *BooleanStreamWriter {
	return &BooleanStreamWriter{
		ByteStreamWriter: NewByteStreamWriter(w),
	}
}

func (b *BooleanStreamWriter) WriteBool(t bool) error {
	// If bitsInData is equal to 8 then write the byte
	// to the underlying ByteStreamWriter.
	if b.bitsInData >= 8 {
		err := b.Flush()
		if err != nil {
			return err
		}
	}
	if t {
		// If true, toggle the bit at relevant position.
		b.data |= (1 << uint(7-b.bitsInData))
	}
	b.bitsInData++
	return nil
}

func (b *BooleanStreamWriter) Flush() error {
	if b.bitsInData > 0 {
		err := b.ByteStreamWriter.WriteByte(b.data)
		if err != nil {
			return err
		}
		b.bitsInData = 0
		b.data = 0
	}
	return b.ByteStreamWriter.Flush()
}

func (b *BooleanStreamWriter) Close() error {
	return b.Flush()
}
