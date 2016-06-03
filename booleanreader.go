package orc

import "io"

type BooleanReader struct {
	*RunLengthByteReader
	bitsInData int
	data       byte
	err        error
	val        bool
}

func NewBooleanReader(r io.ByteReader) *BooleanReader {
	return &BooleanReader{
		RunLengthByteReader: NewRunLengthByteReader(r),
	}
}

func (b *BooleanReader) HasNext() bool {
	// read more data if necessary
	if b.bitsInData == 0 {
		if b.RunLengthByteReader.HasNext() {
			byt := b.RunLengthByteReader.NextByte()
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

func (b *BooleanReader) NextBool() bool {
	return b.val
}

func (b *BooleanReader) Value() interface{} {
	return b.NextBool()
}
