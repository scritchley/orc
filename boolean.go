package orc

import "io"

type BooleanStreamReader struct {
	*ByteStreamReader
	bitsInData int
	data       []byte
	err        error
	val        bool
}

func NewBooleanStreamReader(r io.ByteReader) *BooleanStreamReader {
	return &BooleanStreamReader{
		ByteStreamReader: NewByteStreamReader(r),
	}
}

func (b *BooleanStreamReader) Next() bool {
	// read more data if necessary
	if b.bitsInData == 0 {
		if len(b.data) > 1 {
			b.data = b.data[1:]
			b.bitsInData = 8
		} else if b.ByteStreamReader.Next() {
			byt, ok := b.ByteStreamReader.Bytes()
			if !ok {
				return false
			}
			if len(byt) > 0 {
				b.data = byt
				b.bitsInData = 8
			} else {
				return false
			}
		} else {
			return false
		}
	}
	b.val = (b.data[0] & 0x80) != 0
	// mark bit consumed
	b.data[0] = b.data[0] << 1
	b.bitsInData = b.bitsInData - 1
	return true
}

func (b *BooleanStreamReader) Bool() bool {
	return b.val
}

func (b *BooleanStreamReader) Value() interface{} {
	return b.Bool()
}

func (b *BooleanStreamReader) Error() error {
	return b.ByteStreamReader.Error()
}
