package orc

// import (
// 	"fmt"
// 	"io"
// )

// type BooleanStreamReader struct {
// 	r          io.ByteReader
// 	bitsInData int
// 	data       byte
// 	buffer     []byte
// 	err        error
// 	val        bool
// }

// func NewBooleanStreamReader(r io.ByteReader) *BooleanStreamReader {
// 	return &BooleanStreamReader{
// 		r: r,
// 	}
// }

// func (b *BooleanStreamReader) Next() bool {
// 	// read more data if necessary
// 	if b.bitsInData == 0 {
// 		if len(buffer) == 0 {
// 			// Read the header byte
// 			b0, err := b.r.ReadByte()
// 			if err != nil {
// 				b.err = err
// 				return false
// 			}
// 			// Extract the run length
// 			length := int8(b0)
// 			// Populate the buffer with the next set of values
// 			for i := 0; i < length; i++ {
// 				nb, err := b.r.ReadByte()
// 				if err != nil {
// 					b.err = err
// 					return false
// 				}
// 			}
// 		}
// 		b.bitsInData = 8
// 		b.data = nb
// 	}
// 	b.val = (b.data & 0x80) != 0
// 	// mark bit consumed
// 	b.data <<= 1
// 	b.bitsInData = b.bitsInData - 1
// 	return true
// }

// func (b *BooleanStreamReader) Bool() bool {
// 	return b.val
// }

// func (b *BooleanStreamReader) Error() error {
// 	return b.err
// }
