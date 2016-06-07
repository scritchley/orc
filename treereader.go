package orc

import (
	"bufio"
	"code.simon-critchley.co.uk/orc/proto"
	"io"
)

type Iterator interface {
	HasNext() bool
	Next() interface{}
	Err() error
}

// TreeReader is an interface implemented by all column types.
type TreeReader interface {
	Iterator
	AddStream(stream *proto.Stream, r io.Reader) error
}

type DecimalTreeReaderV2 struct {
	precision   int
	scale       int
	valueStream *RunLengthIntegerReaderV2
	scaleReader *RunLengthIntegerReaderV2
}

func (d *DecimalTreeReaderV2) AddStream(stream *proto.Stream, r io.Reader) error {
	b := bufio.NewReader(r)
	switch stream.GetKind() {
	case proto.Stream_ROW_INDEX:
	case proto.Stream_PRESENT:
	case proto.Stream_DATA:
	case proto.Stream_SECONDARY:
		d.scaleReader = NewRunLengthIntegerReaderV2(b, false, false)
	}
	return nil
}

func NewDecimalTreeReaderV2(column *proto.ColumnEncoding, ty *proto.Type) (*DecimalTreeReaderV2, error) {
	return &DecimalTreeReaderV2{
		precision: int(ty.GetPrecision()),
		scale:     int(ty.GetScale()),
	}, nil
}
func (d *DecimalTreeReaderV2) HasNext() bool {
	return d.scaleReader.HasNext()
}

func (d *DecimalTreeReaderV2) Next() interface{} {
	return d.scaleReader.NextInt()
}

func (d *DecimalTreeReaderV2) Err() error {
	return d.scaleReader.Err()
}

type BooleanTreeReader struct {
	presentStream *BooleanReader
	valueStream   *BooleanReader
}

func NewBooleanTreeReader() *BooleanTreeReader {
	return &BooleanTreeReader{}
}
func (b *BooleanTreeReader) AddStream(stream *proto.Stream, r io.Reader) error {
	br := bufio.NewReader(r)
	switch stream.GetKind() {
	case proto.Stream_PRESENT:
		b.presentStream = NewBooleanReader(br)
	case proto.Stream_DATA:
		b.valueStream = NewBooleanReader(br)
	}
	return nil
}

func (b *BooleanTreeReader) HasNext() bool {
	return b.valueStream.HasNext()
}

func (b *BooleanTreeReader) Next() interface{} {
	return b.valueStream.NextBool()
}

func (b *BooleanTreeReader) Err() error {
	return b.valueStream.Err()
}
