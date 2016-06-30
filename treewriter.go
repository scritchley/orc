package orc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"code.simon-critchley.co.uk/orc/proto"
)

// BufferedWriter wraps a *bufio.Writer and records the current
// position of the writer prior to flushing to the underlying.
type BufferedWriter struct {
	*bufio.Writer
	*bytes.Buffer
	written uint64
}

// NewBufferedWriter returns a new BufferedWriter using the provided
// CompressionCodec.
func NewBufferedWriter(codec CompressionCodec) *BufferedWriter {
	buf := &bytes.Buffer{}
	return &BufferedWriter{
		Writer: bufio.NewWriterSize(
			codec.Encoder(buf),
			int(2*DefaultCompressionChunkSize),
		),
		Buffer: buf,
	}
}

// WriteByte writes a byte to the underlying buffer an increments the total
// number of bytes written.
func (b *BufferedWriter) WriteByte(c byte) error {
	b.written++
	return b.Writer.WriteByte(c)
}

// Write writes the provided byte slice to the underlying buffer an increments
// the total number of bytes written.
func (b *BufferedWriter) Write(p []byte) (int, error) {
	b.written += uint64(len(p))
	return b.Writer.Write(p)
}

// Position returns the number of bytes written to the underlying io.Writer
// not including the buffered bytes.
func (b *BufferedWriter) Position() uint64 {
	return b.written - uint64(b.Buffered())
}

// Len returns the number of bytes that have been written to the buffered
// writer, including bytes that have been buffered but not flushed to the
// underlying writer.
func (b *BufferedWriter) Len() uint64 {
	return b.written + uint64(b.Buffered())
}

// Close flushes any buffered bytes to the underlying writer.
func (b *BufferedWriter) Close() error {
	return b.Writer.Flush()
}

// TreeWriter is an interface for writing to a stream.
type TreeWriter interface {
	// ColumnEncoding returns the column encoding used for the TreeWriter.
	ColumnEncoding() *proto.ColumnEncoding
	// Write writes the interface value i to the TreeWriter, it returns an error
	// if i is of an unexpected type or if an error occurs whilst writing to
	// the underlying stream.
	Write(i interface{}) error
	// Close flushes the remaining data and closes the writer.
	Close() error
	// Flush flushes any outstanding data to the underlying writer.
	Flush() error
}

// BaseTreeWriter is a TreeWriter implementation that writes to the present stream. It
// is the basis for all other TreeWriter implementations.
type BaseTreeWriter struct {
	*BooleanWriter
	*BufferedWriter
	statistics      ColumnStatistics
	indexStatistics ColumnStatistics
}

// NewBaseTreeWriter is a TreeWriter that writes to a present stream.
func NewBaseTreeWriter(isPresent *BufferedWriter, statistics, indexStatistics ColumnStatistics) BaseTreeWriter {
	// Create a buffered writer.
	return BaseTreeWriter{
		BooleanWriter:   NewBooleanWriter(isPresent),
		BufferedWriter:  isPresent,
		statistics:      statistics,
		indexStatistics: indexStatistics,
	}
}

// Write checks whether i is nil and writes an appropriate true or false value to
// the underlying isPresent stream.
func (b BaseTreeWriter) Write(i interface{}) error {
	// Add the value to the statistics
	b.statistics.Add(i)
	b.indexStatistics.Add(i)
	// isPresent is optional, therefore, support nil BooleanWriter
	if b.BooleanWriter == nil {
		return nil
	}
	if i == nil {
		// If interface value is nil, then write false to isPresent stream.
		return b.BooleanWriter.WriteBool(false)
	}
	// Otherwise write a true value to the isPresent stream.
	return b.BooleanWriter.WriteBool(true)
}

// Close flushes the underlying BufferedWriter returning an error if one occurs.
func (b BaseTreeWriter) Close() error {
	if err := b.BooleanWriter.Close(); err != nil {
		return err
	}
	return b.BufferedWriter.Close()
}

// Flush flushes the underlying BufferedWriter returning an error if one occurs.
func (b BaseTreeWriter) Flush() error {
	if err := b.BooleanWriter.Flush(); err != nil {
		return err
	}
	return b.BufferedWriter.Flush()
}

// IntegerWriter is an interface implemented by all integer type writers.
type IntegerWriter interface {
	WriteInt(value int64) error
	Close() error
	Flush() error
}

func createIntegerWriter(kind proto.ColumnEncoding_Kind, w io.ByteWriter, signed bool) (IntegerWriter, error) {
	switch kind {
	case proto.ColumnEncoding_DIRECT_V2, proto.ColumnEncoding_DICTIONARY_V2:
		return NewRunLengthIntegerWriterV2(w, signed), nil
	case proto.ColumnEncoding_DIRECT, proto.ColumnEncoding_DICTIONARY:
		return NewRunLengthIntegerWriter(w, signed), nil
	default:
		return nil, fmt.Errorf("unknown encoding: %s", kind)
	}
}

// IntegerTreeWriter is a TreeWriter implementation that writes an integer type column.
type IntegerTreeWriter struct {
	BaseTreeWriter
	IntegerWriter
	*BufferedWriter
	encoding *proto.ColumnEncoding
}

// NewIntegerTreeWriter returns a new IntegerTreeWriter writing to the provided present and data writers.
func NewIntegerTreeWriter(present, data *BufferedWriter, statistics, indexStatistics ColumnStatistics) (*IntegerTreeWriter, error) {
	// TODO: Inherit column encoding kind from orc.Writer ORC file version.
	iwriter, err := createIntegerWriter(proto.ColumnEncoding_DIRECT_V2, data, true)
	if err != nil {
		return nil, err
	}
	return &IntegerTreeWriter{
		BaseTreeWriter: NewBaseTreeWriter(present, statistics, indexStatistics),
		IntegerWriter:  iwriter,
		BufferedWriter: data,
		encoding: &proto.ColumnEncoding{
			Kind: proto.ColumnEncoding_DIRECT_V2.Enum(),
		},
	}, nil
}

// WriteInt writes an integer value returning an error if one occurs.
func (w *IntegerTreeWriter) WriteInt(value int64) error {
	return w.IntegerWriter.WriteInt(value)
}

// Write writes a value returning an error if one occurs. It accepts any form of
// integer or a nil value for writing nulls to the stream. Any other types will
// return an error.
func (w *IntegerTreeWriter) Write(value interface{}) error {
	switch t := value.(type) {
	case nil:
		// If the value is nil, return with no error. The value is null
		// and a false value will have been written to the present stream.
		// First write the value to the present column.
		if err := w.BaseTreeWriter.Write(value); err != nil {
			return err
		}
		return nil
	case int:
		// First write the value to the present column.
		if err := w.BaseTreeWriter.Write(int64(t)); err != nil {
			return err
		}
		return w.WriteInt(int64(t))
	case int32:
		// First write the value to the present column.
		if err := w.BaseTreeWriter.Write(int64(t)); err != nil {
			return err
		}
		return w.WriteInt(int64(t))
	case int64:
		// First write the value to the present column.
		if err := w.BaseTreeWriter.Write(t); err != nil {
			return err
		}
		return w.WriteInt(t)
	default:
		return fmt.Errorf("cannot write %T to integer column type", t)
	}
}

// Close closes the underlying writers returning an error if one occurs.
func (w *IntegerTreeWriter) Close() error {
	if err := w.BaseTreeWriter.Close(); err != nil {
		return err
	}
	if err := w.IntegerWriter.Close(); err != nil {
		return err
	}
	return w.BufferedWriter.Close()
}

// Flush flushes the underlying writers returning an error if one occurs.
func (w *IntegerTreeWriter) Flush() error {
	if err := w.BaseTreeWriter.Flush(); err != nil {
		return err
	}
	if err := w.IntegerWriter.Flush(); err != nil {
		return err
	}
	return w.BufferedWriter.Flush()
}

// ColumnEncoding returns the column encoding used for the IntegerTreeWriter.
func (w *IntegerTreeWriter) ColumnEncoding() *proto.ColumnEncoding {
	return w.encoding
}

// StructTreeWriter is a TreeWriter implementation that can write a struct column type.
type StructTreeWriter struct {
	BaseTreeWriter
	children []TreeWriter
}

// NewStructTreeWriter returns a StructTreeWriter using the provided io.Writer and children
// TreeWriters. It additionally returns an error if one occurs.
func NewStructTreeWriter(present *BufferedWriter, children []TreeWriter, statistics, indexStatistics ColumnStatistics) (*StructTreeWriter, error) {
	return &StructTreeWriter{
		BaseTreeWriter: NewBaseTreeWriter(present, statistics, indexStatistics),
		children:       children,
	}, nil
}

// Write writes a value to the underlying child TreeWriters. It returns
// an error if one occurs.
func (s *StructTreeWriter) Write(value interface{}) error {
	// First write the value to the present column.
	if err := s.BaseTreeWriter.Write(value); err != nil {
		return err
	}
	values, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("wrong type for struct tree reader, expected []interface{}, got:%T", value)
	}
	if len(values) != len(s.children) {
		return fmt.Errorf("wrong number of values, expected:%v, got:%v", len(s.children), len(values))
	}
	for i := range s.children {
		err := s.children[i].Write(values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the StructTreeWriter and its child TreeWriters returning an
// error if one occurs.
func (s *StructTreeWriter) Close() error {
	if err := s.BaseTreeWriter.Close(); err != nil {
		return err
	}
	for i := range s.children {
		err := s.children[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes the StructTreeWriter and its child TreeWriters returning an
// error if one occurs.
func (s *StructTreeWriter) Flush() error {
	if err := s.BaseTreeWriter.Flush(); err != nil {
		return err
	}
	for i := range s.children {
		err := s.children[i].Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

// ColumnEncoding returns the column encoding for the StructTreeWriter.
func (s *StructTreeWriter) ColumnEncoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}

type BooleanTreeWriter struct {
	BaseTreeWriter
	*BooleanWriter
	*BufferedWriter
}

func NewBooleanTreeWriter(isPresent, data *BufferedWriter, statistics, indexStatistics ColumnStatistics) (*BooleanTreeWriter, error) {
	return &BooleanTreeWriter{
		BaseTreeWriter: NewBaseTreeWriter(isPresent, statistics, indexStatistics),
		BooleanWriter:  NewBooleanWriter(data),
		BufferedWriter: data,
	}, nil
}

func (b *BooleanTreeWriter) Write(value interface{}) error {
	if value == nil {
		return b.BaseTreeWriter.Write(value)
	}
	if bv, ok := value.(bool); ok {
		if err := b.BaseTreeWriter.Write(true); err != nil {
			return err
		}
		return b.BooleanWriter.WriteBool(bv)
	}
	return fmt.Errorf("expected bool or nil value, received %T", value)
}

func (b *BooleanTreeWriter) Close() error {
	if err := b.BaseTreeWriter.Close(); err != nil {
		return err
	}
	if err := b.BooleanWriter.Close(); err != nil {
		return err
	}
	return b.BufferedWriter.Close()
}

func (b *BooleanTreeWriter) Flush() error {
	if err := b.BaseTreeWriter.Flush(); err != nil {
		return err
	}
	if err := b.BooleanWriter.Flush(); err != nil {
		return err
	}
	return b.BufferedWriter.Flush()
}

func (b *BooleanTreeWriter) ColumnEncoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}

type FloatTreeWriter struct {
	BaseTreeWriter
	*BufferedWriter
	bytesPerValue int
}

func NewFloatTreeWriter(isPresent, data *BufferedWriter, statistics, indexStatistics ColumnStatistics, bytesPerValue int) (*FloatTreeWriter, error) {
	return &FloatTreeWriter{
		BaseTreeWriter: NewBaseTreeWriter(isPresent, statistics, indexStatistics),
		BufferedWriter: data,
		bytesPerValue:  bytesPerValue,
	}, nil
}

func (f *FloatTreeWriter) Write(value interface{}) error {
	if err := f.BaseTreeWriter.Write(value); err != nil {
		return err
	}
	if f.bytesPerValue == 8 {
		return f.WriteDouble(value)
	}
	return f.WriteFloat(value)
}

func (f *FloatTreeWriter) WriteDouble(value interface{}) error {
	if val, ok := value.(float64); ok {
		byt := make([]byte, f.bytesPerValue)
		binary.LittleEndian.PutUint64(byt, math.Float64bits(val))
		_, err := f.BufferedWriter.Write(byt)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("expected float64 value, received: %T", value)
}

func (f *FloatTreeWriter) WriteFloat(value interface{}) error {
	if val, ok := value.(float32); ok {
		byt := make([]byte, f.bytesPerValue)
		binary.LittleEndian.PutUint32(byt, math.Float32bits(val))
		_, err := f.BufferedWriter.Write(byt)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("expected float32 value, received: %T", value)
}

func (f *FloatTreeWriter) Close() error {
	if err := f.BaseTreeWriter.Close(); err != nil {
		return err
	}
	return f.BufferedWriter.Close()
}

func (f *FloatTreeWriter) Flush() error {
	if err := f.BaseTreeWriter.Flush(); err != nil {
		return err
	}
	return f.BufferedWriter.Flush()
}

func (f *FloatTreeWriter) ColumnEncoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}
