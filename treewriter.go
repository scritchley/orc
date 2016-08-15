package orc

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"code.simon-critchley.co.uk/orc/proto"
)

// TreeWriter is an interface for writing to a stream.
type TreeWriter interface {
	// Encoding returns the column encoding used for the TreeWriter.
	Encoding() *proto.ColumnEncoding
	// Write writes the interface value i to the TreeWriter, it returns an error
	// if i is of an unexpected type or if an error occurs whilst writing to
	// the underlying stream.
	Write(i interface{}) error
	// Close flushes the remaining data and closes the writer.
	Close() error
	// Flush flushes any outstanding data to the underlying writer.
	Flush() error
	// Streams returns a slice of streams for the TreeWriter.
	Streams() []Stream
	// RowIndex returns the RowIndex for the writer.
	RowIndex() *proto.RowIndex
	// Statistics
	Statistics() ColumnStatistics
}

// BaseTreeWriter is a TreeWriter implementation that writes to the present stream. It
// is the basis for all other TreeWriter implementations.
type BaseTreeWriter struct {
	category          Category
	codec             CompressionCodec
	present           *BooleanWriter
	buffer            *BufferedWriter
	currentStatistics ColumnStatistics
	statistics        ColumnStatistics
	positionRecorders PositionRecorders
	indexEntries      []*proto.RowIndexEntry
	streams           []Stream
	numValues         uint64
	hasNull           bool
}

// NewBaseTreeWriter is a TreeWriter that is embedded in all other TreeWriter implementations.
func NewBaseTreeWriter(category Category, codec CompressionCodec) BaseTreeWriter {
	b := BaseTreeWriter{
		category:          category,
		codec:             codec,
		statistics:        NewColumnStatistics(category),
		currentStatistics: NewColumnStatistics(category),
		indexEntries:      make([]*proto.RowIndexEntry, 0),
		positionRecorders: make(PositionRecorders, 0),
	}
	present := b.AddStream(proto.Stream_PRESENT.Enum())
	b.AddPositionRecorder(present)
	b.present = NewBooleanWriter(present.buffer)
	b.buffer = present.buffer
	return b
}

func (b *BaseTreeWriter) positions() []uint64 {
	var positions []uint64
	for _, recorder := range b.positionRecorders {
		switch b.codec.(type) {
		case CompressionNone:
			positions = append(positions, recorder.Positions()...)
		}
	}
	return positions
}

func (b *BaseTreeWriter) recordIndexEntry() {
	b.indexEntries = append(b.indexEntries, &proto.RowIndexEntry{
		Positions:  b.positions(),
		Statistics: b.currentStatistics.Statistics(),
	})
	b.currentStatistics = NewColumnStatistics(b.category)
	b.numValues = 0
}

// Write checks whether i is nil and writes an appropriate true or false value to
// the underlying isPresent stream.
func (b *BaseTreeWriter) Write(i interface{}) error {
	// Add the value to the statistics
	b.numValues++
	b.statistics.Add(i)
	b.currentStatistics.Add(i)
	// isPresent is optional, therefore, support nil BooleanWriter
	if b.present == nil {
		return nil
	}
	if i == nil {
		// The stream has nulls, therefore, set hasNull to
		// true and write the prior values to the stream.
		b.hasNull = true
		for j := uint64(1); j < b.numValues; j++ {
			err := b.present.WriteBool(true)
			if err != nil {
				return err
			}
		}
		// If interface value is nil, then write false to isPresent stream.
		return b.present.WriteBool(false)
	}
	if b.hasNull {
		// Write to the stream only if it has nulls.
		return b.present.WriteBool(true)
	}
	return nil
}

// Close flushes the underlying BufferedWriter returning an error if one occurs.
func (b *BaseTreeWriter) Close() error {
	if err := b.present.Close(); err != nil {
		return err
	}
	// If the column has no nulls then reset the
	// underlying buffer.
	if !b.statistics.Statistics().GetHasNull() {
		b.buffer.Reset()
		b.buffer.written = 0
	}
	return b.buffer.Close()
}

// Flush flushes the underlying BufferedWriter returning an error if one occurs.
func (b *BaseTreeWriter) Flush() error {
	b.recordIndexEntry()
	if err := b.present.Flush(); err != nil {
		return err
	}
	return b.buffer.Flush()
}

func (b *BaseTreeWriter) AddStream(kind *proto.Stream_Kind) Stream {
	s := Stream{
		kind:   kind,
		buffer: NewBufferedWriter(b.codec),
	}
	b.streams = append(b.streams, s)
	return s
}

func (b *BaseTreeWriter) AddPositionRecorder(recorder PositionRecorder) {
	b.positionRecorders = append(b.positionRecorders, recorder)
}

func (b *BaseTreeWriter) buffers() []*BufferedWriter {
	buffers := make([]*BufferedWriter, len(b.streams))
	for i := range b.streams {
		buffers[i] = b.streams[i].buffer
	}
	return buffers
}

func (b *BaseTreeWriter) Streams() []Stream {
	return b.streams
}

func (b *BaseTreeWriter) RowIndex() *proto.RowIndex {
	return &proto.RowIndex{
		Entry: b.indexEntries,
	}
}

func (b *BaseTreeWriter) Statistics() ColumnStatistics {
	return b.statistics
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

// NewIntegerTreeWriter returns a new IntegerTreeWriter.
func NewIntegerTreeWriter(category Category, codec CompressionCodec) (*IntegerTreeWriter, error) {
	base := NewBaseTreeWriter(category, codec)
	data := base.AddStream(proto.Stream_DATA.Enum())
	base.AddPositionRecorder(data)
	// TODO: Inherit column encoding kind from orc.Writer ORC file version.
	columnEncoding := proto.ColumnEncoding_DIRECT_V2
	iwriter, err := createIntegerWriter(columnEncoding, data.buffer, true)
	if err != nil {
		return nil, err
	}
	return &IntegerTreeWriter{
		BaseTreeWriter: base,
		IntegerWriter:  iwriter,
		BufferedWriter: data.buffer,
		encoding: &proto.ColumnEncoding{
			Kind: columnEncoding.Enum(),
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

// Encoding returns the column encoding used for the IntegerTreeWriter.
func (w *IntegerTreeWriter) Encoding() *proto.ColumnEncoding {
	return w.encoding
}

// StructTreeWriter is a TreeWriter implementation that can write a struct column type.
type StructTreeWriter struct {
	BaseTreeWriter
	children []TreeWriter
}

// NewStructTreeWriter returns a StructTreeWriter using the provided io.Writer and children
// TreeWriters. It additionally returns an error if one occurs.
func NewStructTreeWriter(category Category, codec CompressionCodec, children []TreeWriter) (*StructTreeWriter, error) {
	return &StructTreeWriter{
		BaseTreeWriter: NewBaseTreeWriter(category, codec),
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
		return fmt.Errorf("wrong type for struct tree reader, expected: %T, got: %T", []interface{}{}, value)
	}
	if len(values) != len(s.children) {
		return fmt.Errorf("wrong number of values, expected: %v, got: %v", len(s.children), len(values))
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

// Encoding returns the column encoding for the StructTreeWriter.
func (s *StructTreeWriter) Encoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}

type BooleanTreeWriter struct {
	BaseTreeWriter
	*BooleanWriter
	*BufferedWriter
}

func NewBooleanTreeWriter(category Category, codec CompressionCodec) (*BooleanTreeWriter, error) {
	base := NewBaseTreeWriter(category, codec)
	data := base.AddStream(proto.Stream_DATA.Enum())
	base.AddPositionRecorder(data)
	return &BooleanTreeWriter{
		BaseTreeWriter: base,
		BooleanWriter:  NewBooleanWriter(data.buffer),
		BufferedWriter: data.buffer,
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

func (b *BooleanTreeWriter) Encoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}

// FloatTreeWriter is a TreeWriter that writes to a Float or Double column type.
type FloatTreeWriter struct {
	BaseTreeWriter
	*BufferedWriter
	bytesPerValue int
}

// NewFloatTreeWriter returns a new FloatTreeWriter or an error if one occurs.
func NewFloatTreeWriter(category Category, codec CompressionCodec, bytesPerValue int) (*FloatTreeWriter, error) {
	base := NewBaseTreeWriter(category, codec)
	data := base.AddStream(proto.Stream_DATA.Enum())
	base.AddPositionRecorder(data)
	return &FloatTreeWriter{
		BaseTreeWriter: base,
		BufferedWriter: data.buffer,
		bytesPerValue:  bytesPerValue,
	}, nil
}

// Write writes a float or double value returning an error if one occurs.
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

func (f *FloatTreeWriter) Encoding() *proto.ColumnEncoding {
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT.Enum(),
	}
}

const (
	// InitialDictionarySize is the initial size used when creating the dictionary.
	InitialDictionarySize = 4096
	// DictionaryEncodingThreshold is the threshold ratio of unique items to the total count of items.
	DictionaryEncodingThreshold = 0.1
)

// StringTreeWriter is a TreeWriter implementation that writes to a string type column. It dynamically selects
// the most appropriate encoding format between direct and dictionary encoding based on the cardinality of the
// values up to the first call to Flush.
type StringTreeWriter struct {
	BaseTreeWriter
	data                  *BufferedWriter
	dictionaryData        *BufferedWriter
	lengths               *BufferedWriter
	lengthsIntWriter      IntegerWriter
	dictionaryEncodedData IntegerWriter
	dictionary            *Dictionary
	bufferedValues        []string
	numValues             int
	modeSelected          bool
	isDictionaryEncoded   bool
}

// NewStringTreeWriter returns a new StringTreeWriter or an error if one occurs.
func NewStringTreeWriter(category Category, codec CompressionCodec) (*StringTreeWriter, error) {
	base := NewBaseTreeWriter(category, codec)
	data := base.AddStream(proto.Stream_DATA.Enum())
	base.AddPositionRecorder(data)
	lengths := base.AddStream(proto.Stream_LENGTH.Enum())
	base.AddPositionRecorder(lengths)
	lengthsIntegerWriter, err := createIntegerWriter(proto.ColumnEncoding_DIRECT_V2, lengths.buffer, false)
	if err != nil {
		return nil, err
	}
	s := &StringTreeWriter{
		BaseTreeWriter:   base,
		data:             data.buffer,
		lengths:          lengths.buffer,
		lengthsIntWriter: lengthsIntegerWriter,
		bufferedValues:   make([]string, 0),
		dictionary:       NewDictionary(InitialDictionarySize),
	}
	return s, nil
}

// WriteString writes a string value to the StringTreeWriter returning an error if one occurs.
func (s *StringTreeWriter) WriteString(value string) error {

	// If no mode is selected then just buffer the values
	// and return nil
	if !s.modeSelected {
		s.numValues++
		s.bufferedValues = append(s.bufferedValues, value)
		return nil
	}

	// Get the existing size of the dictionary
	existingSize := s.dictionary.Size()
	// Get the index of the new value added to the dictionary.
	index := s.dictionary.add(value)

	// If dictionary encoding is being used, determine whether this is a new value,
	// if so, write it to the dictionary data stream and length stream.
	if s.isDictionaryEncoded {
		// The value is new if its index is equal to the existing size of the
		// dictionary.
		if index == existingSize {
			// Write the value to the dictionary stream.
			n, err := s.dictionaryData.Write([]byte(value))
			if err != nil {
				return err
			}
			// Write the length of the value to the length stream.
			err = s.lengthsIntWriter.WriteInt(int64(n))
			if err != nil {
				return err
			}
		}
		// Write the dictionary encoded value to the data stream.
		err := s.dictionaryEncodedData.WriteInt(int64(index))
		if err != nil {
			return err
		}
		return nil
	}

	// Write the value to the data stream
	_, err := s.data.Write([]byte(value))
	if err != nil {
		return err
	}

	// Write the length of the value to the length stream.
	err = s.lengthsIntWriter.WriteInt(int64(len(value)))
	if err != nil {
		return err
	}

	return nil
}

// Write writes the provided value to the underlying writers. It returns an
// error if the value is not a string type or if an error occurs during writing.
func (s *StringTreeWriter) Write(value interface{}) error {
	if str, ok := value.(string); ok {
		if err := s.BaseTreeWriter.Write(value); err != nil {
			return err
		}
		return s.WriteString(str)
	}
	return fmt.Errorf("expected string value, received: %T", value)
}

// Flush flushes the underlying writers returning an error if one occurs.
func (s *StringTreeWriter) Flush() error {
	if err := s.determineMode(); err != nil {
		return err
	}
	if err := s.flushBufferedValues(); err != nil {
		return err
	}
	if s.isDictionaryEncoded {
		if err := s.dictionaryEncodedData.Flush(); err != nil {
			return err
		}
		if err := s.dictionaryData.Flush(); err != nil {
			return err
		}
	}
	if err := s.data.Flush(); err != nil {
		return err
	}
	if err := s.lengthsIntWriter.Flush(); err != nil {
		return err
	}
	if err := s.BaseTreeWriter.Flush(); err != nil {
		return err
	}
	return s.lengths.Flush()
}

// Close closes the underlying writes returning an error if one occurs.
func (s *StringTreeWriter) Close() error {
	if s.isDictionaryEncoded {
		if err := s.dictionaryEncodedData.Close(); err != nil {
			return err
		}
		if err := s.dictionaryData.Close(); err != nil {
			return err
		}
	}
	if err := s.data.Close(); err != nil {
		return err
	}
	if err := s.lengthsIntWriter.Close(); err != nil {
		return err
	}
	if err := s.BaseTreeWriter.Close(); err != nil {
		return err
	}
	return s.lengths.Close()
}

// determineMode determines the mode of encoding to use when writing the values to the underlying
// streams. Once a mode is selected it cannot be altered until the next stripe is started.
func (s *StringTreeWriter) determineMode() error {
	// If the mode is already selected then return immediately.
	if s.modeSelected {
		return nil
	}
	// Determine whether dictionary encoding should be used.
	if s.useDictionaryEncoding() {
		s.isDictionaryEncoded = true
		var err error
		// Create an IntegerWriter for the dictionary encoded column.
		s.dictionaryEncodedData, err = createIntegerWriter(proto.ColumnEncoding_DICTIONARY_V2, s.data, false)
		if err != nil {
			return err
		}
		s.dictionaryData = s.BaseTreeWriter.AddStream(proto.Stream_DICTIONARY_DATA.Enum()).buffer
	}
	s.modeSelected = true
	return nil
}

// flushBufferedValues iterates through the bufferedValues and writes each of them back to the writer. This is
// called immediately after the writers mode has been determined so that the values are encoded using the
// appropriate method of either direct or dictionary encoding.
func (s *StringTreeWriter) flushBufferedValues() error {
	if len(s.bufferedValues) > 0 {
		for i := range s.bufferedValues {
			err := s.WriteString(s.bufferedValues[i])
			if err != nil {
				return err
			}
		}
	}
	s.bufferedValues = nil
	return nil
}

func (s *StringTreeWriter) useDictionaryEncoding() bool {
	// TODO: find better way to determine whether dictionary encoding should be
	// used. Currently this method is creating a new dictionary and using
	// it to check the cardinality against the threshold value.
	dict := NewDictionary(InitialDictionarySize)
	for i := range s.bufferedValues {
		dict.add(s.bufferedValues[i])
	}
	return float64(dict.Size())/float64(s.numValues) <= DictionaryEncodingThreshold
	// return false
}

// Encoding returns the column encoding for the writer, either DICTIONARY_V2 or DIRECT_V2.
func (s *StringTreeWriter) Encoding() *proto.ColumnEncoding {
	if s.isDictionaryEncoded {
		return &proto.ColumnEncoding{
			Kind: proto.ColumnEncoding_DICTIONARY_V2.Enum(),
		}
	}
	return &proto.ColumnEncoding{
		Kind: proto.ColumnEncoding_DIRECT_V2.Enum(),
	}
}
