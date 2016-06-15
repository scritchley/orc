package orc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"time"

	"code.simon-critchley.co.uk/orc/proto"
)

var (
	unsupportedFormat = fmt.Errorf("unsupported format")
)

type ValueReader interface {
	Next() bool
	Value() interface{}
	Err() error
}

// TreeReader is an interface that provides methods for reading an individual stream.
type TreeReader interface {
	ValueReader
	IsPresent() bool
}

// BaseTreeReader wraps a *BooleanReader and is used for reading the Present stream
// in all TreeReader implementations.
type BaseTreeReader struct {
	*BooleanReader
}

// NewBaseTreeReader return a new BaseTreeReader from the provided io.Reader.
func NewBaseTreeReader(r io.Reader) BaseTreeReader {
	if r == nil {
		return BaseTreeReader{}
	}
	return BaseTreeReader{NewBooleanReader(bufio.NewReader(r))}
}

// IsPresent returns true if a value is available and is present in the stream.
func (b BaseTreeReader) Next() bool {
	if b.BooleanReader != nil {
		return b.BooleanReader.Next()
	}
	return true
}

// IsPresent returns true if a value is available and is present in the stream.
func (b BaseTreeReader) IsPresent() bool {
	if b.BooleanReader != nil {
		return b.BooleanReader.Bool()
	}
	return true
}

// Err returns the last error to occur.
func (b BaseTreeReader) Err() error {
	if b.BooleanReader != nil {
		return b.BooleanReader.Err()
	}
	return nil
}

// IntegerReader is an interface that provides methods for reading an integer stream.
type IntegerReader interface {
	ValueReader
	Int() int64
}

// IntegerTreeReader is a TreeReader that can read Integer type streams.
type IntegerTreeReader struct {
	BaseTreeReader
	IntegerReader
}

// IsPresent implements the TreeReader interface.
func (i *IntegerTreeReader) IsPresent() bool {
	return i.BaseTreeReader.IsPresent()
}

// Next implements the TreeReader interface.
func (i *IntegerTreeReader) Next() bool {
	return i.BaseTreeReader.Next() && i.IntegerReader.Next()
}

// Err implements the TreeReader interface.
func (i *IntegerTreeReader) Err() error {
	if err := i.IntegerReader.Err(); err != nil {
		return err
	}
	return i.BaseTreeReader.Err()
}

// NewIntegerTreeReader returns a new IntegerReader or an error if one occurs.
func NewIntegerTreeReader(present, data io.Reader, encoding *proto.ColumnEncoding) (*IntegerTreeReader, error) {
	ireader, err := createIntegerReader(encoding.GetKind(), data, true, false)
	if err != nil {
		return nil, err
	}
	return &IntegerTreeReader{
		NewBaseTreeReader(present),
		ireader,
	}, nil
}

func createIntegerReader(kind proto.ColumnEncoding_Kind, in io.Reader, signed, skipCorrupt bool) (IntegerReader, error) {
	switch kind {
	case proto.ColumnEncoding_DIRECT_V2, proto.ColumnEncoding_DICTIONARY_V2:
		return NewRunLengthIntegerReaderV2(bufio.NewReader(in), signed, skipCorrupt), nil
	case proto.ColumnEncoding_DIRECT, proto.ColumnEncoding_DICTIONARY:
		return NewRunLengthIntegerReader(bufio.NewReader(in), signed), nil
	default:
		return nil, fmt.Errorf("unknown encoding: %s", kind)
	}
}

const (
	// TimestampBaseSeconds is 1 January 2015, the base value for all timestamp values.
	TimestampBaseSeconds int64 = 1420070400
)

// TimestampTreeReader is a TreeReader implementation that reads timestamp type columns.
type TimestampTreeReader struct {
	BaseTreeReader
	data      IntegerReader
	secondary IntegerReader
}

// Next implements the TreeReader interface.
func (t *TimestampTreeReader) Next() bool {
	return t.data.Next() && t.secondary.Next()
}

// ValueTimestamp returns the next timestamp value.
func (t *TimestampTreeReader) Timestamp() time.Time {
	return time.Unix(TimestampBaseSeconds+t.data.Int(), t.secondary.Int())
}

// Value implements the TreeReader interface.
func (t *TimestampTreeReader) Value() interface{} {
	return t.Timestamp()
}

// Err implements the TreeReader interface.
func (t *TimestampTreeReader) Err() error {
	if err := t.data.Err(); err != nil {
		return err
	}
	return t.secondary.Err()
}

// NewTimestampTreeReader returns a new TimestampTreeReader along with any error that occurs.
func NewTimestampTreeReader(present, data, secondary io.Reader, encoding *proto.ColumnEncoding) (*TimestampTreeReader, error) {
	dataReader, err := createIntegerReader(encoding.GetKind(), data, true, false)
	if err != nil {
		return nil, err
	}
	secondaryReader, err := createIntegerReader(encoding.GetKind(), secondary, true, false)
	if err != nil {
		return nil, err
	}
	return &TimestampTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		data:           dataReader,
		secondary:      secondaryReader,
	}, nil
}

// DateTreeReader is a TreeReader implementation that can read date column types.
type DateTreeReader struct {
	*IntegerTreeReader
}

// Date returns the next date value as a time.Time.
func (d *DateTreeReader) Date() time.Time {
	return time.Unix(86400*d.Int(), 0)
}

// Value implements the TreeReader interface.
func (d *DateTreeReader) Value() interface{} {
	return d.Date()
}

// NewDateTreeReader returns a new DateTreeReader along with any error that occurs.
func NewDateTreeReader(present, data io.Reader, encoding *proto.ColumnEncoding) (*DateTreeReader, error) {
	reader, err := NewIntegerTreeReader(present, data, encoding)
	if err != nil {
		return nil, err
	}
	return &DateTreeReader{reader}, nil
}

// IntegerReader is an interface that provides methods for reading a string stream.
type StringTreeReader interface {
	TreeReader
	String() string
}

// NewStringTreeReader returns a StringTreeReader implementation along with any error that occurs.s
func NewStringTreeReader(present, data, length, dictionary io.Reader, encoding *proto.ColumnEncoding) (StringTreeReader, error) {
	switch kind := encoding.GetKind(); kind {
	case proto.ColumnEncoding_DIRECT, proto.ColumnEncoding_DIRECT_V2:
		return NewStringDirectTreeReader(present, data, length, kind)
	case proto.ColumnEncoding_DICTIONARY, proto.ColumnEncoding_DICTIONARY_V2:
		return NewStringDictionaryTreeReader(present, data, length, dictionary, encoding)
	}
	return nil, fmt.Errorf("unsupported column encoding: %s", encoding.GetKind())
}

// StringDirectTreeReader is a StringTreeReader implementation that can read direct
// encoded string type columns.
type StringDirectTreeReader struct {
	BaseTreeReader
	length IntegerReader
	data   io.Reader
	err    error
}

func NewStringDirectTreeReader(present, data, length io.Reader, kind proto.ColumnEncoding_Kind) (*StringDirectTreeReader, error) {
	ireader, err := createIntegerReader(kind, length, false, false)
	if err != nil {
		return nil, err
	}
	return &StringDirectTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		length:         ireader,
		data:           data,
	}, nil
}

func (s *StringDirectTreeReader) Next() bool {
	return s.BaseTreeReader.Next() && s.length.Next() && s.err == nil
}

func (s *StringDirectTreeReader) String() string {
	l := int(s.length.Int())
	byt := make([]byte, l, l)
	n, err := s.data.Read(byt)
	if err != nil {
		s.err = err
		return ""
	}
	if n != l {
		s.err = fmt.Errorf("read unexpected number of bytes: %v expected: %v", n, l)
		return ""
	}
	return string(byt)
}

func (s *StringDirectTreeReader) Value() interface{} {
	return s.String()
}

func (s *StringDirectTreeReader) Err() error {
	if s.err != nil {
		return s.err
	}
	if err := s.length.Err(); err != nil {
		return err
	}
	return s.BaseTreeReader.Err()
}

type StringDictionaryTreeReader struct {
	BaseTreeReader
	dictionaryOffsets []int
	dictionaryLength  []int
	reader            IntegerReader
	dictionaryBytes   []byte
	err               error
}

func NewStringDictionaryTreeReader(present, data, length, dictionary io.Reader, encoding *proto.ColumnEncoding) (*StringDictionaryTreeReader, error) {
	ireader, err := createIntegerReader(encoding.GetKind(), data, false, false)
	if err != nil {
		return nil, err
	}
	r := &StringDictionaryTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		reader:         ireader,
	}
	if dictionary != nil && encoding != nil {
		err := r.readDictionaryStream(dictionary)
		if err != nil {
			return nil, err
		}
		if length != nil {
			err = r.readDictionaryLength(length, encoding)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

func (s *StringDictionaryTreeReader) readDictionaryStream(dictionary io.Reader) error {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, dictionary)
	if err != nil {
		return err
	}
	s.dictionaryBytes = buf.Bytes()
	return nil
}

func (s *StringDictionaryTreeReader) readDictionaryLength(length io.Reader, encoding *proto.ColumnEncoding) error {
	lreader, err := createIntegerReader(encoding.GetKind(), length, false, false)
	if err != nil {
		return err
	}
	var offset int
	for lreader.Next() {
		l := int(lreader.Int())
		s.dictionaryLength = append(s.dictionaryLength, l)
		s.dictionaryOffsets = append(s.dictionaryOffsets, offset)
		offset += l
	}
	if err := lreader.Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (s *StringDictionaryTreeReader) IsPresent() bool {
	return s.BaseTreeReader.IsPresent()
}

func (s *StringDictionaryTreeReader) Next() bool {
	return s.BaseTreeReader.Next() && s.reader.Next()
}

func (s *StringDictionaryTreeReader) getIndexLength(i int) (int, int) {
	if i >= len(s.dictionaryLength) || i < 0 {
		s.err = fmt.Errorf("invalid integer value: %v expecting values between 0...%v", i, len(s.dictionaryLength))
		return 0, 0
	}
	if i >= len(s.dictionaryOffsets) || i < 0 {
		s.err = fmt.Errorf("invalid integer value: %v expecting values between 0...%v", i, len(s.dictionaryOffsets))
		return 0, 0
	}
	return s.dictionaryOffsets[i], s.dictionaryLength[i]
}

func (s *StringDictionaryTreeReader) String() string {
	i := int(s.reader.Int())
	offset, length := s.getIndexLength(i)
	return string(s.dictionaryBytes[offset : offset+length])
}

func (s *StringDictionaryTreeReader) Value() interface{} {
	return s.String()
}

func (s *StringDictionaryTreeReader) Err() error {
	if s.err != nil {
		return s.err
	}
	if err := s.reader.Err(); err != nil {
		return err
	}
	return s.BaseTreeReader.Err()
}

type BooleanTreeReader struct {
	BaseTreeReader
	*BooleanReader
}

func (b *BooleanTreeReader) Next() bool {
	return b.BaseTreeReader.Next() && b.BooleanReader.Next()
}

func (b *BooleanTreeReader) Bool() bool {
	return b.BooleanReader.Bool()
}

func (b *BooleanTreeReader) Value() interface{} {
	return b.Bool()
}

func (b *BooleanTreeReader) Err() error {
	if err := b.BooleanReader.Err(); err != nil {
		return err
	}
	return b.BaseTreeReader.Err()
}

func NewBooleanTreeReader(present, data io.Reader, encoding *proto.ColumnEncoding) (*BooleanTreeReader, error) {
	return &BooleanTreeReader{
		NewBaseTreeReader(present),
		NewBooleanReader(bufio.NewReader(data)),
	}, nil
}

type ByteTreeReader struct {
	BaseTreeReader
	*RunLengthByteReader
}

func (b *ByteTreeReader) IsPresent() bool {
	return b.BaseTreeReader.IsPresent()
}

func (b *ByteTreeReader) Next() bool {
	return b.BaseTreeReader.Next() && b.RunLengthByteReader.Next()
}

func (b *ByteTreeReader) Byte() byte {
	return b.RunLengthByteReader.Byte()
}

func (b *ByteTreeReader) Value() interface{} {
	return b.Byte()
}

func (b *ByteTreeReader) Err() error {
	if err := b.RunLengthByteReader.Err(); err != nil {
		return err
	}
	return b.BaseTreeReader.Err()
}

func NewByteTreeReader(present, data io.Reader, encoding *proto.ColumnEncoding) (*ByteTreeReader, error) {
	return &ByteTreeReader{
		NewBaseTreeReader(present),
		NewRunLengthByteReader(bufio.NewReader(data)),
	}, nil
}

type MapTreeReader struct {
	BaseTreeReader
	length IntegerReader
	key    TreeReader
	value  TreeReader
}

func (m *MapTreeReader) Next() bool {
	return m.length.Next() && m.key.Next() && m.value.Next()
}

func (m *MapTreeReader) Map() map[interface{}]interface{} {
	l := int(m.length.Int())
	kv := make(map[interface{}]interface{})
	for i := 0; i < l; i++ {
		k := m.key.Value()
		v := m.value.Value()
		kv[k] = v
	}
	return kv
}

func (m *MapTreeReader) Value() interface{} {
	return m.Map()
}

func NewMapTreeReader(present, length io.Reader, key, value TreeReader, encoding *proto.ColumnEncoding) (*MapTreeReader, error) {
	lengthReader, err := createIntegerReader(encoding.GetKind(), length, false, false)
	if err != nil {
		return nil, err
	}
	return &MapTreeReader{
		NewBaseTreeReader(present),
		lengthReader,
		key,
		value,
	}, nil
}

type ListTreeReader struct {
	BaseTreeReader
	length IntegerReader
	value  TreeReader
}

func (r *ListTreeReader) Next() bool {
	return r.length.Next() && r.value.Next()
}

func (r *ListTreeReader) List() []interface{} {
	l := int(r.length.Int())
	ls := make([]interface{}, l, l)
	for i := range ls {
		ls[i] = r.value.Value()
		if !r.value.Next() {
			break
		}
	}
	return ls
}

func (r *ListTreeReader) Value() interface{} {
	return r.List()
}

func (r *ListTreeReader) Err() error {
	if r.err != nil {
		return r.err
	}
	if err := r.length.Err(); err != nil {
		return err
	}
	return r.BaseTreeReader.Err()
}

func NewListTreeReader(present, length io.Reader, value TreeReader, encoding *proto.ColumnEncoding) (*ListTreeReader, error) {
	lengthReader, err := createIntegerReader(encoding.GetKind(), length, false, false)
	if err != nil {
		return nil, err
	}
	return &ListTreeReader{
		NewBaseTreeReader(present),
		lengthReader,
		value,
	}, nil
}

type StructTreeReader struct {
	BaseTreeReader
	children map[string]TreeReader
}

func (s *StructTreeReader) Next() bool {
	for _, v := range s.children {
		if !v.Next() {
			return false
		}
	}
	return true
}

func (s *StructTreeReader) Struct() map[string]interface{} {
	st := make(map[string]interface{})
	for k, v := range s.children {
		st[k] = v.Value()
	}
	return st
}

func (s *StructTreeReader) Value() interface{} {
	return s.Struct()
}

func (s *StructTreeReader) Err() error {
	if s.err != nil {
		return s.err
	}
	for _, child := range s.children {
		if err := child.Err(); err != nil {
			return err
		}
	}
	return s.BaseTreeReader.Err()
}

func NewStructTreeReader(present io.Reader, children map[string]TreeReader) (*StructTreeReader, error) {
	return &StructTreeReader{
		NewBaseTreeReader(present),
		children,
	}, nil
}

type FloatTreeReader struct {
	BaseTreeReader
	io.Reader
	bytesPerValue int
	err           error
}

func (r *FloatTreeReader) IsPresent() bool {
	return r.BaseTreeReader.IsPresent()
}

func (r *FloatTreeReader) Next() bool {
	return r.BaseTreeReader.Next()
}

func (r *FloatTreeReader) Float() float32 {
	var val uint32
	bs := make([]byte, r.bytesPerValue, r.bytesPerValue)
	n, err := r.Reader.Read(bs)
	if err != nil {
		r.err = err
		return 0
	}
	if n != r.bytesPerValue {
		r.err = fmt.Errorf("read unexpected number of bytes: %v, expected:%v", n, r.bytesPerValue)
		return 0
	}
	for i := 0; i < len(bs); i++ {
		val |= uint32(bs[i]) << uint(i*8)
	}
	return math.Float32frombits(val)
}

func (r *FloatTreeReader) Value() interface{} {
	if r.bytesPerValue == 4 {
		return r.Float()
	}
	return r.Float()
}

func (r *FloatTreeReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.BaseTreeReader.Err()
}

func NewFloatTreeReader(bytesPerValue int, present, data io.Reader, encoding *proto.ColumnEncoding) (*FloatTreeReader, error) {
	return &FloatTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		Reader:         data,
		bytesPerValue:  bytesPerValue,
	}, nil
}

type BinaryTreeReader struct {
	BaseTreeReader
	length IntegerReader
	data   io.Reader
	err    error
}

func (r *BinaryTreeReader) IsPresent() bool {
	return r.BaseTreeReader.IsPresent()
}

func (r *BinaryTreeReader) Next() bool {
	return r.BaseTreeReader.Next() && r.length.Next()
}

func (r *BinaryTreeReader) Binary() []byte {
	l := int(r.length.Int())
	b := make([]byte, l, l)
	n, err := r.data.Read(b)
	if err != nil {
		r.err = err
	} else if n != l {
		r.err = fmt.Errorf("read unexpected number of bytes: %v, expected:%v", n, l)
	}
	return b
}

func (r *BinaryTreeReader) Err() error {
	if r.err != nil {
		return r.err
	}
	if err := r.length.Err(); err != nil {
		return err
	}
	return r.BaseTreeReader.Err()
}

func NewBinaryTreeReader(present, data, length io.Reader, encoding *proto.ColumnEncoding) (*BinaryTreeReader, error) {
	lengthReader, err := createIntegerReader(encoding.GetKind(), length, false, false)
	if err != nil {
		return nil, err
	}
	return &BinaryTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		length:         lengthReader,
		data:           data,
	}, nil
}

type UnionTreeReader struct {
	BaseTreeReader
	data     *RunLengthByteReader
	children []TreeReader
	err      error
}

func NewUnionTreeReader(present, data io.Reader, children []TreeReader) (*UnionTreeReader, error) {
	return &UnionTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		data:           NewRunLengthByteReader(bufio.NewReader(data)),
		children:       children,
	}, nil
}

func (u *UnionTreeReader) Next() bool {
	return u.BaseTreeReader.Next() && u.data.Next()
}

func (u *UnionTreeReader) Value() interface{} {
	i := int(u.data.Byte())
	if i >= len(u.children) {
		u.err = fmt.Errorf("unexpected tag offset: %v expected < %v", i, len(u.children))
	}
	if u.children[i].Next() {
		return u.children[i].Value()
	}
	return fmt.Errorf("no value available in union child column: %v", i)
}

func (u *UnionTreeReader) Err() error {
	if u.err != nil {
		return u.err
	}
	for _, child := range u.children {
		if err := child.Err(); err != nil {
			return err
		}
	}
	return u.BaseTreeReader.Err()
}

type DecimalTreeReader struct {
	BaseTreeReader
	data      io.Reader
	secondary IntegerReader
	err       error
}

func NewDecimalTreeReader(present, data, secondary io.Reader, encoding *proto.ColumnEncoding) (*DecimalTreeReader, error) {
	ireader, err := createIntegerReader(encoding.GetKind(), secondary, false, false)
	if err != nil {
		return nil, err
	}
	return &DecimalTreeReader{
		BaseTreeReader: NewBaseTreeReader(present),
		data:           data,
		secondary:      ireader,
	}, nil
}

func (d *DecimalTreeReader) Next() bool {
	return d.BaseTreeReader.Next() && d.secondary.Next()
}

func (d *DecimalTreeReader) Err() error {
	if d.err != nil {
		return d.err
	}
	if err := d.secondary.Err(); err != nil {
		return err
	}
	return d.BaseTreeReader.Err()
}
