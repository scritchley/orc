package orc

import (
	"bufio"
	"fmt"
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

var (
	unsupportedFormat = fmt.Errorf("unsupported format")
)

type TreeReader interface {
	HasNext() bool
	Next() interface{}
	Err() error
}

type IntegerReader interface {
	TreeReader
	NextInt() int64
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

type StringTreeReader interface {
	TreeReader
	NextString() string
}

func NewStringTreeReader(present, data, length, dictionary io.Reader, encoding *proto.ColumnEncoding) (StringTreeReader, error) {
	switch kind := encoding.GetKind(); kind {
	case proto.ColumnEncoding_DIRECT, proto.ColumnEncoding_DIRECT_V2:
		return NewStringDirectTreeReader(present, data, length, kind)
	case proto.ColumnEncoding_DICTIONARY, proto.ColumnEncoding_DICTIONARY_V2:
		return NewStringDictionaryTreeReader(present, data, length, dictionary, encoding)
	}
	return nil, fmt.Errorf("unsupported column encoding: %s", encoding.GetKind())
}

type StringDirectTreeReader struct {
	lengths IntegerReader
	data    io.Reader
	err     error
}

func NewStringDirectTreeReader(present, data, length io.Reader, kind proto.ColumnEncoding_Kind) (*StringDirectTreeReader, error) {
	ireader, err := createIntegerReader(kind, length, false, false)
	if err != nil {
		return nil, err
	}
	return &StringDirectTreeReader{
		lengths: ireader,
		data:    data,
	}, nil
}

func (s *StringDirectTreeReader) HasNext() bool {
	return s.lengths.HasNext() && s.err == nil
}

func (s *StringDirectTreeReader) NextString() string {
	l := int(s.lengths.NextInt())
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

func (s *StringDirectTreeReader) Next() interface{} {
	return s.NextString()
}

func (s *StringDirectTreeReader) Err() error {
	err := s.lengths.Err()
	if err != nil {
		return err
	}
	return s.err
}

type StringDictionaryTreeReader struct {
	dictionaryBuffer             *DynamicByteSlice
	dictionaryOffsets            []int
	reader                       IntegerReader
	dictionaryBufferInBytesCache []byte
}

func NewStringDictionaryTreeReader(present, data, length, dictionary io.Reader, encoding *proto.ColumnEncoding) (*StringDictionaryTreeReader, error) {
	ireader, err := createIntegerReader(encoding.GetKind(), data, false, false)
	if err != nil {
		return nil, err
	}
	r := &StringDictionaryTreeReader{
		reader: ireader,
	}
	if dictionary != nil && encoding != nil {
		err := r.readDictionaryStream(dictionary)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (s *StringDictionaryTreeReader) readDictionaryStream(in io.Reader) error {
	s.dictionaryBuffer = NewDynamicByteSlice(DefaultNumChunks, DefaultChunkSize)
	return s.dictionaryBuffer.readAll(bufio.NewReader(in))
}

func (s *StringDictionaryTreeReader) HasNext() bool {
	return false
}

func (s *StringDictionaryTreeReader) NextString() string {
	return ""
}

func (s *StringDictionaryTreeReader) Next() interface{} {
	return nil
}

func (s *StringDictionaryTreeReader) Err() error {
	return nil
}
