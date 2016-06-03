package orc

import (
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

type StreamName struct{}

func NewStreamName(id int, kind proto.Stream_Kind) *StreamName {
	return &StreamName{}
}

type StreamFactory struct{}

func (s *StreamFactory) createStream(column int, kind proto.Stream_Kind) io.ByteWriter {
	name := NewStreamName(column, kind)
	switch kind {
	case proto.Stream_BLOOM_FILTER,
		proto.Stream_DATA,
		proto.Stream_DICTIONARY_DATA:

	case proto.Stream_LENGTH,
		proto.Stream_DICTIONARY_COUNT,
		proto.Stream_PRESENT,
		proto.Stream_ROW_INDEX,
		proto.Stream_SECONDARY:

	}
	result := streams.get(name)
	if result == nil {
		result = NewBuffererStream(name.String(), bu)
	}
}
