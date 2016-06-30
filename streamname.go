package orc

import (
	"fmt"
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

type streamMap map[streamName]io.Reader

func (s streamMap) reset() {
	for k := range s {
		delete(s, k)
	}
}

func (s streamMap) set(name streamName, buf io.Reader) {
	s[name] = buf
}

func (s streamMap) get(name streamName) io.Reader {
	if b, ok := s[name]; ok {
		return b
	}
	return nil
}

type streamName struct {
	columnID int
	kind     proto.Stream_Kind
}

func (s streamName) String() string {
	return fmt.Sprintf("col:%v kind:%s", s.columnID, s.kind)
}

type streamWriterMap map[streamName]*BufferedWriter

func (s streamWriterMap) reset() {
	for k := range s {
		delete(s, k)
	}
}

func (s streamWriterMap) create(codec CompressionCodec, name streamName) *BufferedWriter {
	stream := NewBufferedWriter(codec)
	s[name] = stream
	return stream
}

func (s streamWriterMap) size() int64 {
	var total int64
	for i := range s {
		total += int64(s[i].Len())
	}
	return total
}

func (s streamWriterMap) positions(columnID int) []uint64 {
	var positions []uint64
	for k := range s {
		if k.columnID == columnID {
			positions = append(positions, s[k].Position())
		}
	}
	return positions
}

type encodingMap map[int]*proto.ColumnEncoding

func (e encodingMap) add(id int, encoding *proto.ColumnEncoding) {
	e[id] = encoding
}

func (e encodingMap) reset() {
	for k := range e {
		delete(e, k)
	}
}

func (e encodingMap) encodings() []*proto.ColumnEncoding {
	encodings := make([]*proto.ColumnEncoding, len(e))
	for i := range encodings {
		encodings[i] = e[i]
	}
	return encodings
}
