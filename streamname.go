package orc

import (
	"bytes"
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

type streamWriterMap map[streamName]io.Writer

func (s streamWriterMap) reset() {
	for k := range s {
		delete(s, k)
	}
}

func (s streamWriterMap) create(name streamName) io.Writer {
	var stream bytes.Buffer
	s[name] = &stream
	return &stream
}
