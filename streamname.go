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
