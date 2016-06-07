package orc

import (
	"bytes"
	"fmt"

	"code.simon-critchley.co.uk/orc/proto"
)

type streamMap map[streamName]bytes.Buffer

func (s streamMap) reset() {
	for k := range s {
		delete(s, k)
	}
}

func (s streamMap) set(name streamName, buf bytes.Buffer) {
	s[name] = buf
}

func (s streamMap) get(name streamName) (bytes.Buffer, error) {
	if b, ok := s[name]; ok {
		return b, nil
	}
	return bytes.Buffer{}, fmt.Errorf("%s stream not found", name)
}

type streamName struct {
	columnID int
	kind     proto.Stream_Kind
}

func (s streamName) String() string {
	return fmt.Sprintf("col:%v kind:%s", s.columnID, s.kind)
}
