package orc

import (
	"fmt"

	"code.simon-critchley.co.uk/orc/proto"
)

func createTreeReader(schema *TypeDescription, m streamMap, r *Reader) (TreeReader, error) {
	id := schema.getID()
	encoding, err := r.getColumn(id)
	if err != nil {
		return nil, err
	}
	switch category := schema.getCategory(); category {
	case CategoryBoolean:
		return NewBooleanTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	case CategoryByte:
		return NewByteTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	case CategoryShort, CategoryInt, CategoryLong:
		return NewIntegerTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	// case CategoryFloat:
	// case CategoryDouble:
	case CategoryString, CategoryVarchar, CategoryChar:
		return NewStringTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			m.get(streamName{id, proto.Stream_LENGTH}),
			m.get(streamName{id, proto.Stream_DICTIONARY_DATA}),
			encoding,
		)
	// case CategoryDate:
	// case CategoryTimestamp:
	// case CategoryBinary:
	// case CategoryDecimal:
	// case CategoryList:
	// case CategoryMap:
	// case CategoryStruct:
	// case CategoryUnion:
	default:
		return nil, fmt.Errorf("unsupported type: %s", category)
	}
}
