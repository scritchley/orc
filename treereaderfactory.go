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
	case CategoryFloat:
		return NewFloatTreeReader(
			4,
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	case CategoryDouble:
		return NewFloatTreeReader(
			8,
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	case CategoryString, CategoryVarchar, CategoryChar:
		return NewStringTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			m.get(streamName{id, proto.Stream_LENGTH}),
			m.get(streamName{id, proto.Stream_DICTIONARY_DATA}),
			encoding,
		)
	case CategoryDate:
		return NewDateTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			encoding,
		)
	case CategoryTimestamp:
		return NewTimestampTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			m.get(streamName{id, proto.Stream_SECONDARY}),
			encoding,
		)
	case CategoryBinary:
		return NewBinaryTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_DATA}),
			m.get(streamName{id, proto.Stream_SECONDARY}),
			encoding,
		)
	// case CategoryDecimal:
	case CategoryList:
		if len(schema.children) != 1 {
			return nil, fmt.Errorf("expect 1 child for list type, got: %v", len(schema.children))
		}
		valueReader, err := createTreeReader(schema.children[0], m, r)
		if err != nil {
			return nil, err
		}
		return NewListTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_LENGTH}),
			valueReader,
			encoding,
		)
	case CategoryMap:
		if len(schema.children) != 2 {
			return nil, fmt.Errorf("expect 2 children for map type, got: %v", len(schema.children))
		}
		keyReader, err := createTreeReader(schema.children[0], m, r)
		if err != nil {
			return nil, err
		}
		valueReader, err := createTreeReader(schema.children[1], m, r)
		if err != nil {
			return nil, err
		}
		return NewMapTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			m.get(streamName{id, proto.Stream_LENGTH}),
			keyReader,
			valueReader,
			encoding,
		)
	case CategoryStruct:
		children := make(map[string]TreeReader)
		for i := range schema.children {
			child, err := createTreeReader(schema.children[i], m, r)
			if err != nil {
				return nil, err
			}
			children[schema.fieldNames[i]] = child
		}
		return NewStructTreeReader(
			m.get(streamName{id, proto.Stream_PRESENT}),
			children,
		)
		// case CategoryUnion:
	default:
		return nil, fmt.Errorf("unsupported type: %s", category)
	}
}
