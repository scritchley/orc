package orc

import (
	"fmt"
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

type StreamReader interface {
	Next() bool
	Value() interface{}
	Error() error
}

func getReader(r io.ByteReader, t orc_proto.Type, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	switch t.GetKind() {
	case orc_proto.Type_BOOLEAN:
		return getBooleanReader(r, c, s)
	case orc_proto.Type_BYTE:
		return getByteReader(r, c, s)
	case orc_proto.Type_SHORT:
		return getShortReader(r, c, s)
	case orc_proto.Type_INT, orc_proto.Type_LONG:
		return getIntReader(r, c, s)
	case orc_proto.Type_FLOAT:
		return getFloatReader(r, c, s)
	case orc_proto.Type_DOUBLE:
		return getDoubleReader(r, c, s)
	case orc_proto.Type_STRING:
		return getStringReader(r, c, s)
	case orc_proto.Type_BINARY:
		return getBinaryReader(r, c, s)
	case orc_proto.Type_TIMESTAMP:
		return getTimestampReader(r, c, s)
	case orc_proto.Type_LIST:
		return getListReader(r, c, s)
	case orc_proto.Type_MAP:
		return getMapReader(r, c, s)
	case orc_proto.Type_STRUCT:
		return getStructReader(r, c, s)
	case orc_proto.Type_UNION:
		return getUnionReader(r, c, s)
	case orc_proto.Type_DECIMAL:
		return getDecimalReader(r, c, s)
	case orc_proto.Type_DATE:
		return getDateReader(r, c, s)
	case orc_proto.Type_VARCHAR:
		return getVarcharReader(r, c, s)
	case orc_proto.Type_CHAR:
		return getCharReader(r, c, s)
	default:
		return nil, fmt.Errorf("unsupported column encoding %s", t.GetKind().String())
	}
}

func getBooleanReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	switch c.GetKind() {
	case orc_proto.ColumnEncoding_DIRECT:
		return getBooleanDirectReader(r, s)
	default:
		return nil, fmt.Errorf("unsupported boolean encoding %s", c.GetKind().String())
	}
}

func getBooleanDirectReader(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	switch s.GetKind() {
	case orc_proto.Stream_PRESENT, orc_proto.Stream_DATA:
		return NewBooleanStreamReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported boolean stream encoding %s", s.GetKind().String())
	}
}

func getByteReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	return NewByteStreamReader(r), nil
}

func getShortReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	switch c.GetKind() {
	case orc_proto.ColumnEncoding_DIRECT:
		return getShortDirectReader(r, s)
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) encoding %s", c.GetKind().String())
	}
}

func getShortDirectReader(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	switch s.GetKind() {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewByteStreamReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) stream encoding %s", s.GetKind().String())
	}
}

func getIntReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	switch c.GetKind() {
	case orc_proto.ColumnEncoding_DIRECT:
		return getIntDirectReader(r, s)
	case orc_proto.ColumnEncoding_DIRECT_V2:
		return getIntDirectV2Reader(r, s)
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) encoding %s", c.GetKind().String())
	}
}

func getIntDirectReader(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	return nil, nil
}

func getIntDirectV2Reader(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	switch s.GetKind() {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewIntStreamReader(r, true), nil
	default:
		return nil, fmt.Errorf("unsupported int stream encoding %s", s.GetKind().String())
	}
}

func getFloatReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	return nil, fmt.Errorf("unsupported type float")
}

func getDoubleReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	return nil, fmt.Errorf("unsupported type double")
}

func getStringReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {
	switch c.GetKind() {
	case orc_proto.ColumnEncoding_DIRECT:
		return getStringDirect(r, s)
	case orc_proto.ColumnEncoding_DICTIONARY:
		return getStringDictionary(r, s)
	case orc_proto.ColumnEncoding_DICTIONARY_V2:
		return getStringDictionaryV2(r, s)
	default:
		return nil, fmt.Errorf("unsupported string column encoding %s", s.GetKind().String())
	}
}

func getStringDirect(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	switch s.GetKind() {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewIntStreamReader(r, false), nil
	case orc_proto.Stream_LENGTH:
		return NewIntStreamReader(r, false), nil
	default:
		return nil, fmt.Errorf("unsupported string stream encoding %s", s.GetKind().String())
	}
}

func getStringDictionary(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	return nil, nil
}

func getStringDictionaryV2(r io.ByteReader, s orc_proto.Stream) (StreamReader, error) {
	return nil, nil
}

func getBinaryReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getTimestampReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getListReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getMapReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getStructReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getUnionReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getDecimalReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getDateReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getVarcharReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}

func getCharReader(r io.ByteReader, c orc_proto.ColumnEncoding, s orc_proto.Stream) (StreamReader, error) {

	return nil, nil

}
