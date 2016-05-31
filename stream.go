package orc

import (
	"fmt"
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

var (
	unsupportedFormat = fmt.Errorf("unsupported format")
)

type StreamReader interface {
	HasNext() bool
	Next() interface{}
	Error() error
}

func getReader(r io.ByteReader, t orc_proto.Type_Kind, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	switch s {
	case orc_proto.Stream_BLOOM_FILTER, orc_proto.Stream_ROW_INDEX, orc_proto.Stream_DICTIONARY_DATA:
		return nil, unsupportedFormat
	}

	switch t {
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
	case orc_proto.Type_STRING, orc_proto.Type_VARCHAR:
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
	case orc_proto.Type_CHAR:
		return getCharReader(r, c, s)
	default:
		return nil, fmt.Errorf("unsupported column encoding %s", t.String())
	}
}

func getBooleanReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch c {
	case orc_proto.ColumnEncoding_DIRECT:
		return getBooleanDirectReader(r, s)
	default:
		return nil, fmt.Errorf("unsupported boolean encoding %s", c.String())
	}
}

func getBooleanDirectReader(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch s {
	case orc_proto.Stream_PRESENT, orc_proto.Stream_DATA:
		return NewBooleanStreamReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported boolean stream encoding %s", s.String())
	}
}

func getByteReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	return NewByteStreamReader(r), nil
}

func getShortReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch c {
	case orc_proto.ColumnEncoding_DIRECT:
		return getShortDirectReader(r, s)
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) encoding %s", c.String())
	}
}

func getShortDirectReader(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch s {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewByteStreamReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) stream encoding %s", s.String())
	}
}

func getIntReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch c {
	case orc_proto.ColumnEncoding_DIRECT:
		return getIntDirectReader(r, s)
	case orc_proto.ColumnEncoding_DIRECT_V2:
		return getIntDirectV2Reader(r, s)
	default:
		return nil, fmt.Errorf("unsupported short (tinyint) encoding %s", c.String())
	}
}

func getIntDirectReader(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	return nil, unsupportedFormat
}

func getIntDirectV2Reader(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch s {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewIntStreamReaderV2(r, true), nil
	default:
		return nil, fmt.Errorf("unsupported int stream encoding %s", s.String())
	}
}

func getFloatReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	return nil, fmt.Errorf("unsupported type float")
}

func getDoubleReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	return nil, fmt.Errorf("unsupported type double")
}

func getStringReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch c {
	case orc_proto.ColumnEncoding_DIRECT:
		return getStringDirect(r, s)
	case orc_proto.ColumnEncoding_DICTIONARY:
		return getStringDictionary(r, s)
	case orc_proto.ColumnEncoding_DICTIONARY_V2:
		return getStringDictionaryV2(r, s)
	default:
		return nil, fmt.Errorf("unsupported string column encoding %s", s.String())
	}
}

func getStringDirect(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch s {
	case orc_proto.Stream_PRESENT:
		return NewBooleanStreamReader(r), nil
	case orc_proto.Stream_DATA:
		return NewIntStreamReaderV2(r, false), nil
	case orc_proto.Stream_LENGTH:
		return NewIntStreamReaderV2(r, false), nil
	default:
		return nil, fmt.Errorf("unsupported string stream encoding %s", s.String())
	}
}

func getStringDictionary(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	return nil, unsupportedFormat
}

func getStringDictionaryV2(r io.ByteReader, s orc_proto.Stream_Kind) (StreamReader, error) {
	switch s {
	case orc_proto.Stream_DATA:
		return NewIntStreamReaderV2(r, false), nil
	default:
		return nil, unsupportedFormat
	}
}

func getBinaryReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getTimestampReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getListReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getMapReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getStructReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getUnionReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getDecimalReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getDateReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getVarcharReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}

func getCharReader(r io.ByteReader, c orc_proto.ColumnEncoding_Kind, s orc_proto.Stream_Kind) (StreamReader, error) {

	return nil, unsupportedFormat

}
