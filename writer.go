package orc

// import (
// 	"io"

// 	"code.simon-critchley.co.uk/orc/proto"
// )

// const (
// 	HDFSBufferSize       = 256 * 1024
// 	MinRowIndexStride    = 1000
// 	ColumnCountThreshold = 1000
// )

// type CompressionCodec interface {
// }

// type Writer struct {
// 	hdfsBufferSize       int
// 	minRowIndexStride    int
// 	columnCountThreshold int
// 	defaultStripeSize    int64
// 	adjustedStripeSize   int64
// 	rowIndexStride       int
// 	compress             proto.CompressionKind
// 	codec                CompressionCodec
// 	addBlockPadding      bool
// 	bufferSize           int
// 	blockSize            int64
// 	paddingTolerance     float64
// 	schema               TypeDescription
// 	bloomFilterColumns   []bool
// 	// the streams that make up the current stripe
// 	streams   map[StreamName]BufferedStream
// 	rawWriter io.WriteCloser
// }

// type WriterConfigFunc func(w *Writer) error

// func NewWriter(w io.Writer, fns ...WriterConfigFunc) (*Writer, error) {
// 	writer := &Writer{}
// 	for _, fn := range fns {
// 		err := fn(writer)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return writer, nil
// }

// func (w *Writer) createStream(column int, kind proto.Stream_Kind) io.ByteWriter {
// 	name := NewStreamName(column, kind)
// 	switch kind {
// 	case proto.Stream_BLOOM_FILTER,
// 		proto.Stream_DATA,
// 		proto.Stream_DICTIONARY_DATA:

// 	case proto.Stream_LENGTH,
// 		proto.Stream_DICTIONARY_COUNT,
// 		proto.Stream_PRESENT,
// 		proto.Stream_ROW_INDEX,
// 		proto.Stream_SECONDARY:

// 	}
// 	result, ok := w.streams[name]
// 	if !ok || result == nil {
// 		result = NewBuffererStream(name.String(), w.bufferSize, w.codec)
// 		w.streams[name] = result
// 	}
// 	return result
// }

// func (w *Writer) isCompressed() bool {
// 	return w.codec != nil
// }

// func (w *Writer) getBloomFilterColumns() []bool {
// 	return w.bloomFilterColumns
// }
