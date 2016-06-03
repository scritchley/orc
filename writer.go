package orc

import (
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

const (
	HDFSBufferSize       = 256 * 1024
	MinRowIndexStride    = 1000
	ColumnCountThreshold = 1000
)

type CompressionCodec interface {
}

type TypeDescription interface {
}

type StreamName interface {
}

type BufferedStream interface {
}

type Writer struct {
	hdfsBufferSize       int
	minRowIndexStride    int
	columnCountThreshold int
	defaultStripeSize    int64
	adjustedStripeSize   int64
	rowIndexStride       int
	compress             proto.CompressionKind
	codec                CompressionCodec
	addBlockPadding      bool
	bufferSize           int
	blockSize            int64
	paddingTolerance     float64
	schema               TypeDescription
	// the streams that make up the current stripe
	streams   map[StreamName]BufferedStream
	rawWriter io.WriteCloser
}
