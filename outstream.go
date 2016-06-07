package orc

// import (
// 	"bytes"
// 	"io"
// )

// const (
// 	HeaderSize = 3
// )

// type OutputReceiver interface {
// 	io.ByteWriter
// }

// type OutStream struct {
// 	w                 OutputReceiver
// 	bufferSize        int
// 	codec             CompressionCodec
// 	name              string
// 	suppress          bool
// 	headerSize        int
// 	current           []byte
// 	compressed        []byte
// 	overflow          []byte
// 	compressedBytes   int
// 	uncompressedBytes int
// }

// func NewOutStream(name string, bufferSize int, codec CompressionCodec, w OutputReceiver) *OutStream {
// 	return &OutStream{
// 		name:       name,
// 		bufferSize: bufferSize,
// 		codec:      codec,
// 		w:          w,
// 		suppress:   false,
// 		headerSize: HeaderSize,
// 	}
// }

// func (o *OutStream) clear() error {
// 	o.suppress = false
// 	return nil
// }

// func (o *OutStream) writeHeader(buffer []byte, position int, val int, original bool) {
// 	if original {
// 		buffer[position] = byte(val<<1 + 1)
// 	} else {
// 		buffer[position] = byte(val<<1 + 0)
// 	}
// 	buffer[position+1] = byte(val >> 7)
// 	buffer[position+2] = byte(val >> 15)
// }

// func (o *OutStream) getNewOutputBuffer() []byte {
// 	return make([]byte, o.bufferSize+o.headerSize)
// }

// func (o *OutStream) getNewInputBuffer() ([]byte, error) {
// 	if o.codec == nil {
// 		o.current = make([]byte, o.bufferSize)
// 	} else {
// 		o.current = make([]byte, o.bufferSize+o.headerSize)
// 		err := o.writeHeader(o.current, 0, o.bufferSize, true)
// 		if err != nil {
// 			return err
// 		}
// 	}
// }
