package orc

import (
	"bufio"
	"bytes"
)

// BufferedWriter wraps a *bufio.Writer and records the current
// position of the writer prior to flushing to the underlying
// writer.
type BufferedWriter struct {
	*bufio.Writer
	*bytes.Buffer
	codec      CompressionCodec
	checkpoint uint64
	written    uint64
}

// NewBufferedWriter returns a new BufferedWriter using the provided
// CompressionCodec.
func NewBufferedWriter(codec CompressionCodec) *BufferedWriter {
	buf := &bytes.Buffer{}
	return &BufferedWriter{
		codec: codec,
		Writer: bufio.NewWriterSize(
			codec.Encoder(buf),
			int(DefaultCompressionChunkSize),
		),
		Buffer: buf,
	}
}

// WriteByte writes a byte to the underlying buffer an increments the total
// number of bytes written.
func (b *BufferedWriter) WriteByte(c byte) error {
	b.written++
	return b.Writer.WriteByte(c)
}

// Write writes the provided byte slice to the underlying buffer an increments
// the total number of bytes written.
func (b *BufferedWriter) Write(p []byte) (int, error) {
	b.written += uint64(len(p))
	return b.Writer.Write(p)
}

func (b *BufferedWriter) Positions() []uint64 {
	switch b.codec.(type) {
	case CompressionNone:
		checkpoint := b.checkpoint
		b.checkpoint = b.written
		return []uint64{checkpoint, 0}
	default:
		return nil
	}
}

// Close flushes any buffered bytes to the underlying writer.
func (b *BufferedWriter) Close() error {
	return b.Writer.Flush()
}

// Reset resets the underlying bytes.Buffer.
func (b *BufferedWriter) Reset() {
	b.Buffer.Reset()
}
