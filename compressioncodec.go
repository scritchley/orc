package orc

import (
	"compress/flate"
	"io"

	"github.com/golang/snappy"
)

type CompressionCodec interface {
	Encoder(w io.Writer) io.Writer
	Decoder(r io.Reader) io.Reader
}

type CompressionNone struct{}

func (c CompressionNone) Encoder(w io.Writer) io.Writer {
	return w
}

func (c CompressionNone) Decoder(r io.Reader) io.Reader {
	return r
}

type CompressionZlib struct {
	level    int
	strategy int
}

func (c CompressionZlib) Encoder(w io.Writer) io.Writer {
	return w
}

func (c CompressionZlib) Decoder(r io.Reader) io.Reader {
	return &CompressionZlibDecoder{source: r}
}

type CompressionZlibDecoder struct {
	source      io.Reader
	decoded     io.Reader
	isOriginal  bool
	chunkLength int
	remaining   int64
}

func (c *CompressionZlibDecoder) readHeader() (int, error) {
	header := make([]byte, 3, 3)
	_, err := c.source.Read(header)
	if err != nil {
		return 0, err
	}
	c.isOriginal = bool((header[0] & 0x01) == 1)
	c.chunkLength = (int(header[2]) << 15) | (int(header[1]) << 7) | int((uint(header[0]) >> 1))
	if !c.isOriginal {
		c.decoded = flate.NewReader(io.LimitReader(c.source, int64(c.chunkLength)))
	} else {
		c.decoded = io.LimitReader(c.source, int64(c.chunkLength))
	}
	return 0, nil
}

func (c *CompressionZlibDecoder) Read(p []byte) (int, error) {
	if c.decoded == nil {
		return c.readHeader()
	}
	n, err := c.decoded.Read(p)
	if err == io.EOF {
		c.decoded = nil
		return n, nil
	}
	return n, err
}

type CompressionSnappy struct{}

func (c CompressionSnappy) Encoder(w io.Writer) io.Writer {
	return w
}

func (c CompressionSnappy) Decoder(r io.Reader) io.Reader {
	return &CompressionSnappyDecoder{source: r}
}

type CompressionSnappyDecoder struct {
	source      io.Reader
	decoded     io.Reader
	isOriginal  bool
	chunkLength int
	remaining   int64
}

func (c *CompressionSnappyDecoder) readHeader() (int, error) {
	header := make([]byte, 3, 3)
	_, err := c.source.Read(header)
	if err != nil {
		return 0, err
	}
	c.isOriginal = bool((header[0] & 0x01) == 1)
	c.chunkLength = (int(header[2]) << 15) | (int(header[1]) << 7) | int((uint(header[0]) >> 1))
	if !c.isOriginal {
		c.decoded = snappy.NewReader(io.LimitReader(c.source, int64(c.chunkLength)))
	} else {
		c.decoded = io.LimitReader(c.source, int64(c.chunkLength))
	}
	return 0, nil
}

func (c *CompressionSnappyDecoder) Read(p []byte) (int, error) {
	if c.decoded == nil {
		return c.readHeader()
	}
	n, err := c.decoded.Read(p)
	if err == io.EOF {
		c.decoded = nil
		return n, nil
	}
	return n, err
}
