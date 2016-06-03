package orc

import (
	"bufio"
	"compress/flate"
	"io"
)

type Codec interface {
	io.ByteWriter
	io.Closer
}

type ZlibEncoder struct {
	bw *bufio.Writer
	zw *flate.Writer
}

func (z *ZlibEncoder) WriteByte(c byte) error {
	return z.bw.WriteByte(c)
}

func (z *ZlibEncoder) Close() error {
	err := z.bw.Flush()
	if err != nil {
		return err
	}
	return z.zw.Close()
}

func NewZlibEncoder(w io.Writer) (*ZlibEncoder, error) {
	zw, err := flate.NewWriter(w, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(w)

	return &ZlibEncoder{
		bw: bw,
		zw: zw,
	}, nil
}

func ZlibDecoder(r io.Reader) *bufio.Reader {
	return bufio.NewReader(flate.NewReader(r))
}
