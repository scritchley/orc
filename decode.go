package orc

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
)

const (
	tailSize int64 = 16 * 1024
)

// ORCReader is an interface that implements the minimum required methods for a Decoder.
type ORCReader interface {
	io.ReaderAt
	// Size returns the size in bytes of the file being read
	Size() int64
}

// Decoder decodes an ORC file
type Decoder struct {
	dec              func(io.Reader) (io.Reader, error)
	bufferSize       uint64
	postScriptLength int64
	r                ORCReader
	PostScript       PostScript
	Footer           Footer
	Metadata         Metadata
	tail             []byte
}

func NewDecoder(r ORCReader) *Decoder {
	return &Decoder{
		r: r,
	}
}

func (d *Decoder) getTail() error {

	var off int64

	sz := d.r.Size()

	d.tail = make([]byte, tailSize)

	if sz > (tailSize) {
		off = sz - tailSize
	}

	_, err := d.r.ReadAt(d.tail, off)
	if err != nil {
		return err
	}

	return nil
}

// readPostScript reads the PostScript section of an ORC file an unmarshals it into the PostScript property of the decoder
func (d *Decoder) readPostScript() error {

	finalByte := d.tail[len(d.tail)-1]

	d.postScriptLength = int64(finalByte)

	postScriptOffset := len(d.tail) - 1 - int(d.postScriptLength)

	postScriptBuf := d.tail[postScriptOffset:(postScriptOffset + int(d.postScriptLength))]

	err := proto.Unmarshal(postScriptBuf, &d.PostScript)
	if err != nil {
		return err
	}

	d.bufferSize = d.PostScript.GetCompressionBlockSize()

	return d.useCompression()

}

func (d *Decoder) useCompression() error {

	compressionKind := d.PostScript.GetCompression()

	switch compressionKind {
	case CompressionKind_ZLIB:
		// Use the zlibDecoder
		d.dec = zlibDecoder
	case CompressionKind_SNAPPY:
		// Use the snappyDecoder
		return fmt.Errorf("Unsupported compression type: %s", compressionKind.String())
		// d.dec = snappyDecoder
	case CompressionKind_LZO:
		// Use the snappyDecoder
		return fmt.Errorf("Unsupported compression type: %s", compressionKind.String())
		// d.dec = lzoDecoder
	case CompressionKind_NONE:
		// No compression by default
		d.dec = func(r io.Reader) (io.Reader, error) {
			return r, nil
		}
	default:
		return fmt.Errorf("Unsupported compression type: %s", compressionKind.String())
	}

	return nil

}

func (d *Decoder) readTail() error {

	var err error

	sz := d.r.Size()

	metadataLength := int(d.PostScript.GetMetadataLength())

	footerLength := int(d.PostScript.GetFooterLength())

	completeFooterSize := footerLength + metadataLength + int(d.postScriptLength) + 1

	completeFooterBuf := make([]byte, completeFooterSize)

	if completeFooterSize > len(d.tail) {

		n, err := d.r.ReadAt(completeFooterBuf, int64(sz-int64(completeFooterSize)))
		if err != nil {
			return err
		}
		if n != completeFooterSize {
			return fmt.Errorf("Failed to read complete footer, read %v bytes, expected %v bytes", n, completeFooterSize)
		}

	} else {

		completeFooterOffset := len(d.tail) - completeFooterSize
		completeFooterBuf = d.tail[completeFooterOffset:(completeFooterOffset + completeFooterSize)]

	}

	metadataReader, err := d.dec(bytes.NewReader(completeFooterBuf[:metadataLength]))
	if err != nil {
		return err
	}

	metadataBuf, err := ioutil.ReadAll(metadataReader)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(metadataBuf, &d.Metadata)
	if err != nil {
		return err
	}

	footerReader, err := d.dec(bytes.NewReader(completeFooterBuf[metadataLength : metadataLength+footerLength]))
	if err != nil {
		return err
	}

	footerBuf, err := ioutil.ReadAll(footerReader)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(footerBuf, &d.Footer)
	if err != nil {
		return err
	}

	return nil

}

func (d *Decoder) ReadAll(r io.Reader) ([]byte, error) {

	cr, err := d.dec(r)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(cr)

}

func (d *Decoder) Cursor() error {

	stripes := d.Footer.GetStripes()

	for _, stripe := range stripes {

		off := stripe.GetOffset()

		stripeLength := stripe.GetIndexLength() + stripe.GetDataLength() + stripe.GetFooterLength()

		compressedStripeBuf := make([]byte, stripeLength)

		d.r.ReadAt(compressedStripeBuf, int64(off))

		stripeFooterOffset := stripe.GetIndexLength() + stripe.GetDataLength()

		stripeFooterBuf, err := d.ReadAll(bytes.NewReader(compressedStripeBuf[stripeFooterOffset:]))
		if err != nil {
			return err
		}

		var stripeFooter StripeFooter

		err = proto.Unmarshal(stripeFooterBuf, &stripeFooter)
		if err != nil {
			return err
		}

		streams := stripeFooter.GetStreams()

		columns := stripeFooter.GetColumns()

		types := d.Footer.GetTypes()

		var streamOffset uint64

		for _, stream := range streams {

			streamEnd := streamOffset + stream.GetLength()

			streamBuf, err := d.ReadAll(bytes.NewReader(compressedStripeBuf[streamOffset:streamEnd]))
			if err != nil {
				return err
			}

			streamKind := stream.GetKind()

			colIndex := stream.GetColumn()

			colEncodingKind := columns[colIndex].GetKind()

			colType := types[colIndex].GetKind()

			fmt.Println(streamKind, colIndex, colEncodingKind, colType)

			if streamKind == Stream_DATA && colType == Type_INT {

				r := NewIntStreamReader(bytes.NewReader(streamBuf), true)

				for r.Next() {
					v, ok := r.Int()
					if ok {
						fmt.Println(v)
					}
				}

			}

			if streamKind == Stream_DATA && colType == Type_STRING && colEncodingKind == ColumnEncoding_DICTIONARY_V2 {

				r := NewIntStreamReader(bytes.NewReader(streamBuf), true)

				for r.Next() {
					v, ok := r.Int()
					if ok {
						fmt.Println(v)
					}
				}

			}

			streamOffset += stream.GetLength()

		}

	}

	return nil

}
