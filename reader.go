package orc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	gproto "github.com/golang/protobuf/proto"

	"code.simon-critchley.co.uk/orc/proto"
)

var (
	errNoPostScript = errors.New("postscript is nil")
	errNoFooter     = errors.New("footer is nil")
)

const (
	DirectorySizeGuess int64 = 16 * 1024
	MaxPostScriptSize        = 256
)

type FileReader struct {
	*os.File
}

func (f FileReader) Size() int64 {
	stats, err := f.Stat()
	if err != nil {
		return 0
	}
	return stats.Size()
}

type SizedReaderAt interface {
	io.ReaderAt
	Size() int64
}

type Reader struct {
	r                   SizedReaderAt
	postScript          *proto.PostScript
	footer              *proto.Footer
	metadata            *proto.Metadata
	currentStripeOffset int
	stripesLength       int
	types               map[int]*proto.Type
	columns             map[int]*proto.ColumnEncoding
	streams             streamMap
}

func NewReader(r SizedReaderAt) (*Reader, error) {
	reader := &Reader{
		r:       r,
		types:   make(map[int]*proto.Type),
		columns: make(map[int]*proto.ColumnEncoding),
		streams: make(streamMap),
	}
	err := reader.extractMetaInfoFromFooter()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *Reader) getCodec() (CompressionCodec, error) {
	if r.postScript == nil {
		return nil, errNoPostScript
	}
	compressionKind := r.postScript.GetCompression()
	switch compressionKind {
	case proto.CompressionKind_NONE:
		return CompressionNone{}, nil
	case proto.CompressionKind_ZLIB:
		return CompressionZlib{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression kind %s", compressionKind)
	}
}

func (r *Reader) extractMetaInfoFromFooter() error {

	size := int(r.r.Size())
	psPlusByte := MaxPostScriptSize + 1

	// Read the last 256 bytes into buffer to get postscript
	postScriptBytes := make([]byte, psPlusByte, psPlusByte)
	sr := io.NewSectionReader(r.r, int64(size-psPlusByte), int64(psPlusByte)) // Use constant
	_, err := io.ReadFull(sr, postScriptBytes)
	if err != nil {
		return err
	}
	psLen := int(postScriptBytes[len(postScriptBytes)-1] & 0xff)
	psOffset := len(postScriptBytes) - 1 - psLen
	r.postScript = &proto.PostScript{}
	err = gproto.Unmarshal(postScriptBytes[psOffset:psOffset+psLen], r.postScript)
	if err != nil {
		return err
	}

	// Get the offset and length of the footer and preallocate a byte slice.
	footerLength := int(r.postScript.GetFooterLength())
	footerBytes := make([]byte, footerLength, footerLength)
	footerOffset := size - psLen - 1 - footerLength

	// Get the offset and length of the metadata and preallocate a byte slice.
	metadataLength := int(r.postScript.GetMetadataLength())
	metadataBytes := make([]byte, metadataLength, metadataLength)
	metadataOffset := size - psLen - 1 - footerLength - metadataLength

	// Create a section reader containing the metadata and read into the byte slice.
	metadataReader := io.NewSectionReader(r.r, int64(metadataOffset), int64(metadataLength))
	_, err = io.ReadFull(metadataReader, metadataBytes)
	if err != nil {
		return err
	}

	// Create a section reader containing the footer and read into the byte slice.
	footerReader := io.NewSectionReader(r.r, int64(footerOffset), int64(footerLength))
	_, err = io.ReadFull(footerReader, footerBytes)
	if err != nil {
		return err
	}

	// Retrieve the CompressionCodec.
	codec, err := r.getCodec()
	if err != nil {
		return err
	}

	// Decode the metadata into a new byte slice.
	metadataDecoder := codec.Decoder(bytes.NewReader(metadataBytes))
	decodedMetadataBytes, err := ioutil.ReadAll(metadataDecoder)
	if err != nil {
		return err
	}

	// Unmarshal the metadata and store against the reader.
	r.metadata = &proto.Metadata{}
	err = gproto.Unmarshal(decodedMetadataBytes, r.metadata)
	if err != nil {
		return err
	}

	// Decode the footer into a new byte slice.
	footerDecoder := codec.Decoder(bytes.NewReader(footerBytes))
	decodedFooterBytes, err := ioutil.ReadAll(footerDecoder)
	if err != nil {
		return err
	}

	// Unmarshal the footer and store against the reader.
	r.footer = &proto.Footer{}
	err = gproto.Unmarshal(decodedFooterBytes, r.footer)
	if err != nil {
		return err
	}

	// Store the types for access later
	types := r.footer.GetTypes()
	for i, t := range types {
		r.types[i] = t
	}

	// Prepare the first stripe by loading it into memory
	// and creating the required readers.
	err = r.prepareStripeReader()
	if err != nil {
		return err
	}

	return nil

}

func (r *Reader) prepareStripeReader() error {
	stripes, err := r.getStripes()
	if err != nil {
		return err
	}
	r.stripesLength = len(stripes)
	if r.currentStripeOffset >= r.stripesLength {
		return io.EOF
	}

	stripe := stripes[r.currentStripeOffset]
	r.currentStripeOffset++

	// Unmarshal the stripe footer
	stripeOffset := int64(stripe.GetOffset())
	stripeFooterOffset := stripeOffset + int64(stripe.GetIndexLength()+stripe.GetDataLength())
	stripeFooterLength := int64(stripe.GetFooterLength())
	stripeFooterReader := io.NewSectionReader(r.r, stripeFooterOffset, stripeFooterLength)
	stripeFooterBytes := make([]byte, stripeFooterLength, stripeFooterLength)
	_, err = io.ReadFull(stripeFooterReader, stripeFooterBytes)
	if err != nil {
		return err
	}
	codec, err := r.getCodec()
	if err != nil {
		return err
	}

	// Decode the footer into a new byte slice.
	stripeFooterDecoder := codec.Decoder(bytes.NewReader(stripeFooterBytes))
	decodedStripeFooterBytes, err := ioutil.ReadAll(stripeFooterDecoder)
	if err != nil {
		return err
	}

	// Unmarshal the footer and store against the reader.
	stripeFooter := &proto.StripeFooter{}
	err = gproto.Unmarshal(decodedStripeFooterBytes, stripeFooter)
	if err != nil {
		return err
	}

	// Store the columns and their encoding types so that we can access them later.
	columns := stripeFooter.GetColumns()
	for i, column := range columns {
		r.columns[i] = column
	}

	// Iterate through the streams and allocate byte buffers for each.
	streamOffset := stripeOffset
	streams := stripeFooter.GetStreams()

	for _, stream := range streams {
		streamLength := int64(stream.GetLength())
		streamReader := io.NewSectionReader(r.r, streamOffset, streamLength)

		codec, err := r.getCodec()
		if err != nil {
			return err
		}

		dec := codec.Decoder(streamReader)

		var streamBuf bytes.Buffer
		_, err = io.Copy(&streamBuf, dec)
		if err != nil {
			return err
		}

		// Store the byte buffer within the streamMap
		name := streamName{
			columnID: int(stream.GetColumn()),
			kind:     stream.GetKind(),
		}
		r.streams[name] = streamBuf

		streamOffset += streamLength
	}

	return nil
}

func (r *Reader) getTypes() ([]*proto.Type, error) {
	if r.footer != nil {
		return r.footer.GetTypes(), nil
	}
	return nil, errNoFooter
}

func (r *Reader) getStripes() ([]*proto.StripeInformation, error) {
	if r.footer != nil {
		return r.footer.GetStripes(), nil
	}
	return nil, errNoFooter
}

func (r *Reader) Close() error {
	return nil
}
