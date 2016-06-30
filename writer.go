package orc

import (
	"fmt"
	"io"

	gproto "github.com/golang/protobuf/proto"

	"code.simon-critchley.co.uk/orc/proto"
)

var (
	magic                              = "ORC"
	stripeTargetSize            int64  = 200 * 1024 * 1024
	DefaultCompressionChunkSize uint64 = 256 * 1024
	DefaultRowIndexStride       uint32 = 10000
)

type Writer struct {
	schema            *TypeDescription
	streams           streamWriterMap
	w                 io.Writer
	treeWriter        TreeWriter
	columns           []*proto.ColumnEncoding
	stripeRows        uint64
	stripeOffset      uint64
	stripeLength      uint64
	stripeIndexOffset uint64
	stripeTargetSize  int64
	footer            *proto.Footer
	footerLength      uint64
	postScript        *proto.PostScript
	postScriptLength  uint8
	metadata          *proto.Metadata
	metadataLength    uint64
	totalRows         uint64
	statistics        statisticsMap
	currentStatistics statisticsMap
	indexStatistics   statisticsMap
	indexes           map[int]*proto.RowIndex
	indexOffset       uint64
	chunkOffset       uint64
}

func ptrInt64(i int64) *int64 {
	return &i
}

type WriterConfigFunc func(w *Writer) error

func SetSchema(schema *TypeDescription) WriterConfigFunc {
	return func(w *Writer) error {
		w.schema = schema
		w.footer.Types = w.schema.Types()
		return nil
	}
}

func NewWriter(w io.Writer, fns ...WriterConfigFunc) (*Writer, error) {
	writer := &Writer{
		w:                w,
		stripeOffset:     uint64(len(magic)),
		stripeTargetSize: stripeTargetSize,
		streams:          make(streamWriterMap),
		statistics:       make(statisticsMap),
		indexStatistics:  make(statisticsMap),
		indexes:          make(map[int]*proto.RowIndex),
		footer: &proto.Footer{
			RowIndexStride: &DefaultRowIndexStride,
			Statistics:     []*proto.ColumnStatistics{},
		},
		postScript: &proto.PostScript{
			Magic:                &magic,
			CompressionBlockSize: &DefaultCompressionChunkSize,
			Compression:          proto.CompressionKind_NONE.Enum(),
			Version:              []uint32{Version0_12.major, Version0_12.minor},
		},
		metadata: &proto.Metadata{
			StripeStats: []*proto.StripeStatistics{},
		},
	}
	for _, fn := range fns {
		err := fn(writer)
		if err != nil {
			return nil, err
		}
	}
	err := writer.init()
	if err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Writer) getCodec() (CompressionCodec, error) {
	switch kind := w.postScript.GetCompression(); kind {
	case proto.CompressionKind_NONE:
		return CompressionNone{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression kind %s", kind)
	}
}

func (w *Writer) Write(values ...interface{}) error {
	w.stripeRows++
	w.totalRows++
	err := w.treeWriter.Write(values)
	if err != nil {
		return err
	}
	if w.totalRows%uint64(w.footer.GetRowIndexStride()) == 0 {
		err := w.writeIndexes()
		if err != nil {
			return err
		}
		if w.streams.size() >= w.stripeTargetSize {
			err := w.writeStripe()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Writer) init() error {
	if err := w.initOrc(); err != nil {
		return err
	}
	if err := w.initWriters(); err != nil {
		return err
	}
	return nil
}

func (w *Writer) initOrc() error {
	_, err := w.w.Write([]byte(magic))
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) initWriters() error {
	// Assumes that the top level type is always a struct, as is specified
	// in the ORC format docs.
	columns := make(encodingMap)
	currentStatistics := make(statisticsMap)
	indexStatistics := make(statisticsMap)
	codec, err := w.getCodec()
	if err != nil {
		return err
	}
	w.treeWriter, err = createTreeWriter(codec, w.schema, w.streams, columns, currentStatistics, indexStatistics)
	if err != nil {
		return err
	}
	w.columns = columns.encodings()
	w.currentStatistics = currentStatistics
	w.indexStatistics = indexStatistics
	return nil
}

func (w *Writer) closeWriters() error {
	return w.treeWriter.Close()
}

func (w *Writer) flushWriters() error {
	return w.treeWriter.Flush()
}

func (w *Writer) writePostScript() error {
	byt, err := gproto.Marshal(w.postScript)
	if err != nil {
		return err
	}
	if len(byt) > maxPostScriptSize {
		return fmt.Errorf("postscript larger than max allowed size of %v bytes: %v", maxPostScriptSize, len(byt))
	}
	_, err = w.w.Write(byt)
	if err != nil {
		return err
	}
	// Write the length of the post script in the last byte
	_, err = w.w.Write([]byte{byte(len(byt))})
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeFooter() error {
	totalRows := w.totalRows
	w.footer.NumberOfRows = &totalRows
	w.footer.Statistics = w.statistics.statistics()
	byt, err := gproto.Marshal(w.footer)
	if err != nil {
		return err
	}
	footerLength := uint64(len(byt))
	w.postScript.FooterLength = &footerLength
	_, err = w.w.Write(byt)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeMetadata() error {
	byt, err := gproto.Marshal(w.metadata)
	if err != nil {
		return err
	}
	metadataLength := uint64(len(byt))
	w.postScript.MetadataLength = &metadataLength
	_, err = w.w.Write(byt)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeIndexes() error {
	if err := w.flushWriters(); err != nil {
		return err
	}
	for i := 0; i < len(w.indexStatistics); i++ {
		// Get the current offset for the stream
		c := w.indexStatistics[i]
		if rowIndex, ok := w.indexes[i]; ok {
			w.indexes[i].Entry = append(rowIndex.Entry, &proto.RowIndexEntry{
				Positions:  w.streams.positions(i),
				Statistics: c.Statistics(),
			})
		} else {
			w.indexes[i] = &proto.RowIndex{
				Entry: []*proto.RowIndexEntry{
					&proto.RowIndexEntry{
						Positions:  w.streams.positions(i),
						Statistics: c.Statistics(),
					},
				},
			}
		}
		c.Reset()
	}
	return nil
}

func (w *Writer) writeStripe() error {

	// Close the current set of writers.
	if err := w.closeWriters(); err != nil {
		return err
	}

	// Write each stream to the underlying writer.
	var streams []*proto.Stream
	var stripeDataLength uint64

	// Write the row indexes to the stream first.
	for i := 0; i < len(w.indexes); i++ {
		byt, err := gproto.Marshal(w.indexes[i])
		if err != nil {
			return err
		}
		streamInfo := &proto.Stream{
			Column: ptrUint32(uint32(i)),
			Kind:   proto.Stream_ROW_INDEX.Enum(),
			Length: ptrUint64(uint64(len(byt))),
		}
		stripeDataLength += uint64(len(byt))
		streams = append(streams, streamInfo)
		_, err = w.w.Write(byt)
		if err != nil {
			return err
		}
	}
	w.indexes = make(map[int]*proto.RowIndex)

	// Write the remaining streams
	for name, stream := range w.streams {
		kind := name.kind
		streamInfo := &proto.Stream{
			Column: ptrUint32(uint32(name.columnID)),
			Kind:   &kind,
			Length: ptrUint64(uint64(stream.Len())),
		}
		stripeDataLength += uint64(stream.Len())
		streams = append(streams, streamInfo)
		_, err := stream.WriteTo(w.w)
		if err != nil {
			return err
		}
	}

	// Reset the streams ready for the next stripe
	w.streams.reset()

	// Create a stripe footer and write it to the underlying writer.
	stripeFooter := &proto.StripeFooter{
		Streams: streams,
		Columns: w.columns,
	}

	byt, err := gproto.Marshal(stripeFooter)
	if err != nil {
		return err
	}
	_, err = w.w.Write(byt)
	if err != nil {
		return err
	}

	// Append the stripe information to the footer.
	indexLength := w.stripeIndexOffset
	stripeRows := w.stripeRows
	// Reset the stripe rows ready for the next stripe.
	w.stripeRows = 0
	w.stripeIndexOffset = 0

	// Append stripe information to the footer
	footerLength := uint64(len(byt))
	offset := w.stripeOffset
	w.footer.Stripes = append(w.footer.Stripes, &proto.StripeInformation{
		Offset:       &offset,
		IndexLength:  &indexLength,
		DataLength:   ptrUint64(stripeDataLength),
		FooterLength: &footerLength,
		NumberOfRows: &stripeRows,
	})

	// Update the stripe offset for the next stripe
	w.stripeOffset += stripeDataLength + footerLength

	// Add stripe statistics to metadata
	stripeColStats := w.currentStatistics.statistics()
	w.metadata.StripeStats = append(w.metadata.StripeStats, &proto.StripeStatistics{
		ColStats: stripeColStats,
	})

	// Merge the stripe statistics with the total statistics.
	w.statistics.merge(w.currentStatistics)
	return w.initWriters()
}

func (w *Writer) Close() error {
	if err := w.writeIndexes(); err != nil {
		return err
	}
	if err := w.writeStripe(); err != nil {
		return err
	}
	if err := w.writeMetadata(); err != nil {
		return err
	}
	if err := w.writeFooter(); err != nil {
		return err
	}
	if err := w.writePostScript(); err != nil {
		return err
	}
	return nil
}

func ptrUint32(u uint32) *uint32 {
	return &u
}

func ptrUint64(u uint64) *uint64 {
	return &u
}
