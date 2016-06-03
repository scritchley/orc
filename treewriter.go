package orc

import (
	"io"

	"code.simon-critchley.co.uk/orc/proto"
)

type RowIndexPositionRecorder interface{}
type BloomFilterIO interface{}

type TreeWriter struct {
	id                  int
	isPresent           BooleanWriter
	isCompressed        bool
	indexStatistics     *ColumnStatistics
	stripeColStatistics *ColumnStatistics
	fileStatistics      *ColumnStatistics
	childrenWriters     []*TreeWriter
	rowIndexPosition    RowIndexPositionRecorder
	rowIndex            *proto.RowIndex
	rowIndexEntry       *proto.RowIndexEntry
	rowIndexStream      io.ByteWriter
	bloomFilterStream   io.ByteWriter
	bloomFilter         BloomFilterIO
	createBloomFilter   bool
	bloomFilterIndex    *proto.BloomFilterIndex
	bloomFilterEntry    *proto.BloomFilter
	foundNulls          bool
	isPresentOutStream  io.ByteWriter
	stripeStatsBuilder  []*proto.StripeStatistics
	streamFactory       StreamFactory
}

func NewTreeWriter(columnID int, schema TypeDescription, streamFactory StreamFactory, nullable bool) (*TreeWriter, error) {
	tw := &TreeWriter{
		Writer:        Writer{},
		streamFactory: streamFactory,
		isCompressed:  streamFactory.isCompressed(),
		id:            columnID,
	}
	if nullable {
		tw.isPresentOutStream = streamFactory.createStream(id, proto.Stream_PRESENT)
		tw.isPresent = NewBooleanWriter(tw.isPresentOutStream)
	}
	tw.foundNulls = false
	tw.createBloomFilter = streamFactory.getBloomFilterColumns()[columnID]
	tw.indexStatistics = NewColumnStatistics(schema)
	tw.stripeColStatistics = NewColumnStatistics(schema)
	tw.fileStatistics = NewColumnStatistics(schema)
	tw.childrenWriters = make([]*TreeWriter, 0)
	tw.rowIndex = &proto.RowIndex{}
	tw.rowIndexEntry = &proto.RowIndexEntry{}
	tw.rowIndexPosition = NewRowIndexPositionRecorder(tw.rowIndexEntry)
	if streamFactory.buildIndex() {
		tw.rowIndexStream = streamFactory.createStream(tw.id, proto.Stream_ROW_INDEX)
	}
	if tw.createBloomFilter {
		tw.bloomFilterEntry = &proto.BloomFilter{}
		tw.bloomFilterEntry = &proto.BloomFilterIndex{}
		tw.bloomFilterStream = streamFactory.createStream(tw.id, proto.Stream_BLOOM_FILTER)
		tw.bloomFilter = NewBloomFilterIO(streamFactory.getRowIndexStride(), streamFactory.getBloomFilterFPP())
	}
	return tw, nil
}

func (tw *TreeWriter) getRowIndex() *proto.RowIndex {
	return tw.rowIndex
}

func (tw *TreeWriter) getStripeStatistics() *ColumnStatistics {
	return tw.stripeColStatistics
}

func (tw *TreeWriter) getRowIndexEntry() *proto.RowIndexEntry {
	return tw.rowIndexEntry
}

func (tw *TreeWriter) createIntegerWriter(w io.ByteWriter, signed bool, isDirectV2 bool) IntegerWriter {
	if isDirectV2 {
		alignedBitPacking := false
		// TODO: Check streamFactorys requirement
		return NewRunLengthIntegerWriterV2(w, signed, alignedBitPacking)
	}
	return NewRunLengthIntegerWriter(w, signed)
}
