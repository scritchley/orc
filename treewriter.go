package orc

// import (
// 	"io"

// 	"code.simon-critchley.co.uk/orc/proto"
// )

// type RowIndexPositionRecorder interface{}
// type BloomFilterIO interface{}

// type TreeWriter struct {
// 	*Writer
// 	id                  int
// 	isPresent           *BooleanWriter
// 	isCompressed        bool
// 	indexStatistics     *ColumnStatistics
// 	stripeColStatistics *ColumnStatistics
// 	fileStatistics      *ColumnStatistics
// 	childrenWriters     []*TreeWriter
// 	rowIndexPosition    RowIndexPositionRecorder
// 	rowIndex            *proto.RowIndex
// 	rowIndexEntry       *proto.RowIndexEntry
// 	rowIndexStream      io.Writer
// 	bloomFilterStream   io.Writer
// 	bloomFilter         BloomFilterIO
// 	createBloomFilter   bool
// 	bloomFilterIndex    *proto.BloomFilterIndex
// 	bloomFilterEntry    *proto.BloomFilter
// 	foundNulls          bool
// 	isPresentOutStream  io.Writer
// 	stripeStatsBuilders []*proto.StripeStatistics
// }

// func NewTreeWriter(columnID int, schema TypeDescription, w *Writer, nullable bool) (*TreeWriter, error) {
// 	tw := &TreeWriter{
// 		Writer:       w,
// 		isCompressed: w.isCompressed(),
// 		id:           columnID,
// 	}
// 	if nullable {
// 		tw.isPresentOutStream = w.createStream(columnID, proto.Stream_PRESENT)
// 		tw.isPresent = NewBooleanWriter(tw.isPresentOutStream)
// 	}
// 	tw.foundNulls = false
// 	tw.createBloomFilter = w.getBloomFilterColumns()[columnID]
// 	tw.indexStatistics = NewColumnStatistics(schema)
// 	tw.stripeColStatistics = NewColumnStatistics(schema)
// 	tw.fileStatistics = NewColumnStatistics(schema)
// 	tw.childrenWriters = make([]*TreeWriter, 0)
// 	tw.rowIndex = &proto.RowIndex{}
// 	tw.rowIndexEntry = &proto.RowIndexEntry{}
// 	tw.rowIndexPosition = NewRowIndexPositionRecorder(tw.rowIndexEntry)
// 	if w.buildIndex() {
// 		tw.rowIndexStream = w.createStream(tw.id, proto.Stream_ROW_INDEX)
// 	}
// 	if tw.createBloomFilter {
// 		tw.bloomFilterEntry = &proto.BloomFilter{}
// 		tw.bloomFilterEntry = &proto.BloomFilterIndex{}
// 		tw.bloomFilterStream = w.createStream(tw.id, proto.Stream_BLOOM_FILTER)
// 		tw.bloomFilter = NewBloomFilterIO(w.getRowIndexStride(), w.getBloomFilterFPP())
// 	}
// 	return tw, nil
// }

// func (tw *TreeWriter) getRowIndex() *proto.RowIndex {
// 	return tw.rowIndex
// }

// func (tw *TreeWriter) getStripeStatistics() *ColumnStatistics {
// 	return tw.stripeColStatistics
// }

// func (tw *TreeWriter) getRowIndexEntry() *proto.RowIndexEntry {
// 	return tw.rowIndexEntry
// }

// func (tw *TreeWriter) createIntegerWriter(w io.ByteWriter, signed bool, isDirectV2 bool) IntegerWriter {
// 	if isDirectV2 {
// 		alignedBitPacking := false
// 		if w.getEncodingStrategy() == EncodingStrategySpeed {
// 			alignedBitPacking = true
// 		}
// 		return NewRunLengthIntegerWriterV2(w, signed, alignedBitPacking)
// 	}
// 	return NewRunLengthIntegerWriter(w, signed)
// }

// func (tw *TreeWriter) isNewWriteFormat(w *Writer) bool {
// 	return w.getVersion() != ORCFileVersionV_0_11
// }

// func (tw *TreeWriter) writeRootBatch(batch VectorizedRowBatch, offset int, length int) error {
// 	return tw.writeBatch(batch.cols[0], offset, length)
// }

// func (tw *TreeWriter) writeBatch(vector ColumnVector, offset int, length int) error {
// 	if vector.noNulls {
// 		tw.indexStatistics.increment(length)
// 		if tw.isPresent != nil {
// 			for i := 0; i < length; i++ {
// 				err := tw.isPresent.WriteBool(true)
// 				if err != nil {
// 					return err
// 				}
// 			}
// 		}
// 	} else {
// 		if vector.isRepeating {
// 			isNull := vector.isNull[0]
// 			if tw.isPresent != nil {
// 				for i := 0; i < length; i++ {
// 					err := tw.isPresent.WriteBool(!isNull)
// 					if err != nil {
// 						return err
// 					}
// 				}
// 			}
// 			if isNull {
// 				tw.foundNulls = true
// 				tw.indexStatistics.setNull()
// 			} else {
// 				tw.indexStatistics.increment(length)
// 			}
// 		} else {
// 			var nonNullCount int
// 			for i := 0; i < length; i++ {
// 				isNull := vector.isNull[i+offset]
// 				if !isNull {
// 					nonNullCount++
// 				}
// 				if tw.isPresent != nil {
// 					err := tw.isPresent.WriteBool(!isNull)
// 					if err != nil {
// 						return err
// 					}
// 				}
// 			}
// 			tw.indexStatistics.increment(nonNullCount)
// 			if nonNullCount != length {
// 				tw.foundNulls = true
// 				tw.indexStatistics.setNull()
// 			}
// 		}
// 	}
// }

// func (tw *TreeWriter) removeIsPresentPositions() {
// 	for _, entry := range tw.rowIndex.GetEntry() {
// 		positions := entry.GetPositions()
// 		offset := 3
// 		// bit streams use 3 positions if uncompressed, 4 if compressed
// 		if tw.isCompressed {
// 			offset = 4
// 		}
// 		entry.Positions = positions[offset:]
// 	}
// }

// func (tw *TreeWriter) writeStripe(footer *proto.StripeFooter, requiredIndexEntries int) error {
// 	if tw.isPresent != nil {
// 		err := tw.isPresent.Flush()
// 		if err != nil {
// 			return err
// 		}
// 		// if no nulls are found in a stream, then suppress the stream
// 		if !tw.foundNulls {
// 			tw.isPresentOutStream.suppress()
// 			// since isPresent bitstream is suppressed, update the index to
// 			// remove the positions of the isPresent stream
// 			if tw.rowIndexStream != nil {
// 				tw.removeIsPresentPositions()
// 			}
// 		}
// 	}

// 	// merge stripe-level column statistics to file statistics and write it to
// 	// stripe statistics
// 	stripeStats := &proto.StripeStatistics{}
// 	tw.writeStripeStatistics(stripeStats)
// 	tw.stripeStatsBuilders = append(tw.stripeStatsBuilders, stripeStats)

// 	// reset the flag for next stripe
// 	tw.foundNulls = false

// 	footer.Columns = append(footer.Columns, tw.getEncoding())
// 	if w.hasWriterTimezone() {
// 		footer.WriterTimezone = w.getTimezone()
// 	}
// 	if tw.rowIndexStream != nil {
// 		if l := len(tw.rowIndex.GetEntry()); l != requiredIndexEntries {
// 			return fmt.Errorf("Column has wrong number of index entries found: %v expected: %v", l, requiredIndexEntries)
// 		}
// 		err := writeProto(tw.rowIndex, tw.rowIndexStream)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	tw.rowIndex.Reset()
// 	tw.rowIndexEntry.Reset()
// 	if tw.bloomFilterStream != nil {
// 		err := writeProto(tw.bloomFilterIndex, tw.bloomFilterStream)
// 		if err != nil {
// 			return err
// 		}
// 		tw.bloomFilterIndex.Reset()
// 		tw.bloomFilterEntry.Reset()
// 	}
// }

// func (tw *TreeWriter) writeStripeStatistics(stats *proto.StripeStatistics, t *TreeWriter) {
// 	t.fileStatistics.merge(t.stripeColStatistics)
// 	stats.ColStats = append(stats.ColStats, t.stripeColStatistics)
// 	for _, child := range t.getChildrenWriters {
// 		tw.writeStripeStatistics(stats, child)
// 	}
// }

// func (tw *TreeWriter) getChildrenWriters() []*TreeWriter {
// 	return tw.childrenWriters
// }

// func (tw *TreeWriter) getEncoding() *proto.ColumnEncoding {
// 	return newColumnEncoding(proto.ColumnEncoding_DIRECT)
// }

// func (tw *TreeWriter) createRowIndexEntry() error {
// 	tw.stripeColStatistics.merge(tw.indexStatistics)
// 	tw.rowIndexEntry.Statistics = tw.indexStatistics
// 	tw.indexStatistics.reset()
// 	tw.rowIndex.Entry = append(tw.rowIndex.Entry, tw.getRowIndexEntry)
// 	tw.rowIndexEntry.Reset()
// 	tw.addBloomFilterEntry()
// 	tw.recordPosition(tw.rowIndexPosition)
// 	for _, child := range tw.childrenWriters {
// 		err := child.createRowIndexEntry()
// 		if err != nil {
// 			return err
// 		}
// 	}
// }

// func (tw *TreeWriter) addBloomFilterEntry() {
// 	if tw.createBloomFilter {
// 		tw.bloomFilterEntry.NumHashFunctions = tw.bloomFilter.getNumHashFunctions()
// 		tw.bloomFilterEntry.Bitset = append(tw.bloomFilterEntry.Bitset, tw.bloomFilter.getBitSet()...)
// 		tw.bloomFilterIndex.BloomFilter = append(tw.bloomFilterIndex.BloomFilter, tw.bloomFilterEntry)
// 		tw.bloomFilter.reset()
// 		tw.bloomFilterEntry.Reset()
// 	}
// }
