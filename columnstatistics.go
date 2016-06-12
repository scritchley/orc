package orc

// import (
// 	"code.simon-critchley.co.uk/orc/proto"
// )

// type ColumnStatistics interface {
// 	reset()
// 	merge(other ColumnStatistics)
// 	increment(length int)
// }

// func NewColumnStatistics(stats proto.ColumnStatistics) ColumnStatistics {

// }

// type BooleanStatistics struct {
// 	trueCount int64
// }

// func NewBooleanStatistics(stats proto.ColumnStatistics) *BooleanStatistics {
// 	bkt := stats.GetBucketStatistics()
// 	vals := bkt.GetCount()
// 	if len(vals) > 0 {
// 		bs.trueCount = int64(vals[0])
// 	}
// 	return bs
// }

// func (b *BooleanStatistics) reset() {
// 	b.ColumnStatistics.reset()
// 	b.trueCount = 0
// }

// func (b *BooleanStatistics) updateBoolean(value bool, repetitions int) {
// 	if value {
// 		b.trueCount += int64(repetitions)
// 	}
// }

// func (b *BooleanStatistics) merge(other ColumnStatisticsInterface) {
// 	if b2, ok := other.(*BooleanStatistics); ok {
// 		b.trueCount += b2.trueCount
// 	}
// 	b.ColumnStatistics.merge(other)
// }
