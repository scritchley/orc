package orc

import (
	"code.simon-critchley.co.uk/orc/proto"
)

type statisticsMap map[int]ColumnStatistics

func NewColumnStatistics(category Category) ColumnStatistics {
	switch category {
	case CategoryInt, CategoryShort, CategoryLong:
		return NewIntegerColumnStatistics()
	default:
		return NewBaseStatistics()
	}
}

func (e statisticsMap) add(id int, category Category) ColumnStatistics {
	e[id] = NewColumnStatistics(category)
	return e[id]
}

func (e statisticsMap) reset() {
	for k := range e {
		delete(e, k)
	}
}

func (e statisticsMap) statistics() []*proto.ColumnStatistics {
	statistics := make([]*proto.ColumnStatistics, len(e))
	for i := range statistics {
		statistics[i] = e[i].Statistics()
	}
	return statistics
}

func (e statisticsMap) merge(other statisticsMap) {
	for i := range other {
		if _, ok := e[i]; ok {
			e[i].Merge(other[i])
		} else {
			e[i] = other[i]
		}
	}
}

func (e statisticsMap) forEach(fn func(i int, c ColumnStatistics)) {
	for i := 0; i < len(e); i++ {
		s := e[i]
		fn(i, s)
	}
}

type ColumnStatistics interface {
	Statistics() *proto.ColumnStatistics
	Add(value interface{})
	Merge(other ColumnStatistics)
	Reset()
}

type BaseStatistics struct {
	*proto.ColumnStatistics
}

func NewBaseStatistics() BaseStatistics {
	return BaseStatistics{
		&proto.ColumnStatistics{},
	}
}

func (b BaseStatistics) Add(value interface{}) {
	if hasNull := value == nil; hasNull {
		b.HasNull = &hasNull
	}
	n := b.ColumnStatistics.GetNumberOfValues() + 1
	b.ColumnStatistics.NumberOfValues = &n
}

func (b BaseStatistics) Merge(other ColumnStatistics) {
	if bs, ok := other.(BaseStatistics); ok {
		numValues := b.GetNumberOfValues() + bs.GetNumberOfValues()
		b.NumberOfValues = &numValues
	}
}

func (b BaseStatistics) Statistics() *proto.ColumnStatistics {
	return b.ColumnStatistics
}

type IntegerStatistics struct {
	BaseStatistics
	minSet bool
}

func (i *IntegerStatistics) Merge(other ColumnStatistics) {
	if is, ok := other.(*IntegerStatistics); ok {
		if is.IntStatistics.GetMaximum() > i.IntStatistics.GetMaximum() {
			i.IntStatistics.Maximum = is.IntStatistics.Maximum
		}
		if is.IntStatistics.GetMinimum() < i.IntStatistics.GetMinimum() {
			i.IntStatistics.Minimum = is.IntStatistics.Minimum
		}
		sum := i.IntStatistics.GetSum() + is.IntStatistics.GetSum()
		i.IntStatistics.Sum = &sum
		i.BaseStatistics.Merge(is.BaseStatistics)
	}
}

func (i *IntegerStatistics) Add(value interface{}) {
	if val, ok := value.(int64); ok {
		if val > i.IntStatistics.GetMaximum() {
			i.IntStatistics.Maximum = &val
		}
		if !i.minSet {
			i.IntStatistics.Minimum = &val
			i.minSet = true
		}
		if val < i.IntStatistics.GetMinimum() {
			i.IntStatistics.Minimum = &val
		}
		sum := i.IntStatistics.GetSum() + val
		i.IntStatistics.Sum = &sum
	}
	i.BaseStatistics.Add(value)
}

func (i *IntegerStatistics) Statistics() *proto.ColumnStatistics {
	return i.ColumnStatistics
}

func (i *IntegerStatistics) Reset() {
	*i = *NewIntegerColumnStatistics()
}

func NewIntegerColumnStatistics() *IntegerStatistics {
	base := NewBaseStatistics()
	base.IntStatistics = &proto.IntegerStatistics{}
	return &IntegerStatistics{
		BaseStatistics: base,
	}
}

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
