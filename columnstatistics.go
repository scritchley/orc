package orc

import (
	"code.simon-critchley.co.uk/orc/proto"
)

type statisticsMap map[int]ColumnStatistics

func NewColumnStatistics(category Category) ColumnStatistics {
	switch category {
	case CategoryInt, CategoryShort, CategoryLong:
		return NewIntegerStatistics()
	case CategoryString:
		return NewStringStatistics()
	case CategoryBoolean:
		return NewBucketStatistics()
	default:
		return NewBaseStatistics()
	}
}

func (e statisticsMap) add(id int, stats ColumnStatistics) {
	if _, ok := e[id]; ok {
		e[id].Merge(stats)
	} else {
		e[id] = stats
	}
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
	*i = *NewIntegerStatistics()
}

func NewIntegerStatistics() *IntegerStatistics {
	base := NewBaseStatistics()
	base.IntStatistics = &proto.IntegerStatistics{}
	return &IntegerStatistics{
		BaseStatistics: base,
	}
}

type StringStatistics struct {
	BaseStatistics
	minSet bool
}

func NewStringStatistics() *StringStatistics {
	base := NewBaseStatistics()
	base.StringStatistics = &proto.StringStatistics{}
	return &StringStatistics{
		BaseStatistics: base,
	}
}

func (s *StringStatistics) Merge(other ColumnStatistics) {
	if ss, ok := other.(*StringStatistics); ok {
		if ss.StringStatistics.GetMaximum() > s.StringStatistics.GetMaximum() {
			s.StringStatistics.Maximum = ss.StringStatistics.Maximum
		}
		if ss.StringStatistics.GetMinimum() < s.StringStatistics.GetMinimum() {
			s.StringStatistics.Minimum = ss.StringStatistics.Minimum
		}
		sum := s.StringStatistics.GetSum() + ss.StringStatistics.GetSum()
		s.StringStatistics.Sum = &sum
		s.BaseStatistics.Merge(ss.BaseStatistics)
	}
}

func (s *StringStatistics) Add(value interface{}) {
	if val, ok := value.(string); ok {
		if val > s.StringStatistics.GetMaximum() {
			s.StringStatistics.Maximum = &val
		}
		if !s.minSet {
			s.StringStatistics.Minimum = &val
			s.minSet = true
		}
		if val < s.StringStatistics.GetMinimum() {
			s.StringStatistics.Minimum = &val
		}
		sum := s.StringStatistics.GetSum() + int64(len(val))
		s.StringStatistics.Sum = &sum
	}
	s.BaseStatistics.Add(value)
}

func (s *StringStatistics) Reset() {
	*s = *NewStringStatistics()
}

func (s *StringStatistics) Statistics() *proto.ColumnStatistics {
	return s.ColumnStatistics
}

type BucketStatistics struct {
	BaseStatistics
}

func NewBucketStatistics() *BucketStatistics {
	base := NewBaseStatistics()
	base.BucketStatistics = &proto.BucketStatistics{}
	return &BucketStatistics{
		base,
	}
}

func (b *BucketStatistics) Add(value interface{}) {
	if t, ok := value.(bool); ok {
		b.BaseStatistics
	}
	b.BaseStatistics.Add(value)
}
