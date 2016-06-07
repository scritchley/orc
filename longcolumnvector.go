package orc

const (
	LongColumnVectorNullValue = 1
)

type LongColumnVector struct {
	*ColumnVector
	vector []int64
}

func NewLongColumnVector() *LongColumnVector {
	return &LongColumnVector{
		NewColumnVector(VectorRowBatchDefaultSize),
		make([]int64, 0),
	}
}

func (l *LongColumnVector) Populate(values ...int) {

}
