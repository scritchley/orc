package orc

const (
	VectorRowBatchDefaultSize = 1024
)

type ColumnVector struct {
	isNull                []bool
	noNulls               bool
	isRepeating           bool
	preFlattenIsRepeating bool
	preFlattenNoNulls     bool
}

type Type int

// const (
// 	None Type = iota
// 	Long
// 	Double
// 	Bytes
// 	Decimal
// 	Timestamp
// 	Struct
// 	Map
// 	Union
// )

func NewColumnVector(length int) *ColumnVector {
	return &ColumnVector{
		isNull:            make([]bool, length, length),
		noNulls:           true,
		preFlattenNoNulls: true,
	}
}

func (c *ColumnVector) Reset() {
	if c.noNulls {
		for i := range c.isNull {
			c.isNull[i] = false
		}
	}
	c.noNulls = true
	c.isRepeating = false
	c.preFlattenNoNulls = true
	c.preFlattenIsRepeating = false
}

func (c *ColumnVector) flattenRepeatingNulls(selectedInUse bool, sel []int, size int) {
	var nullFillValue bool
	if !c.noNulls {
		nullFillValue = c.isNull[0]
	}
	if selectedInUse {
		for j := 0; j < size; j++ {
			i := sel[j]
			c.isNull[i] = nullFillValue
		}
	} else {
		for i := 0; i < size; i++ {
			c.isNull[i] = nullFillValue
		}
	}
}

func (c *ColumnVector) flattenNoNulls(selectedInUse bool, sel []int, size int) {
	if c.noNulls {
		c.noNulls = false
		if selectedInUse {
			for j := 0; j < size; j++ {
				i := sel[j]
				c.isNull[i] = false
			}
		} else {
			for i := 0; i < size; i++ {
				c.isNull[i] = false
			}
		}
	}
}

func (c *ColumnVector) unFlatten() {
	c.isRepeating = c.preFlattenIsRepeating
	c.noNulls = c.preFlattenNoNulls
}

func (c *ColumnVector) flattenPush() {
	c.preFlattenIsRepeating = c.isRepeating
	c.preFlattenNoNulls = c.noNulls
}

func (c *ColumnVector) ensureSize(size int, preserveData bool) {
	if len(c.isNull) < size {
		oldArray := c.isNull
		c.isNull = make([]bool, size)
		if preserveData && !c.noNulls {
			if c.isRepeating {
				c.isNull[0] = oldArray[0]
			} else {
				copy(c.isNull, oldArray)
			}
		}
	}
}
