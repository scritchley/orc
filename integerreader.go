package orc

type IntegerReader interface {
	HasNext() bool
	NextInt() int64
	Error() error
}
