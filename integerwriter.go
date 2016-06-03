package orc

type IntegerWriter interface {
	WriteInt(int64) error
	Flush() error
	Close() error
}
