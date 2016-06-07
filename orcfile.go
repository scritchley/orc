package orc

type EncodingStrategy int

const (
	EncodingStrategySpeed       EncodingStrategy = 0
	EncodingStrategyCompression EncodingStrategy = 1
)

type CompressionStrategy int

const (
	CompressionStrategySpeed       CompressionStrategy = 0
	CompressionStrategyCompression CompressionStrategy = 1
)

type Version struct {
	name  string
	major int
	minor int
}

var (
	Version0_11 = Version{"0.11", 0, 11}
	Version0_12 = Version{"0.12", 0, 12}
)
