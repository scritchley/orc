package orc

import (
	"encoding/asn1"
	"encoding/json"
	"io"
	"math"
	"math/big"
)

// Decimal is a decimal type.
type Decimal struct {
	Abs *big.Int
	Exp int64
}

// Float64 returns the float64 equivalent of the Decimal value.
func (d Decimal) Float64() float64 {
	return float64(d.Abs.Int64()) / (1 / math.Pow(10, -float64(d.Exp)))
}

// Float32 returns the float32 equivalent of the Decimal value.
func (d Decimal) Float32() float32 {
	return float32(d.Float64())
}

// MarshalJSON implements the json.Marshaller interface.
func (d Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Float64())
}

// decodeBase128Varint decodes an unbounded Base128 varint
// from r, returning a big.Int or an error.
func decodeBase128Varint(r io.ByteReader) (*big.Int, error) {
	var b []byte
	for {
		byt, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		b = append(b, byt)
		// Check whether the Base128 varint continues
		// into the next byte. If not, then break.
		if byt&0x80 == 0 {
			break
		}
	}
	bi := &big.Int{}
	_, err := asn1.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
