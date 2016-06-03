package orc

import (
	"fmt"
	"io"
)

var (
	ErrEOFUnsignedVInt = fmt.Errorf("EOF while reading unsigned vint")
	ErrCorrupt         = fmt.Errorf("ORC file is corrupt")
)

const (
	// MinRepeatSize is the minimum number of repeated values required to use run length encoding.
	MinRepeatSize = 3
	// MaxShortRepeatLength is the maximum run length used for RLEV2IntShortRepeat sequences.
	MaxShortRepeatLength = 10
	// MaxScope is the maximum number of values that can be buffered before being flushed.
	MaxScope = 512
)

//go:generate stringer -type=RLEEncodingType

// RLEEncodingType is a run length encoding type specified within the Apache
// ORC file documentation: https://orc.apache.org/docs/run-length.html
type RLEEncodingType int

const (
	RLEV2IntShortRepeat RLEEncodingType = 0
	RLEV2IntDirect      RLEEncodingType = 1
	RLEV2IntPatchedBase RLEEncodingType = 2
	RLEV2IntDelta       RLEEncodingType = 3
)

// IntStreamReaderV2 reads an encoded stream of integer values.
type IntStreamReaderV2 struct {
	r      io.ByteReader
	buffer []int64
	err    error
	signed bool
}

// NewIntStreamReaderV2 returns a *IntStreamReaderV2 reading from io.ByteReader r.
// It will returned signed values if signed is true and unsigned if signed is
// false.
func NewIntStreamReaderV2(r io.ByteReader, signed bool) *IntStreamReaderV2 {
	return &IntStreamReaderV2{
		r:      r,
		signed: signed,
	}
}

// Next returns true if there is more data available within the stream.
func (i *IntStreamReaderV2) HasNext() bool {
	if len(i.buffer) != 0 {
		return true
	}
	vals, err := readIntValues(i.r, i.signed)
	if err != nil {
		i.err = err
		return false
	}
	i.buffer = vals
	return true
}

// Error returns the last error to occur whilst reading the underlying stream.
func (i *IntStreamReaderV2) Error() error {
	return i.err
}

// Int returns the next value from the stream as an int64 in addition to
// a bool value denoting whether a value was successfully read.
func (i *IntStreamReaderV2) NextInt() int64 {
	l := len(i.buffer)
	if l != 0 {
		val := i.buffer[0]
		if l == 1 {
			i.buffer = []int64{}
		} else {
			i.buffer = i.buffer[1:]
		}
		return val
	}
	return 0
}

// Value satisfies the StreamReader interface. It returns a int64 as an
// interface value or nil if no value is available.
func (i *IntStreamReaderV2) Next() interface{} {
	return i.NextInt()
}

// zigzagEncode encodes a signed integer using zig-zag encoding returning
// an unsigned integer.
func zigzagEncode(i int64) uint64 {
	return uint64((i << 1) ^ (i >> 31))
}

// zigzagDecode decodes an unsigned zig-zag encoded integer into a signed
// integer.
func zigzagDecode(i uint64) int64 {
	return int64((i >> 1) ^ (-(i & 1)))
}

// readIntValues reads a run length encoded set of integers using the correct
// encoding method and returns them as a slice of int64 values along with any
// error that occurs.
func readIntValues(r io.ByteReader, signed bool) ([]int64, error) {
	// Header byte
	b0, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	// Get the encoding type
	enc := RLEEncodingType((uint64(b0) >> 6) & 0x03)
	// Use the relevant read method based on encoding
	switch enc {
	case RLEV2IntShortRepeat:
		return readIntShortRepeat(b0, r, signed)
	case RLEV2IntDelta:
		return readIntDelta(b0, r, signed)
	case RLEV2IntDirect:
		return readIntDirect(b0, r, signed)
	case RLEV2IntPatchedBase:
		return readIntPatchedBase(b0, r)
	default:
		return nil, fmt.Errorf("Test failed, unsupported run-length encoding %v", enc.String())
	}
}

// readBigEndian reads an integer in big endian form from r with length n bytes. It returns an
// int or an error if one occurs.
func readBigEndian(r io.ByteReader, n int) (int64, error) {
	var out int64
	for n > 0 {
		n--
		// store it in a int64 and then shift else integer overflow will occur
		val, err := r.ReadByte()
		if err != nil {
			return out, err
		}
		out |= int64(val) << uint64(n*8)
	}
	return out, nil
}

// readIntShortRepeat reads a RLEV2 Short Repeat encoded set of integer values accepting a header
// byte and an io.ByteReader as arguments. It returns a slice of integers and any error that occurs.
func readIntShortRepeat(b byte, r io.ByteReader, signed bool) ([]int64, error) {
	// Width of the value in bytes
	w := ((uint64(b) >> 3) & 0x07) + 1
	// Run-length
	rl := b & 0x07
	// Run-lengths are stored only after MinRepeat is met
	rl += MinRepeatSize
	// Read the value in BigEndian
	v, err := readBigEndian(r, int(w))
	if err != nil {
		return nil, err
	}
	// If signed then uses zig-zag encoding
	if signed {
		v = int64(zigzagDecode(uint64(v)))
	}
	o := make([]int64, rl)
	// Populate the run with the value
	for i := range o {
		o[i] = v
	}
	return o, nil
}

// readIntDelta reads a RLEV2 Delta encoded set of integer values accepting a header byte and
// an io.ByteReader as arguments. It returns a slice of integers and any error that occurs.
func readIntDelta(b byte, r io.ByteReader, signed bool) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := int((uint64(b) >> 1) & 0x1f)
	if fixedBits != 0 {
		fixedBits = decodeBitWidth(fixedBits)
	}
	// extract the run-length
	length := int(uint64(b&0x01) << 8)
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	// Set the run-length (this is actual run-length - 1)
	length |= int(b)
	// read the first value
	firstValue, err := readVInt(signed, r)
	if err != nil {
		return nil, err
	}
	var numLiterals int
	// Make an empty slice for the run
	literals := make([]int64, MaxScope)
	// Add first value to the slice
	literals[numLiterals] = firstValue
	numLiterals++

	// Store the previous value
	var prevVal int64
	// if fixed bits is 0 then all values have fixed delta
	if fixedBits == 0 {
		fixedDelta, err := readSignedVInt(r)
		if err != nil {
			return nil, err
		}
		for i := 0; i < length; i++ {
			literals[numLiterals] = literals[numLiterals-1] + fixedDelta
			numLiterals++
		}
	} else {
		deltaBase, err := readSignedVInt(r)
		if err != nil {
			return nil, err
		}
		// add delta base and first value
		literals[numLiterals] = firstValue + deltaBase
		numLiterals++
		prevVal = literals[numLiterals-1]
		length -= 1
		// write the unpacked values, add it to previous value and store final
		// value to result buffer. if the delta base value is negative then it
		// is a decreasing sequence else an increasing sequence
		err = readBitPackedInts(literals, numLiterals, length, fixedBits, r)
		if err != nil {
			return literals, err
		}
		for length > 0 {
			if deltaBase < 0 {
				literals[numLiterals] = prevVal - literals[numLiterals]
			} else {
				literals[numLiterals] = prevVal + literals[numLiterals]
			}
			prevVal = literals[numLiterals]
			length--
			numLiterals++
		}
	}
	if length < numLiterals {
		length = numLiterals
	}
	return literals[:length], nil
}

// readIntDirect reads a direct encoded set of integer values from ByteReader r and returns
// them as a slice of int64 values along with any erorr that occurs.
func readIntDirect(b byte, r io.ByteReader, signed bool) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := int((uint64(b) >> 1) & 0x1f)
	if fixedBits != 0 {
		fixedBits = decodeBitWidth(fixedBits)
	}
	// extract the run length
	length := int(uint64(b&0x01) << 8)
	// read a byte
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	length |= int(b)
	// runs are one off
	length += 1
	// Make a slice of ints
	literals := make([]int64, length)
	var numLiterals int
	// read the unpacked values and zigzag decode to result buffer
	readBitPackedInts(literals, numLiterals, length, fixedBits, r)
	if signed {
		for i := 0; i < length; i++ {
			literals[numLiterals] = int64(zigzagDecode(uint64(literals[numLiterals])))
			numLiterals++
		}
	} else {
		numLiterals += length
	}
	return literals, nil
}

// readIntPatchedBase reads a patched base encoded set of integer values from ByteReader
// r and returns them as a slice of int64 values along with any error that occurs.
func readIntPatchedBase(firstByte byte, r io.ByteReader) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := int((uint64(firstByte) >> 1) & 0x1f)
	if fixedBits != 0 {
		fixedBits = decodeBitWidth(fixedBits)
	}
	// extract the run length
	length := int(uint64(firstByte&0x01) << 8)
	// read a byte
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	length |= int(b)
	// runs are one off
	length += 1
	// Slice to store the values
	literals := make([]int64, length)
	// extract the number of bytes occupied by base
	thirdByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	baseWidth := (uint64(thirdByte) >> 5) & 0x07
	// base width is one off
	baseWidth += 1
	// extract patch width
	patchWidth := decodeBitWidth(int(thirdByte) & 0x1F)

	// read fourth byte and extract patch gap width
	fourthByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	patchGapWidth := (uint64(fourthByte) >> 5) & 0x07
	// patch gap width is one off
	patchGapWidth += 1
	// extract the length of the patch list
	patchListLength := fourthByte&0x1F + 1
	// read the next base width number of bytes to extract base value
	base, err := readBigEndian(r, int(baseWidth))
	if err != nil {
		return nil, err
	}
	mask := (int64(1) << ((baseWidth * 8) - 1))
	// if MSB of base value is 1 then base is negative value else positive
	if (base & mask) != 0 {
		base = base & ^mask
		base = -base
	}
	// unpack the data blob
	unpacked := make([]int64, length)
	readInts(unpacked, 0, length, fixedBits, r)

	// unpack the patch blob
	unpackedPatch := make([]int64, int(patchListLength))
	if (patchWidth + int(patchGapWidth)) > 64 {
		return nil, ErrCorrupt
	}
	bitSize := getClosestFixedBits(patchWidth + int(patchGapWidth))
	readInts(unpackedPatch, 0, int(patchListLength), bitSize, r)

	var numLiterals int
	var patchIndex int
	var currentGap int64
	var currentPatch int64

	patchMask := int64((int64(1) << uint64(patchWidth)) - 1)

	currentGap = unpackedPatch[patchIndex] >> uint64(patchWidth)
	currentPatch = unpackedPatch[patchIndex] & patchMask
	var actualGap int64

	// special case: gap is >255 then patch value will be 0.
	// if gap is <=255 then patch value cannot be 0
	for currentGap == 255 && currentPatch == 0 {
		actualGap += 255
		patchIndex++
		currentGap = int64(unpackedPatch[patchIndex] >> uint64(patchWidth))
		currentPatch = unpackedPatch[patchIndex] & patchMask
	}
	// add the left over gap
	actualGap += currentGap

	// unpack data blob, patch it (if required), add base to get final result
	for i := 0; i < len(unpacked); i++ {
		if i == int(actualGap) {
			// extract the patch value
			patchedValue := int64(unpacked[i] | (currentPatch << uint64(fixedBits)))

			// add base to patched value
			literals[numLiterals] = base + patchedValue
			numLiterals++

			// increment the patch to point to next entry in patch list
			patchIndex++

			if patchIndex < int(patchListLength) {
				// read the next gap and patch
				currentGap = unpackedPatch[patchIndex] >> uint64(patchWidth)
				currentPatch = unpackedPatch[patchIndex] & patchMask
				actualGap = 0
				// special case: gap is >255 then patch will be 0. if gap is
				// <=255 then patch cannot be 0
				for currentGap == 255 && currentPatch == 0 {
					actualGap += 255
					patchIndex++
					currentGap = unpackedPatch[patchIndex] >> uint64(patchWidth)
					currentPatch = unpackedPatch[patchIndex] & patchMask
				}
				// add the left over gap
				actualGap += currentGap
				// next gap is relative to the current gap
				actualGap += int64(i)
			}
		} else {
			// no patching required. add base to unpacked value to get final value
			literals[numLiterals] = base + unpacked[i]
			numLiterals++
		}
	}

	return literals, nil

}
