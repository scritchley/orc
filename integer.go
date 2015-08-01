package orc

import (
	"fmt"
	"io"
)

var (
	ORCErrorEOFUnsignedVInt = fmt.Errorf("EOF while reading unsigned vint")
	ORCErrorCorrupt         = fmt.Errorf("ORC file is corrupt")
)

const (
	MinRepeatSize = 3
)

//go:generate stringer -type=RLEEncodingType
type RLEEncodingType int

const (
	RLEV2IntShortRepeat RLEEncodingType = iota
	RLEV2IntDirect
	RLEV2IntPatchedBase
	RLEV2IntDelta
)

type IntStreamReader struct {
	r      io.ByteReader
	buffer []int64
	err    error
	signed bool
}

func NewIntStreamReader(r io.ByteReader, signed bool) *IntStreamReader {
	return &IntStreamReader{
		r:      r,
		signed: signed,
	}
}

func (i *IntStreamReader) Next() bool {
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

func (i *IntStreamReader) Error() error {
	return i.err
}

func (i *IntStreamReader) Int() (int64, bool) {
	l := len(i.buffer)
	if l != 0 {
		val := i.buffer[0]
		if l == 1 {
			i.buffer = []int64{}
		} else {
			i.buffer = i.buffer[1:]
		}
		return val, true
	}
	// This should trigger a panic
	return 0, false
}

func (i *IntStreamReader) Value() interface{} {
	v, ok := i.Int()
	if !ok {
		return nil
	}
	return v
}

// zigzagEncode encodes a signed integer using zig-zag encoding returning an unsigned integer
func zigzagEncode(i int) uint {
	return uint((i << 1) ^ (i >> 31))
}

// zigzagDecode decodes an unsigned zig-zag encoded integer into a signed integer
func zigzagDecode(i uint) int {
	return int((i >> 1) ^ (-(i & 1)))
}

func readIntValues(r io.ByteReader, signed bool) ([]int64, error) {
	// Header byte
	b0, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	// Get the encoding type
	enc := RLEEncodingType((uint(b0) >> 6) & 0x03)
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
		out = out | int64(uint(val)<<(uint(n)*8))
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
		v = int64(zigzagDecode(uint(v)))
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
	fixedBits := int((uint(b) >> 1) & 0x1f)
	if fixedBits != 0 {
		fixedBits = decodeBitWidth(fixedBits)
	}
	// extract the run-length
	length := int((b & 0x01) << 8)
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
	literals := make([]int64, length+1)
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

	return literals, nil

}

func readIntDirect(b byte, r io.ByteReader, signed bool) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := decodeBitWidth((int(b) >> 1) & 0x1F)
	// extract the run length
	length := int((b & 0x01) << 8)
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
	// write the unpacked values and zigzag decode to result buffer
	readBitPackedInts(literals, numLiterals, length, fixedBits, r)
	if signed {
		for i := 0; i < length; i++ {
			literals[numLiterals] = int64(zigzagDecode(uint(literals[numLiterals])))
			numLiterals++
		}
	} else {
		numLiterals += length
	}
	return literals, nil
}

func readIntPatchedBase(b byte, r io.ByteReader) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := decodeBitWidth((int(b) >> 1) & 0x1F)
	// extract the run length
	length := int((b & 0x01) << 8)
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
	baseWidth := (uint(thirdByte) >> 5) & 0x07
	// base width is one off
	baseWidth += 1
	// extract patch width
	patchWidth := decodeBitWidth(int(thirdByte) & 0x1F)

	// read fourth byte and extract patch gap width
	fourthByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	patchGapWidth := (uint(fourthByte) >> 5) & 0x07
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
	readBitPackedInts(unpacked, 0, length, fixedBits, r)

	// unpack the patch blob
	unpackedPatch := make([]int64, int(patchListLength))
	if (patchWidth + int(patchGapWidth)) > 64 {
		return nil, ORCErrorCorrupt
	}
	bitSize := getClosestFixedBits(patchWidth + int(patchGapWidth))
	readBitPackedInts(unpackedPatch, 0, int(patchListLength), bitSize, r)

	var numLiterals int
	var patchIndex int
	var currentGap int64
	var currentPatch int64

	patchMask := int64((int64(1) << uint(patchWidth)) - 1)

	currentGap = unpackedPatch[patchIndex] >> uint(patchWidth)
	currentPatch = unpackedPatch[patchIndex] & patchMask
	var actualGap int64

	// special case: gap is >255 then patch value will be 0.
	// if gap is <=255 then patch value cannot be 0
	for currentGap == 255 && currentPatch == 0 {
		actualGap += 255
		patchIndex++
		currentGap = int64(unpackedPatch[patchIndex] >> uint(patchWidth))
		currentPatch = unpackedPatch[patchIndex] & patchMask
	}
	// add the left over gap
	actualGap += currentGap

	// unpack data blob, patch it (if required), add base to get final result
	for i := 0; i < len(unpacked); i++ {
		if i == int(actualGap) {
			// extract the patch value
			patchedValue := int64(unpacked[i] | (currentPatch << uint(fixedBits)))

			// add base to patched value
			literals[numLiterals] = base + patchedValue
			numLiterals++

			// increment the patch to point to next entry in patch list
			patchIndex++

			if patchIndex < int(patchListLength) {
				// read the next gap and patch
				currentGap = unpackedPatch[patchIndex] >> uint(patchWidth)
				currentPatch = unpackedPatch[patchIndex] & patchMask
				actualGap = 0
				// special case: gap is >255 then patch will be 0. if gap is
				// <=255 then patch cannot be 0
				for currentGap == 255 && currentPatch == 0 {
					actualGap += 255
					patchIndex++
					currentGap = unpackedPatch[patchIndex] >> uint(patchWidth)
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

type FixedBitSizes int

const (
	FixedBitSizeOne FixedBitSizes = iota
	FixedBitSizeTwo
	FixedBitSizeThree
	FixedBitSizeFour
	FixedBitSizeFive
	FixedBitSizeSix
	FixedBitSizeSeven
	FixedBitSizeEight
	FixedBitSizeNine
	FixedBitSizeTen
	FixedBitSizeEleven
	FixedBitSizeTwelve
	FixedBitSizeThirteen
	FixedBitSizeFourteen
	FixedBitSizeFifteen
	FixedBitSizeSixteen
	FixedBitSizeSeventeen
	FixedBitSizeEighteen
	FixedBitSizeNineteen
	FixedBitSizeTwenty
	FixedBitSizeTwentyOne
	FixedBitSizeTwentyTwo
	FixedBitSizeTwentyThree
	FixedBitSizeTwentyFour
	FixedBitSizeTwentySix
	FixedBitSizeTwentyEight
	FixedBitSizeThirty
	FixedBitSizeThirtyTwo
	FixedBitSizeForty
	FixedBitSizeFortyEight
	FixedBitSizeFiftySix
	FixedBitSizeSixtyFour
)

func decodeBitWidth(n int) int {
	if n >= int(FixedBitSizeOne) && n <= int(FixedBitSizeTwentyFour) {
		return n + 1
	} else if n == int(FixedBitSizeTwentySix) {
		return 26
	} else if n == int(FixedBitSizeTwentyEight) {
		return 28
	} else if n == int(FixedBitSizeThirty) {
		return 30
	} else if n == int(FixedBitSizeThirtyTwo) {
		return 32
	} else if n == int(FixedBitSizeForty) {
		return 40
	} else if n == int(FixedBitSizeFortyEight) {
		return 48
	} else if n == int(FixedBitSizeFiftySix) {
		return 56
	} else {
		return 64
	}
}

func getClosestFixedBits(width int) int {
	if width == 0 {
		return 1
	}
	if width >= 1 && width <= 24 {
		return width
	} else if width > 24 && width <= 26 {
		return 26
	} else if width > 26 && width <= 28 {
		return 28
	} else if width > 28 && width <= 30 {
		return 30
	} else if width > 30 && width <= 32 {
		return 32
	} else if width > 32 && width <= 40 {
		return 40
	} else if width > 40 && width <= 48 {
		return 48
	} else if width > 48 && width <= 56 {
		return 56
	} else {
		return 64
	}
}

// readVint reads a variable width integer from ByteReader r.
func readVInt(signed bool, r io.ByteReader) (int64, error) {
	if signed {
		return readSignedVInt(r)
	}
	return readUnsignedVInt(r)
}

// readerSignedVInt reads a signed variable width integer from ByteReader r.
func readSignedVInt(r io.ByteReader) (int64, error) {
	result, err := readUnsignedVInt(r)
	if err != nil {
		return result, err
	}
	return int64((uint(result) >> uint(1)) ^ -(uint(result) & uint(1))), nil
}

// readerUnsignedVInt reads an unsigned variable width integer from ByteReader r.
func readUnsignedVInt(r io.ByteReader) (int64, error) {
	var result int64
	var offset int
	b := int64(0x80)
	for (b & 0x80) != 0 {
		nb, err := r.ReadByte()
		if err != nil {
			return result, err
		}
		b = int64(nb)
		if b == -1 {
			return result, ORCErrorEOFUnsignedVInt
		}
		result |= (b & 0x7f) << uint(offset)
		offset += 7
	}
	return result, nil
}

func readBitPackedInts(buffer []int64, offset int, length int, bitSize int, r io.ByteReader) error {
	var bitsLeft int
	var current int
	for i := offset; i < (offset + length); i++ {
		var result int64
		bitsLeftToRead := bitSize
		for bitsLeftToRead > bitsLeft {
			result <<= uint(bitsLeft)
			result |= int64(current & ((1 << uint(bitsLeft)) - 1))
			bitsLeftToRead -= bitsLeft
			b, err := r.ReadByte()
			if err != nil {
				return err
			}
			current = int(b)
			bitsLeft = 8
		}
		// handle the left over bits
		if bitsLeftToRead > 0 {
			result <<= uint(bitsLeftToRead)
			bitsLeft -= bitsLeftToRead
			result |= int64((current >> uint(bitsLeft)) & ((1 << uint(bitsLeftToRead)) - 1))
		}
		buffer[i] = result
	}
	return nil
}
