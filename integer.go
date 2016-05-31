package orc

import (
	"fmt"
	"io"
	"math"
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
		out |= int64(val) << uint(n*8)
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
	fixedBits := int((uint(b) >> 1) & 0x1f)
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
	literals := make([]int64, 256)
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
	fixedBits := decodeBitWidth((int(b) >> 1) & 0x1F)
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
	// write the unpacked values and zigzag decode to result buffer
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
func readIntPatchedBase(b byte, r io.ByteReader) ([]int64, error) {
	// extract the number of fixed bits
	fixedBits := decodeBitWidth((int(b) >> 1) & 0x1F)
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
		return nil, ErrCorrupt
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

func encodeBitWidth(n int) int {
	if n >= 1 && n <= 24 {
		return n - 1
	} else if n > 24 && n <= 26 {
		return int(FixedBitSizeTwentySix)
	} else if n > 26 && n <= 28 {
		return int(FixedBitSizeTwentyEight)
	} else if n > 28 && n <= 30 {
		return int(FixedBitSizeThirty)
	} else if n > 30 && n <= 32 {
		return int(FixedBitSizeThirtyTwo)
	} else if n > 32 && n <= 40 {
		return int(FixedBitSizeForty)
	} else if n > 40 && n <= 48 {
		return int(FixedBitSizeFortyEight)
	} else if n > 48 && n <= 56 {
		return int(FixedBitSizeFiftySix)
	} else {
		return int(FixedBitSizeSixtyFour)
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
			return result, ErrEOFUnsignedVInt
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

type IntStreamWriterV2 struct {
	w                 io.ByteWriter
	signed            bool
	alignedBitpacking bool
	numLiterals       int
	literals          []int64
	encoding          RLEEncodingType
	prevDelta         int64
	fixedDelta        int64
	zzBits90p         int
	zzBits100p        int
	brBits95p         int
	brBits100p        int
	bitsDeltaMax      int
	patchGapWidth     int
	patchLength       int
	patchWidth        int
	gapVsPatchList    []int64
	isFixedDelta      bool
	variableRunLength int
	fixedRunLength    int
	zigzagLiterals    []int64
	baseRedLiterals   []int64
	adjDeltas         []int64
	min               int64
}

func NewIntStreamWriterV2(w io.ByteWriter, signed bool) *IntStreamWriterV2 {
	i := &IntStreamWriterV2{
		w:                 w,
		signed:            signed,
		literals:          make([]int64, 512, 512),
		zigzagLiterals:    make([]int64, 512, 512),
		baseRedLiterals:   make([]int64, 512, 512),
		adjDeltas:         make([]int64, 512, 512),
		alignedBitpacking: true,
	}
	i.clear()
	return i
}

func (i *IntStreamWriterV2) Flush() error {
	if i.numLiterals != 0 {
		if i.variableRunLength != 0 {
			err := i.determineEncoding()
			if err != nil {
				return err
			}
			return i.writeValues()
		} else if i.fixedRunLength != 0 {
			if i.fixedRunLength < MinRepeatSize {
				i.variableRunLength = i.fixedRunLength
				i.fixedRunLength = 0
				err := i.determineEncoding()
				if err != nil {
					return err
				}
				return i.writeValues()
			} else if i.fixedRunLength >= MinRepeatSize &&
				i.fixedRunLength <= MaxShortRepeatLength {
				i.encoding = RLEV2IntShortRepeat
				return i.writeValues()
			} else {
				i.encoding = RLEV2IntDelta
				i.isFixedDelta = true
				return i.writeValues()
			}
		}
	}
	return nil
}

func (i *IntStreamWriterV2) WriteInt(val int64) error {
	if i.numLiterals == 0 {
		i.initializeLiterals(val)
	} else {
		if i.numLiterals == 1 {
			i.prevDelta = val - i.literals[0]
			i.literals[i.numLiterals] = val
			i.numLiterals++
			// if both values are same count as fixed run else variable run
			if val == i.literals[0] {
				i.fixedRunLength = 2
				i.variableRunLength = 0
			} else {
				i.fixedRunLength = 0
				i.variableRunLength = 2
			}
		} else {
			currentDelta := val - i.literals[i.numLiterals-1]
			if i.prevDelta == 0 && currentDelta == 0 {
				// fixed delta run
				i.literals[i.numLiterals] = val
				i.numLiterals++

				// if variable run is non-zero then we are seeing repeating
				// values at the end of variable run in which case keep
				// updating variable and fixed runs
				if i.variableRunLength > 0 {
					i.fixedRunLength = 2
				}
				i.fixedRunLength += 1

				// if fixed run met the minimum condition and if variable
				// run is non-zero then flush the variable run and shift the
				// tail fixed runs to start of the buffer
				if i.fixedRunLength >= MinRepeatSize && i.variableRunLength > 0 {
					i.numLiterals -= MinRepeatSize
					i.variableRunLength -= MinRepeatSize - 1
					// copy the tail fixed runs
					tailVals := make([]int64, MinRepeatSize)
					copy(tailVals, i.literals[i.numLiterals:i.numLiterals+MinRepeatSize])
					// determine variable encoding and flush values
					err := i.determineEncoding()
					if err != nil {
						return err
					}
					err = i.writeValues()
					if err != nil {
						return err
					}
					// shift tail fixed runs to beginning of the buffer
					for _, l := range tailVals {
						i.literals[i.numLiterals] = l
						i.numLiterals++
					}
				}

				// if fixed runs reached max repeat length then write values
				if i.fixedRunLength == 512 {
					err := i.determineEncoding()
					if err != nil {
						return err
					}
					err = i.writeValues()
					if err != nil {
						return err
					}
				}
			} else {
				// variable delta run

				// if fixed run length is non-zero and if it satisfies the
				// short repeat conditions then write the values as short repeats
				// else use delta encoding
				if i.fixedRunLength >= MinRepeatSize {
					if i.fixedRunLength <= MaxShortRepeatLength {
						i.encoding = RLEV2IntShortRepeat
						err := i.writeValues()
						if err != nil {
							return err
						}
					} else {
						i.encoding = RLEV2IntDelta
						i.isFixedDelta = true
						err := i.writeValues()
						if err != nil {
							return err
						}
					}
				}

				// if fixed run length is <MIN_REPEAT and current value is
				// different from previous then treat it as variable run
				if i.fixedRunLength > 0 && i.fixedRunLength < MinRepeatSize {
					if val != i.literals[i.numLiterals-1] {
						i.variableRunLength = i.fixedRunLength
						i.fixedRunLength = 0
					}
				}

				// after writing values re-initialize the variables
				if i.numLiterals == 0 {
					i.initializeLiterals(val)
				} else {
					// keep updating variable run lengths
					i.prevDelta = val - i.literals[i.numLiterals-1]
					i.literals[i.numLiterals] = val
					i.numLiterals++
					i.variableRunLength += 1

					// if variable run length reach the max scope, write it
					if i.variableRunLength == 512 {
						err := i.determineEncoding()
						if err != nil {
							return err
						}
						err = i.writeValues()
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func (i *IntStreamWriterV2) writeValues() error {
	if i.numLiterals != 0 {
		switch i.encoding {
		case RLEV2IntShortRepeat:
			err := i.writeShortRepeatValues()
			if err != nil {
				return err
			}
		case RLEV2IntDirect:
			err := i.writeDirectValues()
			if err != nil {
				return err
			}
		case RLEV2IntPatchedBase:
			err := i.writePatchedBaseValues()
			if err != nil {
				return err
			}
		default:
			err := i.writeDeltaValues()
			if err != nil {
				return err
			}
		}
		i.clear()
	}
	return nil
}

func (i *IntStreamWriterV2) Close() error {
	return i.Flush()
}

func (i *IntStreamWriterV2) clear() {
	i.numLiterals = 0
	i.encoding = RLEV2IntDirect
	i.prevDelta = 0
	i.fixedDelta = 0
	i.zzBits90p = 0
	i.zzBits100p = 0
	i.brBits95p = 0
	i.brBits100p = 0
	i.bitsDeltaMax = 0
	i.patchGapWidth = 0
	i.patchLength = 0
	i.patchWidth = 0
	i.gapVsPatchList = []int64{}
	i.min = 0
	i.isFixedDelta = true
}

func (i *IntStreamWriterV2) determineEncoding() error {

	// we need to compute zigzag values for DIRECT encoding if we decide to
	// break early for delta overflows or for shorter runs
	i.computeZigZagLiterals()

	i.zzBits100p = percentileBits(i.zigzagLiterals, 0, i.numLiterals, 1.0)

	// not a big win for shorter runs to determine encoding
	if i.numLiterals <= MinRepeatSize {
		i.encoding = RLEV2IntDirect
		return nil
	}

	// Delta encoding check

	// for identifying monotonic sequences
	isIncreasing := true
	isDecreasing := true
	i.isFixedDelta = true

	i.min = i.literals[0]
	max := i.literals[0]
	initialDelta := i.literals[1] - i.literals[0]
	currDelta := initialDelta
	deltaMax := initialDelta
	i.adjDeltas[0] = initialDelta

	for j := 1; j < i.numLiterals; j++ {
		l1 := i.literals[j]
		l0 := i.literals[j-1]
		currDelta = l1 - l0
		i.min = minInt64(i.min, l1)
		max = maxInt64(max, l1)

		isIncreasing = isIncreasing && (l0 <= l1)
		isDecreasing = isDecreasing && (l0 >= l1)

		i.isFixedDelta = i.isFixedDelta && (currDelta == initialDelta)
		if j > 1 {
			i.adjDeltas[j-1] = absInt64(currDelta)
			deltaMax = maxInt64(deltaMax, i.adjDeltas[j-1])
		}
	}

	// its faster to exit under delta overflow condition without checking for
	// PATCHED_BASE condition as encoding using DIRECT is faster and has less
	// overhead than PATCHED_BASE
	if !isSafeSubtract(max, i.min) {
		i.encoding = RLEV2IntDirect
		return nil
	}

	// invariant - subtracting any number from any other in the literals after
	// this point won't overflow

	// if min is equal to max then the delta is 0, this condition happens for
	// fixed values run >10 which cannot be encoded with SHORT_REPEAT
	if i.min == max {
		if !i.isFixedDelta {
			return fmt.Errorf("%v == %v, isFixedDelta cannot be false", i.min, max)
		}
		if currDelta != 0 {
			return fmt.Errorf("%v == %v, currDelta should be zero", i.min, max)
		}
		i.fixedDelta = 0
		i.encoding = RLEV2IntDelta
		return nil
	}

	if i.isFixedDelta {
		if currDelta != initialDelta {
			return fmt.Errorf("currDelta should be equal to initialDelta for fixed delta encoding")
		}
		i.encoding = RLEV2IntDelta
		i.fixedDelta = currDelta
		return nil
	}

	// if initialDelta is 0 then we cannot delta encode as we cannot identify
	// the sign of deltas (increasing or decreasing)
	if initialDelta != 0 {
		// stores the number of bits required for packing delta blob in
		// delta encoding
		i.bitsDeltaMax = findClosestNumBits(deltaMax)

		// monotonic condition
		if isIncreasing || isDecreasing {
			i.encoding = RLEV2IntDelta
			return nil
		}
	}

	// PATCHED_BASE encoding check

	// percentile values are computed for the zigzag encoded values. if the
	// number of bit requirement between 90th and 100th percentile varies
	// beyond a threshold then we need to patch the values. if the variation
	// is not significant then we can use direct encoding
	i.zzBits90p = percentileBits(i.zigzagLiterals, 0, i.numLiterals, 0.9)
	diffBitsLH := i.zzBits100p - i.zzBits90p

	// if the difference between 90th percentile and 100th percentile fixed
	// bits is > 1 then we need patch the values
	if diffBitsLH > 1 {

		// patching is done only on base reduced values.
		// remove base from literals
		for j := 0; j < i.numLiterals; j++ {
			i.baseRedLiterals[j] = i.literals[j] - i.min
		}

		// 95th percentile width is used to determine max allowed value
		// after which patching will be done
		i.brBits95p = percentileBits(i.baseRedLiterals, 0, i.numLiterals, 0.95)

		// 100th percentile is used to compute the max patch width
		i.brBits100p = percentileBits(i.baseRedLiterals, 0, i.numLiterals, 1.0)

		// after base reducing the values, if the difference in bits between
		// 95th percentile and 100th percentile value is zero then there
		// is no point in patching the values, in which case we will
		// fallback to DIRECT encoding.
		// The decision to use patched base was based on zigzag values, but the
		// actual patching is done on base reduced literals.
		if (i.brBits100p - i.brBits95p) != 0 {
			i.encoding = RLEV2IntPatchedBase
			i.preparePatchedBlob()
			return nil
		} else {
			i.encoding = RLEV2IntDirect
			return nil
		}
	} else {
		// if difference in bits between 95th percentile and 100th percentile is
		// 0, then patch length will become 0. Hence we will fallback to direct
		i.encoding = RLEV2IntDirect
		return nil
	}
}

func (i *IntStreamWriterV2) computeZigZagLiterals() {
	// populate zigzag encoded literals
	zzEncVal := int64(0)
	for j := 0; j < i.numLiterals; j++ {
		if i.signed {
			zzEncVal = int64(zigzagEncode(i.literals[j]))
		} else {
			zzEncVal = i.literals[j]
		}
		i.zigzagLiterals[j] = zzEncVal
	}
}

func (i *IntStreamWriterV2) preparePatchedBlob() {
	// mask will be max value beyond which patch will be generated
	mask := (1 << uint(i.brBits95p)) - 1

	// since we are considering only 95 percentile, the size of gap and
	// patch array can contain only be 5% values
	i.patchLength = int(math.Ceil(float64(i.numLiterals) * 0.05))

	gapList := make([]int, i.patchLength, i.patchLength)
	patchList := make([]int64, i.patchLength, i.patchLength)

	// #bit for patch
	i.patchWidth = i.brBits100p - i.brBits95p
	i.patchWidth = getClosestFixedBits(i.patchWidth)

	// if patch bit requirement is 64 then it will not possible to pack
	// gap and patch together in a long. To make sure gap and patch can be
	// packed together adjust the patch width
	if i.patchWidth == 64 {
		i.patchWidth = 56
		i.brBits95p = 8
		mask = (1 << uint(i.brBits95p)) - 1
	}

	gapIdx := 0
	patchIdx := 0
	prev := 0
	gap := 0
	maxGap := 0

	for j := 0; j < i.numLiterals; j++ {
		// if value is above mask then create the patch and record the gap
		if i.baseRedLiterals[j] > int64(mask) {
			gap = j - prev
			if gap > maxGap {
				maxGap = gap
			}

			// gaps are relative, so store the previous patched value index
			prev = j
			gapList[gapIdx] = gap
			gapIdx++

			// extract the most significant bits that are over mask bits
			patch := int64(uint(i.baseRedLiterals[j]) >> uint(i.brBits95p))
			patchList[patchIdx] = patch
			patchIdx++

			// strip off the MSB to enable safe bit packing
			i.baseRedLiterals[j] &= int64(mask)
		}
	}

	// adjust the patch length to number of entries in gap list
	i.patchLength = gapIdx

	// if the element to be patched is the first and only element then
	// max gap will be 0, but to store the gap as 0 we need atleast 1 bit
	if maxGap == 0 && i.patchLength != 0 {
		i.patchGapWidth = 1
	} else {
		i.patchGapWidth = findClosestNumBits(int64(maxGap))
	}

	// special case: if the patch gap width is greater than 256, then
	// we need 9 bits to encode the gap width. But we only have 3 bits in
	// header to record the gap width. To deal with this case, we will save
	// two entries in patch list in the following way
	// 256 gap width => 0 for patch value
	// actual gap - 256 => actual patch value
	// We will do the same for gap width = 511. If the element to be patched is
	// the last element in the scope then gap width will be 511. In this case we
	// will have 3 entries in the patch list in the following way
	// 255 gap width => 0 for patch value
	// 255 gap width => 0 for patch value
	// 1 gap width => actual patch value
	if i.patchGapWidth > 8 {
		i.patchGapWidth = 8
		// for gap = 511, we need two additional entries in patch list
		if maxGap == 511 {
			i.patchLength += 2
		} else {
			i.patchLength += 1
		}
	}

	// create gap vs patch list
	gapIdx = 0
	patchIdx = 0
	i.gapVsPatchList = make([]int64, i.patchLength, i.patchLength)
	for j := 0; j < i.patchLength; j++ {
		g := gapList[gapIdx]
		gapIdx++
		p := patchList[patchIdx]
		patchIdx++
		for g > 255 {
			i.gapVsPatchList[j] = (255 << uint(i.patchWidth))
			j++
			g -= 255
		}

		// store patch value in LSBs and gap in MSBs
		i.gapVsPatchList[j] = int64(g<<uint(i.patchWidth)) | p
	}

}

func (i *IntStreamWriterV2) initializeLiterals(val int64) {
	i.literals[i.numLiterals] = val
	i.numLiterals++
	i.fixedRunLength = 1
	i.variableRunLength = 1
}

func (i *IntStreamWriterV2) writeShortRepeatValues() error {
	var repeatVal int64
	if i.signed {
		repeatVal = int64(zigzagEncode(i.literals[0]))
	} else {
		repeatVal = i.literals[0]
	}
	numBitsRepeatVal := findClosestNumBits(repeatVal)
	var numBytesRepeatVal int
	if numBitsRepeatVal%8 == 0 {
		numBytesRepeatVal = int(uint(numBitsRepeatVal) >> 3)
	} else {
		numBytesRepeatVal = int(uint(numBitsRepeatVal)>>3) + 1
	}

	header := i.getOpCode()
	header |= (numBytesRepeatVal - 1) << 3

	i.fixedRunLength -= MinRepeatSize
	header |= i.fixedRunLength

	err := i.w.WriteByte(uint8(header))
	if err != nil {
		return err
	}

	for j := numBytesRepeatVal - 1; j >= 0; j-- {
		b := uint8((uint(repeatVal) >> uint(j*8)) & 0xff)
		err := i.w.WriteByte(b)
		if err != nil {
			return err
		}
	}

	i.fixedRunLength = 0

	return nil
}

func (i *IntStreamWriterV2) getOpCode() int {
	return int(i.encoding << 6)
}

func (i *IntStreamWriterV2) writeDirectValues() error {

	fb := i.zzBits100p

	if i.alignedBitpacking {
		fb = getClosestFixedBits(fb)
	}

	efb := encodeBitWidth(fb) << 1

	i.variableRunLength -= 1

	tailBits := int(uint(i.variableRunLength&0x100) >> 8)

	headerFirstByte := i.getOpCode() | efb | tailBits

	headerSecondByte := i.variableRunLength & 0xff

	err := i.w.WriteByte(uint8(headerFirstByte))
	if err != nil {
		return err
	}

	err = i.w.WriteByte(uint8(headerSecondByte))
	if err != nil {
		return err
	}

	err = writeInts(i.zigzagLiterals, 0, i.numLiterals, fb, i.w)
	if err != nil {
		return err
	}

	i.variableRunLength = 0

	return nil

}

func (i *IntStreamWriterV2) writePatchedBaseValues() error {

	// NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
	// because patch is applied to MSB bits. For example: If fixed bit width of
	// base value is 7 bits and if patch is 3 bits, the actual value is
	// constructed by shifting the patch to left by 7 positions.
	// actual_value = patch << 7 | base_value
	// So, if we align base_value then actual_value can not be reconstructed.

	fb := i.brBits95p
	efb := encodeBitWidth(fb) << 1

	i.variableRunLength -= 1

	tailBits := int(uint(i.variableRunLength&0x100) >> 8)

	headerFirstByte := i.getOpCode() | efb | tailBits

	headerSecondByte := i.variableRunLength & 0xff

	var isNegative bool
	if i.min < 0 {
		isNegative = true
	}
	if isNegative {
		i.min -= -i.min
	}

	baseWidth := findClosestNumBits(i.min) + 1
	var baseBytes int
	if baseWidth%8 == 0 {
		baseBytes = baseWidth / 8
	} else {
		baseBytes = (baseWidth / 8) + 1
	}
	bb := (baseBytes - 1) << 5

	if isNegative {
		i.min |= (1 << uint((baseBytes*8)-1))
	}

	headerThirdByte := bb | encodeBitWidth(i.patchWidth)

	headerFourthByte := (i.patchGapWidth-1)<<5 | i.patchLength

	err := i.w.WriteByte(uint8(headerFirstByte))
	if err != nil {
		return err
	}

	err = i.w.WriteByte(uint8(headerSecondByte))
	if err != nil {
		return err
	}

	err = i.w.WriteByte(uint8(headerThirdByte))
	if err != nil {
		return err
	}

	err = i.w.WriteByte(uint8(headerFourthByte))
	if err != nil {
		return err
	}

	for j := baseBytes - 1; j >= 0; j-- {
		b := byte((uint(i.min) >> uint(j*8)) & 0xff)
		err = i.w.WriteByte(b)
		if err != nil {
			return err
		}
	}

	closestFixedBits := getClosestFixedBits(fb)

	err = writeInts(i.baseRedLiterals, 0, i.numLiterals, closestFixedBits, i.w)
	if err != nil {
		return err
	}

	closestFixedBits = getClosestFixedBits(i.patchGapWidth + i.patchWidth)

	err = writeInts(i.gapVsPatchList, 0, len(i.gapVsPatchList), closestFixedBits, i.w)
	if err != nil {
		return err
	}

	i.variableRunLength = 0

	return nil
}

func (i *IntStreamWriterV2) writeDeltaValues() error {
	len := 0
	fb := i.bitsDeltaMax
	efb := 0

	if i.alignedBitpacking {
		fb = getClosestFixedBits(fb)
	}

	if i.isFixedDelta {
		// if fixed run length is greater than threshold then it will be fixed
		// delta sequence with delta value 0 else fixed delta sequence with
		// non-zero delta value
		if i.fixedRunLength > MinRepeatSize {
			// ex. sequence: 2 2 2 2 2 2 2 2
			len = i.fixedRunLength - 1
			i.fixedRunLength = 0
		} else {
			// ex. sequence: 4 6 8 10 12 14 16
			len = i.variableRunLength - 1
			i.fixedRunLength = 0
		}
	} else {
		// fixed width 0 is used for long repeating values.
		// sequences that require only 1 bit to encode will have an additional bit
		if fb == 1 {
			fb = 2
		}
		efb = encodeBitWidth(fb)
		efb = efb << 1
		len = i.variableRunLength - 1
		i.variableRunLength = 0
	}

	tailBits := int(uint(len&0x100) >> 8)

	headerFirstByte := i.getOpCode() | efb | tailBits

	headerSecondByte := len & 0xff

	err := i.w.WriteByte(uint8(headerFirstByte))
	if err != nil {
		return err
	}

	err = i.w.WriteByte(uint8(headerSecondByte))
	if err != nil {
		return err
	}

	if i.signed {
		err := writeVsint(i.w, i.literals[0])
		if err != nil {
			return err
		}
	} else {
		err := writeVuint(i.w, i.literals[0])
		if err != nil {
			return err
		}
	}

	if i.isFixedDelta {
		// if delta is fixed then we don't need to store delta blob
		err := writeVsint(i.w, i.fixedDelta)
		if err != nil {
			return err
		}
	} else {
		// store the first value as delta value using zigzag encoding
		err := writeVsint(i.w, i.adjDeltas[0])
		if err != nil {
			return err
		}

		err = writeInts(i.adjDeltas, 1, i.numLiterals-2, fb, i.w)
		if err != nil {
			return err
		}
	}

	return nil
}

func absInt64(a int64) int64 {
	if a > 0 {
		return a
	}
	return -a
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func isSafeSubtract(left, right int64) bool {
	return (left^right) >= 0 || (left^(left-right)) >= 0
}

func percentileBits(data []int64, offset int, length int, p float64) int {
	if (p > 1.0) || (p <= 0.0) {
		return -1
	}

	// histogram that store the encoded bit requirement for each values.
	// maximum number of bits that can encoded is 32 (refer FixedBitSizes)
	hist := make([]int, 32, 32)

	// compute the histogram
	for i := offset; i < (offset + length); i++ {
		idx := encodeBitWidth(findClosestNumBits(data[i]))
		hist[idx] += 1
	}

	perLen := length * int(1.0-p)

	// return the bits required by pth percentile length
	for i := len(hist) - 1; i >= 0; i-- {
		perLen -= hist[i]
		if perLen < 0 {
			return decodeBitWidth(i)
		}
	}

	return 0

}

func findClosestNumBits(value int64) int {
	var count int
	for value != 0 {
		count++
		value = int64(uint(value) >> 1)
	}
	return getClosestFixedBits(count)
}

func writeInts(input []int64, offset int, l int, bitSize int, w io.ByteWriter) error {

	if input == nil || len(input) < 1 || offset < 0 || l < 1 || bitSize < 1 {
		return nil
	}

	switch bitSize {
	case 1:
		return unrolledBitPack1(input, offset, l, w)
	case 2:
		return unrolledBitPack2(input, offset, l, w)
	case 4:
		return unrolledBitPack4(input, offset, l, w)
	case 8:
		return unrolledBitPack8(input, offset, l, w)
	case 16:
		return unrolledBitPack16(input, offset, l, w)
	case 24:
		return unrolledBitPack24(input, offset, l, w)
	case 32:
		return unrolledBitPack32(input, offset, l, w)
	case 40:
		return unrolledBitPack40(input, offset, l, w)
	case 48:
		return unrolledBitPack48(input, offset, l, w)
	case 56:
		return unrolledBitPack56(input, offset, l, w)
	case 64:
		return unrolledBitPack64(input, offset, l, w)
	}

	bitsLeft := 8
	var current byte
	for i := offset; i < (offset + l); i++ {
		value := input[i]
		bitsToWrite := bitSize
		for bitsToWrite > bitsLeft {
			// add the bits to the bottom of the current word
			current |= uint8(uint(value) >> uint(bitsToWrite-bitsLeft))
			// subtract out the bits we just added
			bitsToWrite -= bitsLeft
			// zero out the bits above bitsToWrite
			value &= (1 << uint(bitsToWrite)) - 1
			err := w.WriteByte(current)
			if err != nil {
				return err
			}
			current = 0
			bitsLeft = 8
		}
		bitsLeft -= bitsToWrite
		current |= uint8(value << uint(bitsLeft))
		if bitsLeft == 0 {
			err := w.WriteByte(current)
			if err != nil {
				return err
			}
			current = 0
			bitsLeft = 8
		}
	}

	// flush
	if bitsLeft != 8 {
		err := w.WriteByte(current)
		if err != nil {
			return err
		}
		current = 0
		bitsLeft = 8
	}

	return nil
}

func unrolledBitPack1(input []int64, offset int, len int, w io.ByteWriter) error {
	numHops := 8
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := 0
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | (int(input[i]&1) << 7) |
			(int(input[i+1]&1) << 6) |
			(int(input[i+2]&1) << 5) |
			(int(input[i+3]&1) << 4) |
			(int(input[i+4]&1) << 3) |
			(int(input[i+5]&1) << 2) |
			(int(input[i+6]&1) << 1) |
			int(input[i+7])&1)
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
		val = 0
	}

	if remainder > 0 {
		startShift := 7
		for i := endUnroll; i < endOffset; i++ {
			val = (val | int(input[i]&1)<<uint(startShift))
			startShift -= 1
		}
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
	}
	return nil
}

func unrolledBitPack2(input []int64, offset int, len int, w io.ByteWriter) error {
	numHops := 4
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := 0
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | (int(input[i]&3) << 6) |
			(int(input[i+1]&3) << 4) |
			(int(input[i+2]&3) << 2) |
			int(input[i+3])&3)
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
		val = 0
	}

	if remainder > 0 {
		startShift := 6
		for i := endUnroll; i < endOffset; i++ {
			val = (val | int(input[i]&3)<<uint(startShift))
			startShift -= 2
		}
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
	}
	return nil
}

func unrolledBitPack4(input []int64, offset int, len int, w io.ByteWriter) error {
	numHops := 2
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	val := 0
	for i := offset; i < endUnroll; i = i + numHops {
		val = (val | (int(input[i]&15) << 4) | int(input[i+1])&15)
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
		val = 0
	}

	if remainder > 0 {
		startShift := 4
		for i := endUnroll; i < endOffset; i++ {
			val = (val | int(input[i]&15)<<uint(startShift))
			startShift -= 2
		}
		err := w.WriteByte(byte(val))
		if err != nil {
			return err
		}
	}
	return nil
}

func unrolledBitPack8(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 1)
}

func unrolledBitPack16(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 2)
}

func unrolledBitPack24(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 3)
}

func unrolledBitPack32(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 4)
}

func unrolledBitPack40(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 5)
}

func unrolledBitPack48(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 6)
}

func unrolledBitPack56(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 7)
}

func unrolledBitPack64(input []int64, offset int, len int, w io.ByteWriter) error {
	return unrolledBitPackBytes(input, offset, len, w, 8)
}

func unrolledBitPackBytes(input []int64, offset int, len int, w io.ByteWriter, numBytes int) error {
	numHops := 8
	remainder := len % numHops
	endOffset := offset + len
	endUnroll := endOffset - remainder
	var i int
	for i = offset; i < endUnroll; i = i + numHops {
		err := writeLongBE(w, input, i, numHops, numBytes)
		if err != nil {
			return err
		}
	}

	if remainder > 0 {
		return writeRemainingLongs(w, i, input, remainder, numBytes)
	}
	return nil
}

func writeLongBE(w io.ByteWriter, input []int64, offset int, numHops int, numBytes int) error {
	writeBuffer := make([]byte, 64, 64)
	switch numBytes {
	case 1:
		writeBuffer[0] = byte(input[offset+0] & 255)
		writeBuffer[1] = byte(input[offset+1] & 255)
		writeBuffer[2] = byte(input[offset+2] & 255)
		writeBuffer[3] = byte(input[offset+3] & 255)
		writeBuffer[4] = byte(input[offset+4] & 255)
		writeBuffer[5] = byte(input[offset+5] & 255)
		writeBuffer[6] = byte(input[offset+6] & 255)
		writeBuffer[7] = byte(input[offset+7] & 255)
	case 2:
		writeLongBE2(writeBuffer, input[offset+0], 0)
		writeLongBE2(writeBuffer, input[offset+1], 2)
		writeLongBE2(writeBuffer, input[offset+2], 4)
		writeLongBE2(writeBuffer, input[offset+3], 6)
		writeLongBE2(writeBuffer, input[offset+4], 8)
		writeLongBE2(writeBuffer, input[offset+5], 10)
		writeLongBE2(writeBuffer, input[offset+6], 12)
		writeLongBE2(writeBuffer, input[offset+7], 14)
	case 3:
		writeLongBE3(writeBuffer, input[offset+0], 0)
		writeLongBE3(writeBuffer, input[offset+1], 3)
		writeLongBE3(writeBuffer, input[offset+2], 6)
		writeLongBE3(writeBuffer, input[offset+3], 9)
		writeLongBE3(writeBuffer, input[offset+4], 12)
		writeLongBE3(writeBuffer, input[offset+5], 15)
		writeLongBE3(writeBuffer, input[offset+6], 18)
		writeLongBE3(writeBuffer, input[offset+7], 21)
	case 4:
		writeLongBE4(writeBuffer, input[offset+0], 0)
		writeLongBE4(writeBuffer, input[offset+1], 4)
		writeLongBE4(writeBuffer, input[offset+2], 8)
		writeLongBE4(writeBuffer, input[offset+3], 12)
		writeLongBE4(writeBuffer, input[offset+4], 16)
		writeLongBE4(writeBuffer, input[offset+5], 20)
		writeLongBE4(writeBuffer, input[offset+6], 24)
		writeLongBE4(writeBuffer, input[offset+7], 28)
	case 5:
		writeLongBE5(writeBuffer, input[offset+0], 0)
		writeLongBE5(writeBuffer, input[offset+1], 5)
		writeLongBE5(writeBuffer, input[offset+2], 10)
		writeLongBE5(writeBuffer, input[offset+3], 15)
		writeLongBE5(writeBuffer, input[offset+4], 20)
		writeLongBE5(writeBuffer, input[offset+5], 25)
		writeLongBE5(writeBuffer, input[offset+6], 30)
		writeLongBE5(writeBuffer, input[offset+7], 35)
	case 6:
		writeLongBE6(writeBuffer, input[offset+0], 0)
		writeLongBE6(writeBuffer, input[offset+1], 6)
		writeLongBE6(writeBuffer, input[offset+2], 12)
		writeLongBE6(writeBuffer, input[offset+3], 18)
		writeLongBE6(writeBuffer, input[offset+4], 24)
		writeLongBE6(writeBuffer, input[offset+5], 30)
		writeLongBE6(writeBuffer, input[offset+6], 36)
		writeLongBE6(writeBuffer, input[offset+7], 42)
	case 7:
		writeLongBE7(writeBuffer, input[offset+0], 0)
		writeLongBE7(writeBuffer, input[offset+1], 7)
		writeLongBE7(writeBuffer, input[offset+2], 14)
		writeLongBE7(writeBuffer, input[offset+3], 21)
		writeLongBE7(writeBuffer, input[offset+4], 28)
		writeLongBE7(writeBuffer, input[offset+5], 35)
		writeLongBE7(writeBuffer, input[offset+6], 42)
		writeLongBE7(writeBuffer, input[offset+7], 49)
	case 8:
		writeLongBE8(writeBuffer, input[offset+0], 0)
		writeLongBE8(writeBuffer, input[offset+1], 8)
		writeLongBE8(writeBuffer, input[offset+2], 16)
		writeLongBE8(writeBuffer, input[offset+3], 24)
		writeLongBE8(writeBuffer, input[offset+4], 32)
		writeLongBE8(writeBuffer, input[offset+5], 40)
		writeLongBE8(writeBuffer, input[offset+6], 48)
		writeLongBE8(writeBuffer, input[offset+7], 56)
	}

	toWrite := numHops * numBytes
	for j := 0; j < toWrite; j++ {
		err := w.WriteByte(writeBuffer[j])
		if err != nil {
			return err
		}
	}
	return nil
}

func writeLongBE2(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 0)
}

func writeLongBE3(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 0)
}

func writeLongBE4(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 24)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+3] = byte(uint64(val) >> 0)
}

func writeLongBE5(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 32)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 24)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+3] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+4] = byte(uint64(val) >> 0)
}

func writeLongBE6(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 40)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 32)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 24)
	writeBuffer[wbOffset+3] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+4] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+5] = byte(uint64(val) >> 0)
}

func writeLongBE7(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 48)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 40)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 32)
	writeBuffer[wbOffset+3] = byte(uint64(val) >> 24)
	writeBuffer[wbOffset+4] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+5] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+6] = byte(uint64(val) >> 0)
}

func writeLongBE8(writeBuffer []byte, val int64, wbOffset int) {
	writeBuffer[wbOffset+0] = byte(uint64(val) >> 56)
	writeBuffer[wbOffset+1] = byte(uint64(val) >> 48)
	writeBuffer[wbOffset+2] = byte(uint64(val) >> 40)
	writeBuffer[wbOffset+3] = byte(uint64(val) >> 32)
	writeBuffer[wbOffset+4] = byte(uint64(val) >> 24)
	writeBuffer[wbOffset+5] = byte(uint64(val) >> 16)
	writeBuffer[wbOffset+6] = byte(uint64(val) >> 8)
	writeBuffer[wbOffset+7] = byte(uint64(val) >> 0)
}

func writeRemainingLongs(w io.ByteWriter, offset int, input []int64, remainder int, numBytes int) error {
	numHops := remainder
	idx := 0
	writeBuffer := make([]byte, 64, 64)
	switch numBytes {
	case 1:
		for remainder > 0 {
			writeBuffer[idx] = byte(input[offset+idx] & 255)
			remainder--
			idx++
		}
	case 2:
		for remainder > 0 {
			writeLongBE2(writeBuffer, input[offset+idx], idx*2)
			remainder--
			idx++
		}
	case 3:
		for remainder > 0 {
			writeLongBE3(writeBuffer, input[offset+idx], idx*3)
			remainder--
			idx++
		}
	case 4:
		for remainder > 0 {
			writeLongBE4(writeBuffer, input[offset+idx], idx*4)
			remainder--
			idx++
		}

	case 5:
		for remainder > 0 {
			writeLongBE5(writeBuffer, input[offset+idx], idx*5)
			remainder--
			idx++
		}
	case 6:
		for remainder > 0 {
			writeLongBE6(writeBuffer, input[offset+idx], idx*6)
			remainder--
			idx++
		}
	case 7:
		for remainder > 0 {
			writeLongBE7(writeBuffer, input[offset+idx], idx*7)
			remainder--
			idx++
		}
	case 8:
		for remainder > 0 {
			writeLongBE8(writeBuffer, input[offset+idx], idx*8)
			remainder--
			idx++
		}
	}

	toWrite := numHops * numBytes
	for j := 0; j < toWrite; j++ {
		err := w.WriteByte(writeBuffer[j])
		if err != nil {
			return err
		}
	}
	return nil
}

func writeVuint(w io.ByteWriter, value int64) error {
	for {
		if (value & ^0x7f) == 0 {
			err := w.WriteByte(byte(value))
			if err != nil {
				return err
			}
			return nil
		} else {
			err := w.WriteByte(byte(0x80 | (value & 0x7f)))
			if err != nil {
				return err
			}
			value = int64(uint(value) >> 7)
		}
	}
}

func writeVsint(w io.ByteWriter, value int64) error {
	return writeVuint(w, (value<<1)^(value>>63))
}
