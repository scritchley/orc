package orc

import (
	"bytes"
	"fmt"
	"io"
)

const (
	DefaultByteArrayChunkSize = 32 * 1024
	DefaultNumChunks          = 128
)

type DynamicByteSlice struct {
	chunkSize         int
	data              [][]byte
	length            int
	initializedChunks int
}

func NewDynamicByteSlice(numChunks, chunkSize int) *DynamicByteSlice {
	return &DynamicByteSlice{
		chunkSize: chunkSize,
		data:      make([][]byte, numChunks, numChunks),
	}
}

func (d *DynamicByteSlice) grow(chunkIndex int) {
	if chunkIndex >= d.initializedChunks {
		if chunkIndex >= len(d.data) {
			newSize := int(maxInt64(int64(chunkIndex+1), int64(2*len(d.data))))
			newChunk := make([][]byte, newSize, newSize)
			copy(newChunk, d.data)
			d.data = newChunk
		}
		for i := d.initializedChunks; i <= chunkIndex; i++ {
			d.data[i] = make([]byte, d.chunkSize)
		}
		d.initializedChunks = chunkIndex + 1
	}
}

func (d *DynamicByteSlice) getIndex(index int) byte {
	if index >= d.length {
		return 0
	}
	i := index / d.chunkSize
	j := index % d.chunkSize
	return d.data[i][j]
}

func (d *DynamicByteSlice) set(index int, value byte) {
	i := index / d.chunkSize
	j := index % d.chunkSize
	d.grow(i)
	if index >= d.length {
		d.length = index + 1
	}
	d.data[i][j] = value
}

func (d *DynamicByteSlice) addByte(value byte) int {
	i := d.length / d.chunkSize
	j := d.length % d.chunkSize
	d.grow(i)
	d.data[i][j] = value
	result := d.length
	d.length++
	return result
}

func (d *DynamicByteSlice) add(value []byte, valueOffset int, valueLength int) int {
	i := d.length / d.chunkSize
	j := d.length % d.chunkSize
	d.grow((d.length + valueLength) / d.chunkSize)
	remaining := valueLength
	for remaining > 0 {
		size := int(minInt64(int64(remaining), int64(d.chunkSize-j)))
		copy(d.data[i][j:j+size], value[valueOffset:valueOffset+size])
		remaining -= size
		valueOffset += size
		i++
		j = 0
	}
	result := d.length
	d.length += valueLength
	return result
}

func (d *DynamicByteSlice) readAll(r io.ByteReader) error {
	currentChunk := d.length / d.chunkSize
	currentOffset := d.length % d.chunkSize
	d.grow(currentChunk)
	var i int
	for i = currentOffset; i < d.chunkSize-currentOffset; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		d.data[currentChunk][i] = b
	}
	d.length += i
	return nil
}

func (d *DynamicByteSlice) compare(other []byte, otherOffset int, otherLength int, ourOffset int, ourLength int) int {
	currentChunk := ourOffset / d.chunkSize
	currentOffset := ourOffset % d.chunkSize
	maxLength := int(minInt64(int64(otherLength), int64(ourLength)))
	for maxLength > 0 &&
		other[otherOffset] == d.data[currentChunk][currentOffset] {
		otherOffset++
		currentOffset++
		if currentOffset == d.chunkSize {
			currentChunk++
			currentOffset = 0
		}
		maxLength--
	}
	if maxLength == 0 {
		return otherLength - ourLength
	}
	otherByte := 0xff & other[otherOffset]
	ourByte := 0xff & d.data[currentChunk][currentOffset]
	if otherByte > ourByte {
		return 1
	}
	return -1
}

func (d *DynamicByteSlice) size() int {
	return d.length
}

func (d *DynamicByteSlice) clear() {
	d.length = 0
	for i := 0; i < len(d.data); i++ {
		d.data[i] = nil
	}
	d.initializedChunks = 0
}

func (d *DynamicByteSlice) setText(result bytes.Buffer, offset int, length int) {
	result.Reset()
	currentChunk := offset / d.chunkSize
	currentOffset := offset % d.chunkSize
	currentLength := int(minInt64(int64(length), int64(d.chunkSize-currentOffset)))
	for length > 0 {
		_, err := result.Write(d.data[currentChunk][currentOffset : currentOffset+currentLength])
		if err != nil {
			// TODO: Handle this somehow.
			panic(err)
		}
		length -= currentLength
		currentChunk++
		currentOffset = 0
		currentLength = int(minInt64(int64(length), int64(d.chunkSize-currentOffset)))
	}
}

func (d *DynamicByteSlice) write(out io.ByteWriter, offset int, length int) error {
	currentChunk := offset / d.chunkSize
	currentOffset := offset % d.chunkSize
	for length > 0 {
		currentLength := int(minInt64(int64(length), int64(d.chunkSize-currentOffset)))
		for i := currentOffset; i < currentLength; i++ {
			err := out.WriteByte(d.data[currentChunk][i])
			if err != nil {
				return err
			}
		}
		length -= currentLength
		currentChunk++
		currentOffset = 0
	}
	return nil
}

func (d *DynamicByteSlice) String() (string, error) {
	var i int
	var buf bytes.Buffer
	_, err := buf.WriteString(`{`)
	if err != nil {
		return "", err
	}
	l := d.length - 1
	for i := 0; i < l; i++ {
		_, err := buf.WriteString(fmt.Sprintf("%#x", d.getIndex(i)))
		if err != nil {
			return "", err
		}
		_, err = buf.WriteString(`,`)
		if err != nil {
			return "", err
		}
	}
	_, err = buf.WriteString(fmt.Sprintf("%#x", d.getIndex(i)))
	if err != nil {
		return "", err
	}
	_, err = buf.WriteString(`}`)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (d *DynamicByteSlice) setByteBuffer(result bytes.Buffer, offset int, length int) {
	result.Reset()
	currentChunk := offset / d.chunkSize
	currentOffset := offset % d.chunkSize
	currentLength := int(minInt64(int64(length), int64(d.chunkSize-currentOffset)))
	for length > 0 {
		result.Write(d.data[currentChunk][currentOffset : currentOffset+currentLength])
		length -= currentLength
		currentChunk++
		currentOffset = 0
		currentLength = int(minInt64(int64(length), int64(d.chunkSize-currentOffset)))
	}
}

func (d *DynamicByteSlice) get() []byte {
	result := make([]byte, d.length)
	if d.length > 0 {
		var currentChunk int
		var currentOffset int
		currentLength := int(minInt64(int64(d.length), int64(d.chunkSize)))
		var destOffset int
		totalLength := d.length
		for totalLength > 0 {
			copy(result[destOffset:destOffset+currentLength], d.data[currentChunk][currentOffset:currentOffset+currentLength])
			destOffset += currentLength
			totalLength -= currentLength
			currentChunk++
			currentOffset = 0
			currentLength = int(minInt64(int64(totalLength), int64(d.chunkSize-currentOffset)))
		}
	}
	return result
}

func (d *DynamicByteSlice) getSizeInBytes() int64 {
	return int64(d.initializedChunks * d.chunkSize)
}
