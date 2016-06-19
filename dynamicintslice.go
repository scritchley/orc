package orc

import (
	"bytes"
	"fmt"
)

const (
	defaultChunkSize = 8 * 1024
	initChunks       = 128
)

type DynamicIntSlice struct {
	chunkSize         int
	data              [][]int
	length            int
	initializedChunks int
}

func NewDynamicIntSlice(chunkSize int) *DynamicIntSlice {
	return &DynamicIntSlice{
		chunkSize: chunkSize,
		data:      make([][]int, initChunks, initChunks),
	}
}

func (d *DynamicIntSlice) grow(chunkIndex int) {
	if chunkIndex >= d.initializedChunks {
		if chunkIndex >= len(d.data) {
			newSize := int(maxInt64(int64(chunkIndex+1), int64(2*len(d.data))))
			newChunk := make([][]int, newSize, newSize)
			copy(newChunk, d.data)
			d.data = newChunk
		}
		for i := d.initializedChunks; i <= chunkIndex; i++ {
			d.data[i] = make([]int, d.chunkSize)
		}
		d.initializedChunks = chunkIndex + 1
	}
}

func (d *DynamicIntSlice) get(index int) int {
	if index >= d.length {
		return 0
	}
	i := index / d.chunkSize
	j := index % d.chunkSize
	return d.data[i][j]
}

func (d *DynamicIntSlice) set(index, value int) {
	i := index / d.chunkSize
	j := index % d.chunkSize
	d.grow(i)
	if index >= d.length {
		d.length = index + 1
	}
	d.data[i][j] = value
}

func (d *DynamicIntSlice) increment(index, value int) {
	i := index / d.chunkSize
	j := index % d.chunkSize
	d.grow(i)
	if index >= d.length {
		d.length = index + 1
	}
	d.data[i][j] += value
}

func (d *DynamicIntSlice) add(value int) {
	i := d.length / d.chunkSize
	j := d.length % d.chunkSize
	d.grow(i)
	d.data[i][j] = value
	d.length++
}

func (d *DynamicIntSlice) size() int {
	return d.length
}

func (d *DynamicIntSlice) clear() {
	d.length = 0
	// TODO: Check logic
	for i := 0; i < len(d.data); i++ {
		d.data[i] = nil
	}
	d.initializedChunks = 0
}

func (d *DynamicIntSlice) String() (string, error) {
	var i int
	l := d.length - 1
	var buf bytes.Buffer
	_, err := buf.WriteString(`{`)
	if err != nil {
		return "", err
	}
	for i = 0; i < l; i++ {
		_, err = buf.WriteString(fmt.Sprint(d.get(i)))
		if err != nil {
			return "", err
		}
		_, err = buf.WriteString(`,`)
		if err != nil {
			return "", err
		}
	}
	_, err = buf.WriteString(fmt.Sprint(d.get(i)))
	if err != nil {
		return "", err
	}
	_, err = buf.WriteString(`}`)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (d *DynamicIntSlice) getSizeInBytes() int64 {
	return int64(4 * d.initializedChunks * d.chunkSize)
}
