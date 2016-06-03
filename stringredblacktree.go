package orc

import ()

type StringRedBlackTree struct {
	*RedBlackTree
	keyOffsets *DynamicIntSlice
	byteArray  *DynamicByteSlice
	newKey     string
}

func NewStringRedBlackTree(initialCapacity int) *StringRedBlackTree {
	rbt := NewRedBlackTree(initialCapacity)
	srbt := &StringRedBlackTree{
		RedBlackTree: rbt,
		keyOffsets:   NewDynamicIntSlice(initialCapacity),
		byteArray:    NewDynamicByteSlice(DefaultByteArrayChunkSize, DefaultNumChunks),
	}
	rbt.CompareValue = srbt.compareValue
	return srbt
}

func (s *StringRedBlackTree) add(value string) int {
	s.newKey = value
	return s.addNewKey()
}

func (s *StringRedBlackTree) addNewKey() int {
	// if the newKey is actually new, add it to our byteArray and store the offset & length
	if s.Add() {
		len := len(s.newKey)
		s.keyOffsets.add(s.byteArray.add([]byte(s.newKey), 0, len))
	}
	return s.lastAdd
}

func (s *StringRedBlackTree) addBytes(bytes []byte, offset int, length int) int {
	s.newKey = string(bytes[offset : offset+length])
	return s.addNewKey()
}

func (s *StringRedBlackTree) compareValue(position int) int {
	start := s.keyOffsets.get(position)
	var end int
	if position+1 == s.keyOffsets.size() {
		end = s.byteArray.size()
	} else {
		end = s.keyOffsets.get(position + 1)
	}
	return s.byteArray.compare([]byte(s.newKey), 0, len(s.newKey), start, end-start)
}

func (s *StringRedBlackTree) getSizeInBytes() int64 {
	return s.byteArray.getSizeInBytes() + s.keyOffsets.getSizeInBytes() + s.RedBlackTree.getSizeInBytes()
}

func (s *StringRedBlackTree) getCharacterSize() int {
	return s.byteArray.size()
}

func (s *StringRedBlackTree) clear() {
	s.RedBlackTree.clear()
	s.byteArray.clear()
	s.keyOffsets.clear()
}
