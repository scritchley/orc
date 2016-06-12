package orc

const (
	Null        = -1
	LeftOffset  = 0
	RightOffset = 1
	ElementSize = 2
)

type RedBlackTree struct {
	size         int
	data         *DynamicIntSlice
	root         int
	lastAdd      int
	wasAdd       bool
	CompareValue func(position int) int
}

func NewRedBlackTree(initialCapacity int) *RedBlackTree {
	return &RedBlackTree{
		data: NewDynamicIntSlice(initialCapacity * ElementSize),
		root: Null,
	}
}

func (r *RedBlackTree) insert(left int, right int, isRed bool) int {
	position := r.size
	r.size++
	r.setLeftRed(position, left, isRed)
	r.setRight(position, right)
	return position
}

func (r *RedBlackTree) isRed(position int) bool {
	return position != Null && (r.data.get(position*ElementSize+LeftOffset)&1) == 1
}

func (r *RedBlackTree) setRed(position int, isRed bool) {
	offset := position*ElementSize + LeftOffset
	if isRed {
		r.data.set(offset, r.data.get(offset)|1)
	} else {
		r.data.set(offset, r.data.get(offset) & ^1)
	}
}

func (r *RedBlackTree) getLeft(position int) int {
	return r.data.get(position*ElementSize+LeftOffset) >> 1
}

func (r *RedBlackTree) getRight(position int) int {
	return r.data.get(position*ElementSize + RightOffset)
}

func (r *RedBlackTree) setLeft(position int, left int) {
	offset := position*ElementSize + LeftOffset
	r.data.set(offset, (left<<1)|(r.data.get(offset)&1))
}

func (r *RedBlackTree) setLeftRed(position int, left int, isRed bool) {
	offset := position*ElementSize + LeftOffset
	var red int
	if isRed {
		red = 1
	}
	r.data.set(offset, (left<<1)|red)
}

func (r *RedBlackTree) setRight(position int, right int) {
	r.data.set(position*ElementSize+RightOffset, right)
}

func (r *RedBlackTree) add(node int, fromLeft bool, parent int, grandParent int, greatGrandparent int) bool {
	if node == Null {
		if r.root == Null {
			r.lastAdd = r.insert(Null, Null, false)
			r.root = r.lastAdd
			r.wasAdd = true
			return false
		} else {
			r.lastAdd = r.insert(Null, Null, true)
			node = r.lastAdd
			r.wasAdd = true
			// connect the new node into the tree
			if fromLeft {
				r.setLeft(parent, node)
			} else {
				r.setRight(parent, node)
			}
		}
	} else {
		compare := r.CompareValue(node)
		var keepGoing bool
		// Recurse down to find where the node needs to be added
		if compare < 0 {
			keepGoing = r.add(r.getLeft(node), true, node, parent, grandParent)
		} else if compare > 0 {
			keepGoing = r.add(r.getRight(node), false, node, parent, grandParent)
		} else {
			r.lastAdd = node
			r.wasAdd = false
			return false
		}
		// we don't need to fix the root (because it is always set to black)
		if node == r.root || !keepGoing {
			return false
		}
	}

	if r.isRed(node) && r.isRed(parent) {
		if parent == r.getLeft(grandParent) {
			uncle := r.getRight(grandParent)
			if r.isRed(uncle) {
				r.setRed(parent, false)
				r.setRed(uncle, false)
				r.setRed(grandParent, true)
				return true
			} else {
				if node == r.getRight(parent) {
					// case 1.2
					// swap node and parent
					tmp := node
					node = parent
					parent = tmp
					// left-rotate on node
					r.setLeft(grandParent, parent)
					r.setLeft(node, r.getLeft(parent))
					r.setLeft(parent, node)
				}

				r.setRed(parent, false)
				r.setRed(grandParent, true)
				// right-rotate on grandparent
				if greatGrandparent == Null {
					r.root = parent
				} else if r.getLeft(greatGrandparent) == grandParent {
					r.setLeft(greatGrandparent, parent)
				} else {
					r.setRight(greatGrandparent, parent)
				}
				r.setLeft(grandParent, r.getRight(parent))
				r.setRight(parent, grandParent)
				return false
			}
		} else {
			uncle := r.getLeft(grandParent)
			if r.isRed(uncle) {
				r.setRed(parent, false)
				r.setRed(uncle, false)
				r.setRed(grandParent, true)
				return true
			} else {
				if node == r.getLeft(parent) {
					// case 2.2
					// swap node and parent
					tmp := node
					node = parent
					parent = tmp

					// right-rotate on node
					r.setRight(grandParent, parent)
					r.setLeft(node, r.getRight(parent))
					r.setRight(parent, node)
				}

				// case 2.2 and 2.3
				r.setRed(parent, false)
				r.setRed(grandParent, true)
				// left-rotate on grandparent
				if greatGrandparent == Null {
					r.root = parent
				} else if r.getRight(greatGrandparent) == grandParent {
					r.setRight(greatGrandparent, parent)
				} else {
					r.setLeft(greatGrandparent, parent)
				}
				r.setRight(grandParent, r.getLeft(parent))
				r.setLeft(parent, grandParent)
				return false
			}
		}
	} else {
		return true
	}
}

func (r *RedBlackTree) Add() bool {
	r.add(r.root, false, Null, Null, Null)
	if r.wasAdd {
		r.setRed(r.root, false)
		return true
	} else {
		return false
	}
}

func (r *RedBlackTree) Size() int {
	return r.size
}

func (r *RedBlackTree) clear() {
	r.root = Null
	r.size = 0
	r.data.clear()
}

func (r *RedBlackTree) getSizeInBytes() int64 {
	return int64(r.data.getSizeInBytes())
}
