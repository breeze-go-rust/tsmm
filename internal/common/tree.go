package common

import (
	"fmt"
	"unsafe"
)

const BucketHeaderSize = int(unsafe.Sizeof(InBTree{}))

// InBTree represents the on-file representation of a bucket.
// This is stored as the "value" of a tree key. If the tree is small enough,
// then its root page can be stored inline in the "value", after the tree
// header. In the case of inline trees, the "root" will be 0.
type InBTree struct {
	root     Pgid // page id of the tree's root-level page
	overflow uint32
	name     string // tree name
	sequence uint64 // monotonically incrementing, used by NextSequence()
}

func NewInBTree(root Pgid, overflow uint32, name string, seq uint64) *InBTree {
	return &InBTree{
		root:     root,
		overflow: overflow,
		name:     name,
		sequence: seq,
	}
}

func (b *InBTree) RootPage() Pgid {
	return b.root
}

func (b *InBTree) SetRootPage(id Pgid) {
	b.root = id
}

func (b *InBTree) Name() string {
	return b.name
}

func (b *InBTree) SetName(name string) {
	b.name = name
}

func (b *InBTree) Overflow() uint32 {
	return b.overflow
}

func (b *InBTree) SetOverflow(overflow uint32) {
	b.overflow = overflow
}

// InSequence returns the sequence. The reason why not naming it `Sequence`
// is to avoid duplicated name as `(*Tree) Sequence()`
func (b *InBTree) InSequence() uint64 {
	return b.sequence
}

func (b *InBTree) SetInSequence(v uint64) {
	b.sequence = v
}

func (b *InBTree) IncSequence() {
	b.sequence++
}

func (b *InBTree) InlinePage(v []byte) *Page {
	return (*Page)(unsafe.Pointer(&v[BucketHeaderSize]))
}

func (b *InBTree) String() string {
	return fmt.Sprintf("<pgid=%d,name=%s,seq=%d>", b.root, b.name, b.sequence)
}
