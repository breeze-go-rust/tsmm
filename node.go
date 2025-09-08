package tsmm

import (
	"bytes"
	"fmt"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"sort"
	"sync"
	"sync/atomic"
)

type node struct {
	bTree       *BTree
	key         []byte
	pgid        common.Pgid
	overflow    uint32
	page        *common.Page
	isLeaf      bool
	hash        [20]byte
	parent      *node
	children    nodes
	inodes      common.Inodes
	childrenNum int32
}

func (n *node) update(group *sync.WaitGroup, kvs common.KVS, from, to int) error {
	defer group.Done()
	common.Assert(len(kvs) == 0, "update: kvs is empty")
	if n.isLeaf {
		if n.parent == nil {
			n.parent = &node{isLeaf: false, bTree: n.bTree}
			atomic.AddInt32(&n.parent.childrenNum, 1)
		}
		n.leafNode(kvs[from:to], group)
		return nil
	}
	return nil
}

func (n *node) put(oldKey, newKey []byte, value []byte, pgId common.Pgid, overflow uint32, flags uint32, hash []byte) {
	if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	}
	if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}
	index := sort.Search(len(n.inodes), func(i int) bool {
		return bytes.Compare(n.inodes[i].Key(), oldKey) != -1
	})
	exact := len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].Key(), oldKey)
	if !exact {
		n.inodes = append(n.inodes, &common.Inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}
	inode := n.inodes[index]
	inode.SetKey(newKey)
	inode.SetValue(value)
	inode.SetPgid(pgId)
	inode.SetFlags(flags)
	inode.SetOverflow(overflow)
	if hash != nil {
		inode.SetHash(hash)
	}
}

func (n *node) del(oldKey []byte) {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].Key(), oldKey) != -1 })
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].Key(), oldKey) {
		return
	}
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)
}

func (n *node) read(p *common.Page) {
	n.pgid = p.Id()
	n.overflow = p.Overflow()
	n.isLeaf = p.IsLeafPage()
	n.inodes = common.ReadInodeFromPage(p)

	if len(n.inodes) > 0 {
		n.key = n.inodes[0].Key()
		common.Assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

func (n *node) write(p *common.Page, data []byte) {
	if n.isLeaf {
		p.SetFlags(common.LeafPageFlag)
	} else {
		p.SetFlags(common.BranchPageFlag)
	}
	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.Id()))
	}
	p.SetHash(n.hash[:])
	p.SetCount(uint16(len(n.inodes)))
	if p.Count() == 0 {
		return
	}
	common.WriteInodeToPage(n.inodes, p)
}

func (n *node) free() {
	if n.page.Id() != 0 {
		n.bTree.freelist.Free(n.bTree.ctx.meta.Txid(), n.page)
	}
}

func (n *node) leafNode(kvs common.KVS, group *sync.WaitGroup) {

}
func compareKeys(left, right []byte) int {
	return bytes.Compare(left, right)
}

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].Key(), s[j].inodes[0].Key()) == -1
}
