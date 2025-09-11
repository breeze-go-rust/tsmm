package go_tsmm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/breeze-go-rust/go-tsmm/internal/common"
	"math"
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
	nLock       sync.Mutex
}

func (n *node) update(group *sync.WaitGroup, kvs common.Inodes, from, to int) error {
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
	// 对于 Branch 节点
	if len(n.inodes) == 0 {
		child := &node{isLeaf: true, inodes: make(common.Inodes, 0), bTree: n.bTree, parent: n}
		atomic.AddInt32(&n.childrenNum, 1)
		group.Add(1)
		if err := n.bTree.branchNodePool.Invoke(&task{wg: group, n: child, kvs: kvs, from: from, to: to}); err != nil {
			return fmt.Errorf("invoke failed: %v", err)
		}
	} else {
		ranges := n.split(kvs, from, to)
		group.Add(len(ranges))
		atomic.AddInt32(&n.childrenNum, int32(len(ranges)))
		tempInodes := make(common.Inodes, len(n.inodes))
		copy(tempInodes, n.inodes)
		for idx, irs := range ranges {
			in := tempInodes[idx]
			child, err := n.findChild(in.Pgid(), in.Overflow())
			if err != nil {
				return err
			}
			if child.isLeaf {
				n.nLock.Lock()
				n.del(child.key)
				n.nLock.Unlock()
			}
			if err := n.bTree.branchNodePool.Invoke(&task{wg: group, n: child, kvs: kvs, from: irs.from, to: irs.to}); err != nil {
				return fmt.Errorf("invoke failed: %v", err)
			}
		}
	}
	return nil
}
func (n *node) findChild(pgId common.Pgid, overflow uint32) (*node, error) {
	child, err := n.bTree.parent().pageNode(pgId, overflow)
	if err != nil {
		return nil, fmt.Errorf("find child failed with pgid=%d,overflow=%d: %v", pgId, overflow, err)
	}
	if child != nil {
		if child.pgid != pgId || child.overflow != overflow {
			panic(fmt.Sprintf("find child error, want:pgid=%d,overflow=%d,but got: pgid=%d,overflow=%d", pgId, overflow, child.pgid, child.overflow))
		}
		return child, nil
	}
	return nil, fmt.Errorf("find child not found pgid=%d,overflow=%d", pgId, overflow)

}
func (n *node) split(kvs common.Inodes, start, end int) map[int]inodeRange {
	var search func(inodes common.Inodes, key []byte, base int, length int) (int, bool)

	search = func(inodes common.Inodes, key []byte, base int, length int) (int, bool) {
		var exact bool
		index := sort.Search(length, func(i int) bool {
			ret := bytes.Compare(inodes[i+base].Key(), key)
			if ret == 0 {
				exact = true
			}
			return ret != -1
		})
		index = index + base
		if !exact && index > 0 {
			index--
		}
		return index, exact
	}
	res := make(map[int]inodeRange)
	// obtain first and last inode branch
	FirstIndex, _ := search(n.inodes, kvs[start].Key(), 0, len(n.inodes))
	LastIndex, _ := search(n.inodes, kvs[end].Key(), 0, len(n.inodes))
	// if first and last inode branch are the same, just return
	if FirstIndex == LastIndex {
		res[FirstIndex] = inodeRange{from: start, to: end}
		return res
	}

	exact := false
	from := start
	var to int
	for i := FirstIndex; i <= LastIndex; i++ {
		if from > end {
			break
		}
		if i == LastIndex {
			res[i] = inodeRange{from: from, to: end}
			break
		}
		to, exact = search(kvs, n.inodes[i+1].Key(), from, end-from+1)
		if exact {
			to--
		}
		if to >= from {
			res[i] = inodeRange{from: from, to: to}
			from = to + 1
		}
	}
	return res
}

func (n *node) leafNode(kvs common.Inodes, group *sync.WaitGroup) {
	if len(kvs) == 0 {
		return
	}

}

// 通过 归并的方式 将 kvs 和 n.inodes 的 所有的 inode 全部 合并起来
func (n *node) leafNodeMergeInodes(kvs common.Inodes) {
	bTree := n.bTree.parent()
	threshold := bTree.pSize // 默认4K大小
	if bTree.EnableCompress() {
		threshold = 4 * bTree.pSize
	}
	tempInodes := make(common.Inodes, len(n.inodes)) // 原始
	tempKvs := make(common.Inodes, len(kvs))         // 新的数据
	var (
		aIndex, bIndex int // 采用普通归并的方式
	)
	copy(tempInodes, n.inodes)
	copy(tempKvs, kvs)
	manager := newLeafSpillManager(bTree, n, int(threshold), bTree.EnableCompress())
	defer manager.close()

	for aIndex < len(tempInodes) && bIndex < len(tempKvs) {
		compare := bytes.Compare(tempInodes[aIndex].Key(), tempKvs[bIndex].Key())
		switch compare {
		case -1: // tmpInodes 数据写入
			manager.appendInode(tempInodes[aIndex], nil)
			aIndex++
		case 0: // 存在相同的数据
			oldFid := binary.LittleEndian.Uint64(n.inodes[bIndex].Value()[:8])
			oldIndex := binary.LittleEndian.Uint64(n.inodes[bIndex].Value()[8:16])
			if tempKvs[bIndex].Value() != nil {
				actualValue := manager.genInode(tempKvs[bIndex], oldFid, oldIndex, bTree.header.InSequence())
				manager.appendInode(tempKvs[bIndex], actualValue)
			} else {
				manager.del(oldFid, oldIndex)
			}
			bIndex++
			aIndex++
		case 1: // tempKvs 数据写入
			if tempKvs[bIndex].Value() != nil {
				actualValue := manager.genInode(tempKvs[bIndex], math.MaxUint64, math.MaxUint64, bTree.header.InSequence())
				manager.appendInode(tempKvs[bIndex], actualValue)
			}
		}
	}

	for ; aIndex < len(tempInodes); aIndex++ {
		if tempInodes[aIndex].Value() != nil {
			manager.appendInode(tempInodes[aIndex], nil)
		}
	}

	for ; bIndex < len(tempKvs); bIndex++ {
		if tempKvs[bIndex].Value() != nil {
			manager.appendInode(tempKvs[bIndex], nil)
		}
	}

}

func (n *node) updateBranchNode(group *sync.WaitGroup) {
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

func compareKeys(left, right []byte) int {
	return bytes.Compare(left, right)
}

type inodeRange struct {
	from, to int
}
type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].Key(), s[j].inodes[0].Key()) == -1
}

type task struct {
	wg       *sync.WaitGroup
	n        *node
	kvs      common.Inodes
	from, to int
}

func (t *task) leafNodePoolTask() {
	if err := t.n.update(t.wg, t.kvs, t.from, t.to); err != nil {
		return
	}
}

func (t *task) branchNodePoolTask() {
	if t.n.isLeaf {
		panic("branch node is leaf")
	}
	t.n.updateBranchNode(t.wg)
}
