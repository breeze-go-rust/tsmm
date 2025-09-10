package go_tsmm

import (
	"bytes"
	"encoding/binary"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"github.com/breeze-go-rust/tsmm/util/hasher"
	"sync"
)

const (
	ValueSize = 13
	HashSize  = 20
)

type leafSpillManager struct {
	bTree          *BTree
	n              *node
	dts            []*dataTemp
	compressEnable bool
	threshold      int
	update         func([]byte, uint64, uint64, uint64) (uint64, uint64) // data+fid+index+seq  fid+index
	del            func(uint64, uint64)                                  // fid,index
}

func newLeafSpillManager(tree *BTree, n *node, threshold int, compress bool) *leafSpillManager {
	lsm := &leafSpillManager{
		bTree: tree,
		n:     n,
		dts: []*dataTemp{
			nil,
			newDataTemp(threshold, tree.dataBufferPool, tree.hashBufferPool),
		},
		compressEnable: false,
		threshold:      threshold,
		update:         tree.vlog.Update,
		del:            tree.vlog.Del,
	}
	return lsm
}

// 重新生成 value
func (lsm *leafSpillManager) genInode(inode *common.Inode, oldFid uint64, oldIndex uint64, seq uint64) []byte {
	value := inode.Value()
	key := inode.Key()
	h := hasher.NewHash()
	res, _ := h.Hash(append(key, value...))
	fid, index := lsm.update(res, oldFid, oldIndex, seq) // 直接拿到下标 就好了
	valueBuf := make([]byte, ValueSize)
	binary.LittleEndian.PutUint64(valueBuf[:8], fid)     // 8字节写入文件句柄
	binary.LittleEndian.PutUint64(valueBuf[8:16], index) // 8字节写入 索引号
	inode.SetHash(res)
	return nil
}

func (lsm *leafSpillManager) appendInode(inode *common.Inode, value []byte) {
	if inode == nil {
		return
	}
	temp := lsm.dts[1] // 取活跃通道 1
	elementSize := binary.MaxVarintLen64 + len(inode.Key()) + ValueSize + HashSize
	tempSize := temp.size + elementSize
	if len(temp.inodes)+1 < common.MinKeysPerPage*2 || tempSize < lsm.threshold {
		if value != nil {
			i := &common.Inode{}
			i.SetKey(inode.Key())
			i.SetValue(value)
			i.SetHash(inode.Hash())
			temp.inodes = append(temp.inodes, i)
		} else {
			temp.inodes = append(temp.inodes, inode)
		}
		temp.size = tempSize
	}
	// 1 到了限制，开始写 0
	if lsm.dts[0] == nil {
		lsm.dts[0] = newDataTemp(lsm.threshold, lsm.bTree.dataBufferPool, lsm.bTree.hashBufferPool)
		lsm.dts[0], lsm.dts[1] = lsm.dts[1], lsm.dts[0]
	} else {
		// 0,1 都满了，将 0 刷盘

	}
}

func (lsm *leafSpillManager) flush() {
	dt := lsm.dts[0]
	if len(dt.inodes) == 0 {
		return
	}
	hero := &node{bTree: lsm.bTree, isLeaf: lsm.n.isLeaf, parent: lsm.n.parent, inodes: make(common.Inodes, len(dt.inodes))}
	copy(hero.inodes, dt.inodes)
	var data []byte
	if lsm.compressEnable {
		data = lsm.bTree.compressor.Encode(data, dt.dataBuffer.Bytes())
	} else {
		data = dt.dataBuffer.Bytes()
	}
	// 申请 Page
	count := (len(data) + int(common.PageHeaderSize) + int(common.DefaultPageSize) - 1) / common.DefaultPageSize
	hero.overflow = uint32(count) - 1
	h := hasher.NewHash()
	hash, _ := h.Hash(dt.hashBuffer.Bytes())
	defer hasher.Return(h)
	copy(hero.hash[:], hash)
	p := lsm.bTree.allocate(count)
	hero.pgid = p.Id()
	p.SetCount(uint16(len(hero.inodes)))
	p.SetFlags(common.LeafPageFlag)
	p.SetSize(uint32(len(data)))

	common.WriteInodeToPage(hero.inodes, p)

	if hero.parent != nil {
		hero.parent.nLock.Lock()
		hero.parent.put(hero.key, hero.key, nil, hero.pgid, hero.overflow, common.NormalTreeFlag, hero.hash[:])
		hero.parent.nLock.Unlock()
	}
	dt.clear()
	lsm.dts[0], lsm.dts[1] = lsm.dts[1], lsm.dts[0]
}

func (lsm *leafSpillManager) close() {
	for _, dt := range lsm.dts {
		if dt != nil {
			dt.clear()
		}
	}
}

type dataTemp struct {
	size       int
	hasher     *hasher.Hasher
	inodes     common.Inodes
	dataBuffer *bytes.Buffer
	hashBuffer *bytes.Buffer
	lenBuf     [8]byte
}

func newDataTemp(size int, dataPool, hashPool *sync.Pool) *dataTemp {
	dt := &dataTemp{
		size:       size,
		lenBuf:     [8]byte{},
		inodes:     make(common.Inodes, 0),
		hasher:     hasher.NewHash(),
		dataBuffer: dataPool.Get().(*bytes.Buffer),
		hashBuffer: hashPool.Get().(*bytes.Buffer),
	}
	dt.dataBuffer.Reset()
	dt.hashBuffer.Reset()
	return dt
}

func (dt *dataTemp) Size() int {
	return dt.size
}

func (dt *dataTemp) merge(src *dataTemp) {
	dt.size += src.size
	dt.inodes = append(dt.inodes, src.inodes...)
	dt.dataBuffer.Write(src.dataBuffer.Bytes())
	dt.hashBuffer.Write(src.hashBuffer.Bytes())
}

func (dt *dataTemp) clear() {
	hasher.Return(dt.hasher)

	dt.dataBuffer.Reset()
	dt.hashBuffer.Reset()
	dt.inodes = common.Inodes{}
}
