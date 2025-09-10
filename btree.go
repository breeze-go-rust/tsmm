package go_tsmm

import (
	"bytes"
	"fmt"
	"github.com/breeze-go-rust/tsmm/cache"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"github.com/breeze-go-rust/tsmm/internal/compress"
	"github.com/breeze-go-rust/tsmm/internal/freelist"
	"github.com/breeze-go-rust/tsmm/util"
	"github.com/breeze-go-rust/tsmm/vexodb"
	"github.com/panjf2000/ants/v2"
	"path/filepath"
	"sync"
	"unsafe"
)

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5

type BTree struct {
	header         *common.InBTree
	versionNum     uint32
	isSubBTree     bool
	isReadOnly     bool
	parentBTree    *BTree
	rootPage       *common.Page // root's page
	bTrees         map[string]*BTree
	dirtyBTrees    map[string]*BTree
	dirtyNodeCache cache.Cache
	rootNode       *node
	batch          *SkipList
	freelist       freelist.Interface
	ctx            *context
	metas          []*common.Meta
	pageMgr        *PageMgr
	metaMgr        *MetaMgr
	fillPercent    float64
	leafNodePool   *ants.MultiPoolWithFunc
	branchNodePool *ants.MultiPoolWithFunc
	subBTreePool   *ants.MultiPoolWithFunc
	pSize          uint32
	compressor     compress.Compressor
	compressEnable bool

	dataBufferPool *sync.Pool
	hashBufferPool *sync.Pool
	vlog           *vexodb.ValueLog
}

const (
	BTreePageFileIndex = "index"
)

func NewBTree(isReadOnly bool, isSubBTree bool, noSync bool,
	baseBTreePath string,
	compressType string,
	activateMetaVersion int,
	seq uint64, name string, pgId common.Pgid, overflow uint32) (*BTree, error) {

	pageFilePath := filepath.Join(baseBTreePath, BTreePageFileIndex)
	metaFilePath := filepath.Join(baseBTreePath, "versions")

	var err error
	bTree := &BTree{
		batch:      NewSkipList(),
		isSubBTree: isSubBTree,
		isReadOnly: isReadOnly,
		header:     common.NewInBTree(pgId, overflow, name, seq),
		versionNum: uint32(activateMetaVersion),
		compressor: compress.NewCompressor(compressType),
	}
	if !isSubBTree {
		bTree.bTrees = make(map[string]*BTree)
		bTree.dirtyBTrees = make(map[string]*BTree)
		bTree.freelist = freelist.NewHashMapFreelist()
		bTree.metas = make([]*common.Meta, 40)
		bTree.leafNodePool, err = ants.NewMultiPoolWithFunc(40, ants.DefaultAntsPoolSize, func(a any) {}, ants.RoundRobin)
		common.Assert(err != nil, "bTree: create leaf node multi pool failed.")
		bTree.branchNodePool, err = ants.NewMultiPoolWithFunc(40, ants.DefaultAntsPoolSize, func(a any) {}, ants.RoundRobin)
		common.Assert(err != nil, "bTree: create branch node multi pool failed.")
		bTree.subBTreePool, err = ants.NewMultiPoolWithFunc(40, ants.DefaultAntsPoolSize, func(a any) {}, ants.RoundRobin)
		common.Assert(err != nil, "bTree: create bTree node multi pool failed.")
		bTree.pageMgr, err = NewPageMgr(pageFilePath, noSync)
		common.Assert(err != nil, "bTree: create page manager failed.")
		bTree.metaMgr, err = NewMetaMgr(metaFilePath, activateMetaVersion, noSync)
		common.Assert(err != nil, "bTree: create meta manager failed.")
	}
	if err := bTree.init(); err != nil {

	}
	return bTree, nil
}

func (b *BTree) init() error {
	// 对当前活跃的 40 版本的 调整
	return nil
}

func (b *BTree) Put(key, value []byte) error {
	// 对 Key 进行解析
	prefix, name, realKey := util.ParseKey(key)
	if name == nil { // 不存在 子树
		if prefix == nil || bytes.Compare(prefix, util.AccountPrefix()) != 0 {
			return fmt.Errorf("invalid prefix")
		}
		return b.batch.Put(realKey, value)
	}
	tree := b.createIfNotExists(string(name))
	return tree.batch.Put(realKey, value)
}

func (b *BTree) Delete(key []byte) error {
	return b.Put(key, nil)
}

func (b *BTree) createIfNotExists(name string) *BTree {
	if tree, ok := b.bTrees[name]; ok {
		return tree
	}
	// TODO 从这个主树上去找这个子树

	// 也没找到，构建一个新的子树
	btree := &BTree{
		header:     &common.InBTree{},
		rootPage:   &common.Page{},
		rootNode:   nil,
		isSubBTree: true,
		batch:      NewSkipList(),
	}
	btree.header.SetName(name)
	btree.parentBTree = b
	b.bTrees[name] = btree
	return btree
}

func (b *BTree) Update() error {
	var wg sync.WaitGroup
	var errCh chan error
	if len(b.dirtyBTrees) != 0 {
		errCh = make(chan error, len(b.dirtyBTrees))
		for _, tree := range b.dirtyBTrees {
			wg.Add(1)
			go func(tree *BTree) {
				errCh <- tree.update(&wg, tree.batch.Dump())
			}(tree)
		}
	}
	wg.Wait()
	return b.update(&wg, b.batch.Dump())
}

func (b *BTree) update(wg *sync.WaitGroup, kvs common.Inodes) error {
	if len(kvs) == 0 {
		wg.Done()
		return nil
	}
	if b.header.RootPage() == 0 {
		b.rootNode = &node{isLeaf: true, bTree: b, pgid: 0, overflow: 0}
	} else {
		var err error
		b.rootNode, err = b.pageNode(b.header.RootPage(), b.header.Overflow())
		if err != nil {
			return err
		}
	}
	return b.rootNode.update(wg, kvs, 0, len(kvs)-1)
}

func (b *BTree) parent() *BTree {
	if b.parentBTree == nil {
		return b
	}
	return b.parentBTree
}

func (b *BTree) pageSize() uint32 {
	return b.pSize
}

func (b *BTree) page(id common.Pgid, overflow uint32) (*common.Page, error) {
	page, err := b.pageMgr.ReadAt(id, overflow)
	if err != nil {
		return nil, fmt.Errorf("pageMgr.ReadAt(%d, %d): %w", id, overflow, err)
	}
	if page.Overflow() != overflow {
		return page, fmt.Errorf("overflow is not equal to want. want %d, got %d", overflow, page.Overflow())
	}
	return page, nil
}

func (b *BTree) pageNode(id common.Pgid, overflow uint32) (*node, error) {
	p, err := b.page(id, overflow)
	if err != nil {
		return nil, err
	}
	n := &node{bTree: b}
	n.read(p)
	return n, nil
}

func (b *BTree) allocate(count int) *common.Page {
	buf := make([]byte, count*common.DefaultPageSize)
	page := (*common.Page)(unsafe.Pointer(&buf[0]))
	defer func() {
		// TODO 缓存当前 page
	}()
	page.SetOverflow(uint32(count) - 1)
	pid := b.freelist.Allocate(common.TxID(b.header.InSequence()), count)
	if pid != 0 {
		page.SetId(pid)
		return page
	}
	return page
}

func (b *BTree) EnableCompress() bool {
	return b.compressEnable
}

// 以下操作，仅 主树 可以操作
