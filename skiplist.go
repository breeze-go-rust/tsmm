package go_tsmm

import (
	"bytes"
	"github.com/breeze-go-rust/tsmm/internal/common"
	"math/rand"
	"time"
)

const (
	maxLevel    = 32   // 最大层数
	probability = 0.25 // 节点出现在更高层的概率 (1/4)
)

type SkipList struct {
	head    *skipListNode // 头节点
	level   int           // 当前最大层数
	length  int           // 节点数量
	randSrc *rand.Rand    // 随机数生成器
}

func NewSkipList() *SkipList {
	// 初始化随机种子
	src := rand.NewSource(time.Now().UnixNano())
	randSrc := rand.New(src)
	// 创建头节点（不存储实际数据）
	head := newNode(nil, nil, maxLevel)
	return &SkipList{
		head:    head,
		level:   1,
		randSrc: randSrc,
	}
}

func (s *SkipList) Put(key []byte, value []byte) error {
	update := make([]*skipListNode, maxLevel) // 更新路径
	current := s.head

	// 从最高层开始查找插入位置
	for i := s.level - 1; i >= 0; i-- {
		// 在当前层向前搜索，直到找到大于或等于key的节点
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current // 记录搜索路径
	}

	// 检查最底层是否已存在相同key
	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		current.value = value // 更新现有值
		return nil
	}

	// 为新节点随机生成层数
	newLevel := s.randomLevel()
	if newLevel > s.level {
		// 更新更高层的路径指向头节点
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel // 更新跳表层数
	}

	// 创建新节点
	newNode := newNode(key, value, newLevel)

	// 逐层插入新节点
	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	s.length++ // 增加节点计数
	return nil
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	current := s.head

	// 从最高层开始搜索
	for i := s.level - 1; i >= 0; i-- {
		// 在当前层向前搜索，直到找到大于或等于key的节点
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	// 检查最底层的下一个节点
	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, nil // 找到键
	}
	return nil, ErrorKeyNotFound // 键不存在
}

func (s *SkipList) Delete(key []byte) error {
	update := make([]*skipListNode, maxLevel) // 更新路径
	current := s.head

	// 从最高层开始查找删除位置
	for i := s.level - 1; i >= 0; i-- {
		// 在当前层向前搜索，直到找到大于或等于key的节点
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current // 记录搜索路径
	}

	// 定位到待删除节点
	current = current.forward[0]
	if current == nil || !bytes.Equal(current.key, key) {
		return ErrorKeyNotFound // 键不存在
	}

	// 逐层移除节点引用
	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != current {
			break // 更高层不存在该节点
		}
		update[i].forward[i] = current.forward[i]
	}

	// 更新跳表层级（如果顶层变为空）
	for s.level > 1 && s.head.forward[s.level-1] == nil {
		s.level--
	}

	s.length-- // 减少节点计数
	return nil
}

func (s *SkipList) Size() int {
	return s.length
}

func (s *SkipList) Dump() common.Inodes {
	kvs := make(common.Inodes, 0, s.length)
	current := s.head.forward[0] // 从最底层第一个节点开始

	// 遍历最底层链表
	for current != nil {
		// 复制键值避免外部修改影响内部数据
		inode := common.Inode{}
		inode.SetKey(current.key)
		inode.SetValue(current.value)
		kvs = append(kvs, &inode)
		current = current.forward[0]
	}
	return kvs
}

// 随机生成节点层数 (1 ~ maxLevel)
func (s *SkipList) randomLevel() int {
	level := 1
	// 按概率增加层数
	for s.randSrc.Float64() < probability && level < maxLevel {
		level++
	}
	return level
}

// 跳表节点结构
type skipListNode struct {
	key     []byte
	value   []byte
	forward []*skipListNode // 每层的后继指针数组
}

// 创建新节点
func newNode(key, value []byte, level int) *skipListNode {
	return &skipListNode{
		key:     key,
		value:   value,
		forward: make([]*skipListNode, level),
	}
}
