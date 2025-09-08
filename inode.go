package tsmm

type Inode struct {
	KV       KV
	Flags    uint16
	PID      uint64
	Overflow uint64
	Hash     [HashSize]byte
}

type Inodes []*Inode

const HashSize = 20
