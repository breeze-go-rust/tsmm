package hasher

import "sync"

var hashPool = sync.Pool{
	New: func() interface{} {
		return NewHasher(SHA1)
	},
}

func NewHash() *Hasher {
	return hashPool.Get().(*Hasher)
}

func Return(h *Hasher) {
	h.cleanIfDirty()
	hashPool.Put(h)
}
