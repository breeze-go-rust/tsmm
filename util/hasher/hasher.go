package hasher

import (
	"crypto/sha1"
	"hash"
)

type Hasher struct {
	inner hash.Hash
	dirty bool
}

func NewHasher(ht HashType) *Hasher {
	ht = ht & 0xf0
	switch ht {
	case SHA1:
		return &Hasher{inner: sha1.New()}
	default:
		return &Hasher{inner: sha1.New()}
	}
}

func (h *Hasher) Hash(msg []byte) (hash []byte, err error) {
	h.cleanIfDirty()
	h.dirty = true
	if _, err := h.inner.Write(msg); err != nil {
		return nil, err
	}
	return h.inner.Sum(nil), nil
}

func (h *Hasher) cleanIfDirty() {
	if h.dirty {
		h.inner.Reset()
	}
}
