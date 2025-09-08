package tsmm

// Batch BTree Put Buffer
type Batch interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Size() int
	Dump() KVS
}
