package tsmm

type Iterator interface {
	Key() []byte
	Value() []byte
	Get(key []byte) ([]byte, error)
	Seek(key []byte) bool
	Next() bool
	Error() error
	Last() bool
	Prev() bool
	First() bool
}
