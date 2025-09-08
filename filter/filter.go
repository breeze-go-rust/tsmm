package filter

// Buffer is the interface that wraps basic Alloc, Write and WriteByte methods.
type Buffer interface {
	Alloc(n int) []byte
	Write(p []byte) (n int, err error)
	WriteByte(c byte) error
}

// Filter is the filter.
type Filter interface {
	Name() string
	NewGenerator() FilterGenerator
	Contains(filter, key []byte) bool
}

// FilterGenerator is the filter generator.
type FilterGenerator interface {
	Add(key []byte)
	Generate(b Buffer)
}
