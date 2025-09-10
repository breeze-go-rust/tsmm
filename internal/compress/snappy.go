package compress

import "github.com/klauspost/compress/snappy"

type SnappyCompressor struct {
}

func NewSnappyCompressor() *SnappyCompressor {
	return &SnappyCompressor{}
}

func (s *SnappyCompressor) Encode(data, src []byte) []byte {
	return snappy.Encode(data, src)
}

func (s *SnappyCompressor) Decode(data, src []byte) ([]byte, error) {
	return snappy.Decode(data, src)
}
