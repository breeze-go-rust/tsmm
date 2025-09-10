package compress

type DirectCompressor struct{}

func NewDirectCompressor() *DirectCompressor {
	return &DirectCompressor{}
}
func (DirectCompressor) Encode(data, src []byte) []byte {
	return src
}

func (DirectCompressor) Decode(data, src []byte) ([]byte, error) {
	return src, nil
}
