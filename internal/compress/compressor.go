package compress

type Compressor interface {
	Encode(data, src []byte) []byte
	Decode(data, src []byte) ([]byte, error)
}

func NewCompressor(cType string) Compressor {
	switch cType {
	case "snappy":
		return NewSnappyCompressor()
	case "zstd":
		return NewZSTDCompressor()
	case "direct":
		return NewDirectCompressor()
	default:
		return NewDirectCompressor()
	}
}
