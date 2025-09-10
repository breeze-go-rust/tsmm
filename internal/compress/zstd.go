package compress

import "github.com/klauspost/compress/zstd"

type ZSTDCompressor struct {
	Encoder *zstd.Encoder
	Decoder *zstd.Decoder
}

func NewZSTDCompressor() *ZSTDCompressor {
	writer, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(0))
	reader, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	return &ZSTDCompressor{
		Encoder: writer,
		Decoder: reader,
	}
}
func (zsc *ZSTDCompressor) Encode(data, src []byte) []byte {
	return zsc.Encoder.EncodeAll(src, data)
}

func (zsc *ZSTDCompressor) Decode(data, src []byte) ([]byte, error) {
	return zsc.Encoder.EncodeAll(src, data), nil
}
