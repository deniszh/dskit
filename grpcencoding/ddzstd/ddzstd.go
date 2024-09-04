package ddzstd

import (
	"bytes"
	"github.com/DataDog/zstd"
	"google.golang.org/grpc/encoding"
	"io"
)

const Name = "ddzstd"

type compressor struct{}

func init() {
	encoding.RegisterCompressor(newCompressor())
}

func newCompressor() *compressor {
	c := &compressor{}
	return c
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdWriteCloser{
		writer: w,
	}, nil
}

type zstdWriteCloser struct {
	writer io.Writer    // Compressed data will be written here.
	buf    bytes.Buffer // Buffer uncompressed data here, compress on Close.
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	// prefer faster compression decompression rather than compression ratio
	compressed, err := zstd.CompressLevel(nil, z.buf.Bytes(), 3)
	if err != nil {
		return err
	}
	_, err = z.writer.Write(compressed)
	return err
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Use Datadog's API to decompress data
	uncompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}

func (c *compressor) Name() string {
	return Name
}
