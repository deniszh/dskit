package s2

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/encoding"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS2(t *testing.T) {
	c := newCompressor()
	assert.Equal(t, "s2", c.Name())

	tests := []struct {
		test  string
		input string
	}{
		{"empty", ""},
		{"short", "hello world"},
		{"long", strings.Repeat("123456789", 1024)},
	}
	for _, test := range tests {
		t.Run(test.test, func(t *testing.T) {
			var buf bytes.Buffer
			// Compress
			w, err := c.Compress(&buf)
			require.NoError(t, err)
			n, err := w.Write([]byte(test.input))
			require.NoError(t, err)
			assert.Len(t, test.input, n)
			err = w.Close()
			require.NoError(t, err)
			// Decompress
			r, err := c.Decompress(&buf)
			require.NoError(t, err)
			out, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, test.input, string(out))
		})
	}
}

func BenchmarkS2SCompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, _ := c.Compress(io.Discard)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}

func BenchmarkS2Decompress(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	c := newCompressor()
	var buf bytes.Buffer
	w, _ := c.Compress(&buf)
	_, _ = w.Write(data)
	reader := bytes.NewReader(buf.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := c.Decompress(reader)
		_, _ = io.ReadAll(r)
		_, _ = reader.Seek(0, io.SeekStart)
	}
}

func BenchmarkS2GrpcCompressionPerf(b *testing.B) {
	data := []byte(strings.Repeat("123456789", 1024))
	grpcc := encoding.GetCompressor(Name)

	// Reset the timer to exclude setup time from the measurements
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			var buf bytes.Buffer
			writer, _ := grpcc.Compress(&buf)
			_, _ = writer.Write(data)
			_ = writer.Close()

			compressedData := buf.Bytes()
			reader, _ := grpcc.Decompress(bytes.NewReader(compressedData))
			var result bytes.Buffer
			_, _ = result.ReadFrom(reader)
		}
	}
}
