package compression

import (
	"bytes"

	"github.com/DataDog/zstd"
	"github.com/mkocikowski/libkafka/compression"
	"github.com/pierrec/lz4"
)

type Lz4 struct{}

/*
func (c *Lz4) Compress(data []byte) ([]byte, error) {
	// https://github.com/pierrec/lz4/blob/master/example_test.go#L35-L48
	buf := make([]byte, len(data))
	ht := make([]int, 64<<10) // buffer for the compression table
	n, err := lz4.CompressBlock(data, buf, ht)
	if err != nil {
		return nil, err
	}
	if n >= len(data) {
		return nil, fmt.Errorf("data is not compressible")
	}
	buf = buf[:n] // compressed data
	return buf, nil
}
*/

func (c *Lz4) Compress(src []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := lz4.NewWriter(buf)
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Lz4) Type() int16 {
	return compression.Lz4
}

type Zstd struct {
	Level int
}

func (c *Zstd) Compress(src []byte) ([]byte, error) {
	return zstd.CompressLevel(nil, src, c.Level)
}

func (c *Zstd) Decompress(src []byte) ([]byte, error) {
	return zstd.Decompress(nil, src)
}

func (c *Zstd) Type() int16 {
	return compression.Zstd
}

type None struct{}

func (c *None) Compress(src []byte) ([]byte, error) {
	return src, nil
}

func (c *None) Decompress(src []byte) ([]byte, error) {
	return src, nil
}

func (c *None) Type() int16 {
	return compression.None
}
