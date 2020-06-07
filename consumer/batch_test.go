package consumer

import (
	"encoding/base64"
	"testing"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/compression"
)

func TestUnitBatchGetRecords(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{RecordSet: recordSet}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	b := e.Batches[1]
	if b.CompressedBytes != 75 {
		t.Fatal(b.CompressedBytes)
	}
	records, err := b.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("%+v", records)
	}
	r := records[1]
	if s := string(r.Value); s != "banana" {
		t.Fatal(s)
	}
}

// expected behavior is that if the batch already has an error, then calling
// Records will return that same error
func TestUnitBatchGetRecordsError(t *testing.T) {
	b := &Batch{Error: ErrCodecNotFound}
	_, err := b.Records()
	if err != ErrCodecNotFound {
		t.Fatal(err)
	}
}

type mockGzip struct{}

func (*mockGzip) Compress(b []byte) ([]byte, error)   { return b, nil }
func (*mockGzip) Decompress(b []byte) ([]byte, error) { return b, nil }
func (*mockGzip) Type() int16                         { return compression.Gzip }

func TestUnitBatchDecompress(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{RecordSet: recordSet}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	b := e.Batches[1]
	if err := b.Compress(&mockGzip{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Records(); err != ErrBatchCompressed {
		t.Fatal(err)
	}
	b.Decompress(nil)
	if b.Error != ErrCodecNotFound {
		t.Fatal(b.Error)
	}
	b.Error = nil // need to reset otherwise Decode is nop
	b.Decompress(map[int16]batch.Decompressor{compression.Gzip: &mockGzip{}})
	records, err := b.Records()
	if err != nil {
		t.Fatal(err)
	}
	if s := string(records[1].Value); s != "banana" {
		t.Fatal(s)
	}
}
