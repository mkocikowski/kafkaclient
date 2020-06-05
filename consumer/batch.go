package consumer

import (
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/compression"
	"github.com/mkocikowski/libkafka/record"
)

func parseResponseBatch(b []byte) *Batch {
	responseBatch, err := batch.Unmarshal(b) // this is not so cheap TODO: lazy ?
	if err != nil {
		return &Batch{Error: kafkaclient.Errorf("error unmarshaling batch: %w", err)}
	}
	return &Batch{
		Batch:           *responseBatch,
		CompressedBytes: responseBatch.BatchLengthBytes,
	}
}

// Batch is the unit at which data it fetched from kafka. A successful fetch request will return one
// or more batches. Each batch, if unmarshaled successfully, will have one or more records in it.
type Batch struct {
	libkafka.Batch
	Topic           string
	Partition       int32
	Error           error
	CompressedBytes int32
}

var ErrCodecNotFound = kafkaclient.Errorf("codec not found")

// Decompress the batch. Decompressing a batch that is not compressed is a nop. Mutates the batch.
// If Batch.Error is not nil Decompress is a nop. Sets Batch.Error on error. Not safe for concurrent
// use.
func (b *Batch) Decompress(decompressors map[int16]batch.Decompressor) {
	if b.Error != nil {
		return
	}
	d := decompressors[b.Batch.CompressionType()]
	if d == nil {
		b.Error = ErrCodecNotFound
		return
	}
	if err := b.Batch.Decompress(d); err != nil {
		b.Error = err
	}
}

var ErrBatchCompressed = kafkaclient.Errorf("batch is compressed")

// Records retrieves individual records from the batch. Batch must be decompressed.
func (b *Batch) Records() ([]*record.Record, error) {
	if b.Error != nil {
		return nil, b.Error
	}
	if b.Batch.CompressionType() != compression.None {
		return nil, ErrBatchCompressed
	}
	recordsBytes := b.Batch.Records()
	records := make([]*record.Record, len(recordsBytes))
	for i, b := range recordsBytes {
		r, err := record.Unmarshal(b)
		if err != nil {
			return nil, err
		}
		records[i] = r
	}
	return records, nil
}

func (b *Batch) MaxTimestamp() time.Time {
	t := time.Unix(0, b.Batch.MaxTimestamp*int64(time.Millisecond))
	return t
}
