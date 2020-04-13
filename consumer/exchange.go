package consumer

import (
	"fmt"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/record"
)

type Batch struct {
	libkafka.Batch
	Topic     string
	Partition int32
	Error     error
}

func (b *Batch) Records(decompressors map[int16]batch.Decompressor) ([]*record.Record, error) {
	d := decompressors[b.Batch.CompressionType()]
	if d == nil {
		return nil, fmt.Errorf("no decompressor for type %d", b.Batch.CompressionType())
	}
	if err := b.Batch.Decompress(d); err != nil {
		return nil, err
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

type Exchange struct {
	fetcher.Response
	RequestError  error
	Batches       []*Batch
	InitialOffset int64
	FinalOffset   int64
}

func (e *Exchange) parseFetchResponse(r *fetcher.Response, err error) {
	if err != nil {
		e.RequestError = err
		return
	}
	e.Response = *r
	for _, b := range r.RecordSet.Batches() {
		batch, err := batch.Unmarshal(b)
		e.Batches = append(e.Batches, &Batch{
			Batch:     *batch,
			Topic:     r.Topic,
			Partition: r.Partition,
			Error:     err,
		})
	}
}
