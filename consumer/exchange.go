package consumer

import (
	"fmt"
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api/Metadata"
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
	Leader        *Metadata.Broker
	RequestBegin  time.Time
	ResponseEnd   time.Time
	RequestError  error
	InitialOffset int64
	FinalOffset   int64
	Batches       []*Batch
}

func parseResponseBatch(b []byte) *Batch {
	responseBatch, err := batch.Unmarshal(b)
	if err != nil {
		return &Batch{Error: kafkaclient.Errorf("error unmarshaling batch: %w", err)}
	}
	return &Batch{Batch: *responseBatch}
}

func (e *Exchange) parseFetchResponse(r *fetcher.Response, err error) {
	if err != nil {
		e.RequestError = kafkaclient.Errorf("%w", err)
		return
	}
	e.Response = *r
	for _, b := range r.RecordSet.Batches() {
		batch := parseResponseBatch(b)
		batch.Topic, batch.Partition = r.Topic, r.Partition
		e.Batches = append(e.Batches, batch)
	}
}
