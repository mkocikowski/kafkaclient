package consumer

import (
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/compression"
	"github.com/mkocikowski/libkafka/record"
)

// Exchange carries all information relevant to a single "fetch" request-response. This is what the
// consumer outputs to the user.
type Exchange struct {
	fetcher.Response
	RequestBegin time.Time
	ResponseEnd  time.Time
	// RequestError records "low level" errors. Things like network timeouts, unexpected EOF
	// reading response, etc. Even when RequestError is nil, the exchange may have failed or
	// partially failed: the fetcher.ResponseError code may be other than libkafka.ERR_NONE,
	// which shows that even though request-response completed successfuly, Kafka returned an
	// error code. Or, individual batches may have errors (which counts as "partial" failure).
	RequestError error
	// Offset of the fetch request
	InitialOffset int64
	// Offset of the last record in the batch. The initial offset of the next fetch should be
	// the FinalOffset+1 (this is handled by the ResponseHandlerFunc).
	FinalOffset int64
	// Batches are unmarshaled from fetcher.Response.RecordSet (see its godoc for additional
	// details). The unmarshaling is done by the consumer. A successful response will have one
	// or more batches. If there is an error unmarshaling a batch (such as when the CRC doesn't
	// match) then batch's Error will be other than nil, but the batch will still be appended to
	// this slice.
	Batches []*Batch
}

var ErrNilResponse = kafkaclient.Errorf("nil response from broker")

func (e *Exchange) parseResponse(resp *fetcher.Response, err error) {
	e.ResponseEnd = time.Now().UTC()
	if err != nil {
		e.RequestError = kafkaclient.Errorf("%w", err)
		return
	}
	if resp == nil {
		e.RequestError = ErrNilResponse
		return
	}
	e.Response = *resp
	if resp.RecordSet != nil {
		for _, b := range resp.RecordSet.Batches() { // this is cheap
			batch := parseResponseBatch(b)
			// batches will, at some point, likely move around on their own, so
			// important we keep topic partition information with them
			batch.Topic, batch.Partition = resp.Topic, resp.Partition
			e.Batches = append(e.Batches, batch)
		}
	}
}

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
