// Package batch implements a concurrent record batch builder.
package batch

import (
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient/producer"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/record"
)

// Builder for record batches. Make sure to set public field values before calling Start. Do not
// change them after calling Start. Safe for concurrent use. The builder is called "sequential"
// because even tough it runs multiple workers, workload is distributed sequentially: first batch to
// worker 1, second to worker 2, etc (as opposed: all workers take records from input at the same
// time and then all flush at the same time with a thundering herd).
type SequentialBuilder struct {
	// Compressor must be safe for concurrent use
	Compressor batch.Compressor
	// Each batch will have at least this many records. There is no "max": user can send slices
	// or any size on the input channel. It is up to the user to enforce sanity of input slices.
	MinRecords int
	// Incoming records are collected into sets, the size of which (the number of records in
	// each set) is determined by MinRecords. Each of these sets of records must be built into
	// a batch: records need to be serialized into wire format and then compressed. Each set of
	// records is precessed by a worker and results in a single producer.Batch. NumWorkers
	// determines the number of workers doing the serialization and compression. This is most
	// likely the most expensive part of the whole pipeline (especially when compression is
	// enabled) so set this accordingly (but doesn't make sense for it to be more than the
	// number of available cores). Must be >0
	NumWorkers int
	//
	in   <-chan []*libkafka.Record
	out  chan *producer.Batch
	sets chan []*libkafka.Record
	wg   sync.WaitGroup
}

func (b *SequentialBuilder) collectLoop() {
	var set []*record.Record
	for r := range b.in {
		set = append(set, r...)
		if len(set) >= b.MinRecords {
			b.sets <- set
			set = nil
		}
	}
	if len(set) > 0 {
		b.sets <- set
	}
	close(b.sets)
}

func (b *SequentialBuilder) buildLoop() {
	for records := range b.sets {
		builder := batch.NewBuilder(time.Now().UTC())
		builder.Add(records...)
		t := time.Now().UTC()
		batch, err := builder.Build(time.Now().UTC())
		producerBatch := &producer.Batch{
			Batch:         batch,
			BuildError:    err,
			BuildBegin:    t,
			BuildComplete: time.Now().UTC(),
		}
		if err == nil {
			producerBatch.UncompressedBytes = producerBatch.BatchLengthBytes
			producerBatch.CompressError = batch.Compress(b.Compressor)
			producerBatch.CompressComplete = time.Now().UTC()
		}
		b.out <- producerBatch
	}
}

// Start building batches. Returns channel on which workers return completed batches. When input
// channel is closed the workers drain it, output any remaining batches (even if smaller than
// MinRecords), exit, and the output channel is closed. It is more efficient to send multiple
// records at a time on the input channel but the size of the input slices is independent of
// MinRecords (and so open to abuse: you could send a huge input slice; up to you to ensure slice
// sanity). Empty slices passed on input are ignored. If any record is nil, this will result in
// batch.ErrNilSlice when the batch is built. You should call Start only once.
func (b *SequentialBuilder) Start(input <-chan []*libkafka.Record) <-chan *producer.Batch {
	b.in = input
	b.sets = make(chan []*libkafka.Record, b.NumWorkers)
	go b.collectLoop()
	b.out = make(chan *producer.Batch, b.NumWorkers)
	for i := 0; i < b.NumWorkers; i++ {
		b.wg.Add(1)
		go func() {
			b.buildLoop()
			b.wg.Done()
		}()
	}
	go func() {
		b.wg.Wait()
		close(b.out)
	}()
	return b.out
}
