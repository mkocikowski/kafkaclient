// Package builder implements a concurrent record batch builder.
package builder

import (
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/record"
)

// Builder for record batches. Make sure to set public field values before
// calling Start. Do not change them after calling Start. Safe for concurrent
// use.
type Builder struct {
	// Compressor must be safe for concurrent use
	Compressor batch.Compressor
	// Each batch will have at least this many records
	MinRecords int
	// Must be >0
	NumWorkers int
	//
	in  <-chan []*record.Record
	out chan *batch.Batch
	wg  sync.WaitGroup
}

func (b *Builder) buildSingleBatch() (*batch.Batch, error) {
	singleBatchBuilder := batch.NewBuilder(time.Now())
	for r := range b.in {
		singleBatchBuilder.Add(r...)
		if singleBatchBuilder.NumRecords() >= b.MinRecords {
			break
		}
	}
	return singleBatchBuilder.Build(time.Now(), b.Compressor)
}

func (b *Builder) run() {
	for {
		batch, err := b.buildSingleBatch()
		if err != nil {
			return
		}
		b.out <- batch
	}
}

// Start building batches. Spins up NumWorkers workers which all read records
// from the same input channel. Returns channel to which workers send completed
// batches. When input channel is closed the workers drain it, output any
// remaining batches (even if smaller than MinRecords), exit, and the output
// channel is closed. It is more efficient to send multiple records at a time
// on the input channel but the size of the input slices is independent of
// MinRecords. You should call Start only once.
func (b *Builder) Start(input <-chan []*record.Record) <-chan *batch.Batch {
	b.in = input
	b.out = make(chan *batch.Batch, b.NumWorkers)
	for i := 0; i < b.NumWorkers; i++ {
		b.wg.Add(1)
		go func() {
			b.run()
			b.wg.Done()
		}()
	}
	go func() {
		b.wg.Wait()
		close(b.out)
	}()
	return b.out
}
