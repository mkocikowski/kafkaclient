// Package batches implements a concurrent record batch builder.
package batches

import (
	"sync"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/record"
)

// Builder for record batches. Make sure to set public field values before
// calling Start. Do not change them after calling Start. Safe for concurrent
// use.
type SequentialBuilder struct {
	// Compressor must be safe for concurrent use
	Compressor batch.Compressor
	// Each batch will have at least this many records
	MinRecords int
	// Must be >0
	NumWorkers int
	//
	in   <-chan []*libkafka.Record
	sets chan []*libkafka.Record
	out  chan *libkafka.Batch
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
		builder := batch.NewBuilder(time.Now())
		builder.Add(records...)
		batch, err := builder.Build(time.Now(), b.Compressor)
		if err != nil {
			return
		}
		b.out <- batch
	}
}

// Start building batches. Returns channel to which workers send completed
// batches. When input channel is closed the workers drain it, output any
// remaining batches (even if smaller than MinRecords), exit, and the output
// channel is closed. It is more efficient to send multiple records at a time
// on the input channel but the size of the input slices is independent of
// MinRecords. You should call Start only once.
func (b *SequentialBuilder) Start(input <-chan []*libkafka.Record) <-chan *libkafka.Batch {
	b.in = input
	b.sets = make(chan []*libkafka.Record, b.NumWorkers)
	go b.collectLoop()
	b.out = make(chan *libkafka.Batch, b.NumWorkers)
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
