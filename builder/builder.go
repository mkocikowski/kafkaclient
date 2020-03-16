package builder

import (
	"log"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/record"
)

type Builder struct {
	Input      <-chan *record.Record
	MaxRecords int
	Compressor batch.Compressor
	//
	wg  sync.WaitGroup
	out chan *batch.Batch
}

func (b *Builder) run() {
	builder := batch.NewBuilder(time.Now())
	for r := range b.Input {
		builder.Add(r)
		if builder.NumRecords() == b.MaxRecords { // limit=0 means no limit
			x, err := builder.Build(time.Now(), b.Compressor)
			if err != nil {
				log.Println(err)
				return
			}
			b.out <- x
			builder = batch.NewBuilder(time.Now())
		}
	}
}

func (b *Builder) Start(numWorkers int) <-chan *batch.Batch {
	b.out = make(chan *batch.Batch)
	for i := 0; i < numWorkers; i++ {
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
