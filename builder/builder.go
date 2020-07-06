// Package builder implements a partitioning record batch builder.
package builder

import (
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient/producer"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
)

type Record interface {
	Key() []byte
	Value() []byte
}

type Partitioner interface {
	// Partition must be safe for concurrent use. Nil is a valid value for
	// the key. Behavior for when number of partitions set with
	// SetNumPartitions <1 is undefined.
	Partition(key []byte) int32
	// SetNumPartitions does not need to be safe for concurrent use. The
	// intention is that it be called once as part of builder setup.
	// Behavior on subsequent calls, or on calls while partitioner is being
	// used, is undefined. Number of partitions should be >0. If <1
	// behavior is undefined.
	SetNumPartitions(int32)
}

// Builder for record batches. Make sure to set public field values before
// calling Start. Do not change them after calling Start. Safe for concurrent
// use.
//
// The builder collects records from its input channel. It groups (partitions)
// incoming records. When a group (set of records for given partition) reaches
// thresholds defined by MinRecords and MinUncompressedBytes, it is sent to a
// worker that marshals the records into a batch and compresses the batch.
type SequentialBuilder struct {
	// Compressor must be safe for concurrent use
	Compressor batch.Compressor
	// Each batch will have at least this many records. There is no "max":
	// user can send slices or any size on the input channel. It is up to
	// the user to enforce sanity of input slices.
	MinRecords int
	// Each batch will have uncompressed payload (sum of uncompressed
	// record values) of at least this many bytes. Combined with MinRecords
	// (both have to be true) this determines when to "flush".
	MinUncompressedBytes int
	// Incoming records are collected into sets, the size of which (the
	// number of records in each set) is determined by MinRecords and
	// MinUncompressedBytes. Each of these sets of records must be built
	// into a batch: records need to be serialized into wire format and
	// then compressed.
	//
	// Each set of records is processed by a worker and results in a single
	// producer.Batch. NumWorkers determines the number of workers doing
	// the serialization and compression. This is most likely the most
	// expensive part of the whole pipeline (especially when compression is
	// enabled) so set this accordingly (but doesn't make sense for it to
	// be more than the number of available cores). Must be >0
	NumWorkers int
	// Partitioner to use. Max number of "in flight" batches will be equal
	// to number of partitions plus NumWorkers. See NopPartitioner.
	Partitioner Partitioner
	//
	in        <-chan []Record
	flush     chan time.Duration
	out       chan *producer.Batch
	collected chan *buffer
	wg        sync.WaitGroup
}

type buffer struct {
	records   []*libkafka.Record
	bytes     int
	partition int32
	started   time.Time
	enqueued  time.Time
}

func (b *SequentialBuilder) collectLoop() {
	buffers := make(map[int32]*buffer)
LOOP:
	for {
		select {
		case d := <-b.flush:
			for partition, buf := range buffers {
				if time.Since(buf.started) > d {
					buf.enqueued = time.Now()
					b.collected <- buf
					delete(buffers, partition)
				}
			}
			continue LOOP
		case records, ok := <-b.in:
			if !ok {
				break LOOP
			}
			for _, r := range records {
				if r == nil {
					continue
				}
				partition := b.Partitioner.Partition(r.Key())
				buf := buffers[partition]
				if buf == nil {
					buf = &buffer{
						partition: partition,
						started:   time.Now().UTC(),
					}
					buffers[partition] = buf
				}
				libkRecord := libkafka.NewRecord(r.Key(), r.Value())
				buf.records = append(buf.records, libkRecord)
				buf.bytes += len(libkRecord.Value)
				if len(buf.records) >= b.MinRecords &&
					buf.bytes >= b.MinUncompressedBytes {
					buf.enqueued = time.Now()
					b.collected <- buf
					delete(buffers, partition)
				}
			}
		}

	} // end LOOP
	for _, buf := range buffers {
		b.collected <- buf
	}
	close(b.collected)
}

func (b *SequentialBuilder) buildLoop() {
	for buf := range b.collected {
		t := time.Now().UTC()
		builder := batch.NewBuilder(time.Now().UTC())
		// TODO(optimize): ??? would it make sense to have builder.Set
		// as opposed to Add which copies the slice ?
		builder.Add(buf.records...)
		// builder.Build returns error if batch is empty or if there is
		// a nil record somewhere in the batch. The way the records are
		// collected in the SequentialBuilder collect loop ensures that
		// neither of these happens, so error SHOULD always be nil.
		batch, err := builder.Build(time.Now().UTC())
		producerBatch := &producer.Batch{
			Partition:     buf.partition,
			BuildError:    err,
			BuildEnqueued: buf.enqueued,
			BuildBegin:    t,
			BuildComplete: time.Now().UTC(),
		}
		if err == nil {
			producerBatch.UncompressedBytes = batch.BatchLengthBytes
			producerBatch.CompressError = batch.Compress(b.Compressor)
			producerBatch.CompressComplete = time.Now().UTC()
			producerBatch.Batch = *batch
		}
		b.out <- producerBatch
	}
}

// Start building batches. Returns channel on which workers return completed
// batches. The depth of that channel is equal to the number of workers. When
// input channel is closed the workers drain it, output any remaining batches
// (even if smaller than MinRecords), exit, and the output channel is closed.
// It is more efficient to send multiple records at a time on the input channel
// but the size of the input slices is independent of MinRecords (and so open
// to abuse: you could send a huge input slice; up to you to ensure slice
// sanity). Empty slices and nil records within slices are silently dropped,
// and so batches returned on the output channel SHOULD always be error free
// and have >0 records. You should call Start only once.
func (b *SequentialBuilder) Start(input <-chan []Record) <-chan *producer.Batch {
	b.in = input
	b.flush = make(chan time.Duration, 1)
	b.collected = make(chan *buffer, b.NumWorkers)
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

// Flush all batches "older" than d. If the builder is partitioned, will look
// through all partition batches; if the builder is not partitioned, there is
// only 1 batch. This takes precedence over MinRecords and
// MinUncompressedBytes. Passing d=0 immediately flushes all batches. Empty
// batches are never flushed. Does not block. If there is already a flush
// enqueued / in progress, flush returns immediately as nop.
func (b *SequentialBuilder) Flush(d time.Duration) {
	select {
	case b.flush <- d:
	default:
	}
	// i don't close b.flush at any point. this does not result in a leak,
	// and keeps me from needing to deal with panics when sending to closed
	// channel. if no one is receiving from the channel (collect loop has
	// exited) then this is a nop.
}
