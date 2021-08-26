// Package producer impelments an asynchronous kafka producer.
package producer

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient/errors"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
)

// Batch is the unit on which the producer operates. In addition to fields inherited from
// libkafka.Batch (related to the wire protocol), the producer Batch records the entire "life cycle"
// of a batch: from recording timings on batch production, errors, through multiple (possibly)
// Produce api calls.  Batches are created by builders (in the batch package) and are passed along
// to methods that mutate them recording additional information.
type Batch struct {
	libkafka.Batch
	// Topic is set by the producer. Batches received from the builder have
	// no topic set, producer sets it before attempting first exchange.
	Topic string
	// Partition to which the batch is intended to be produced. Special
	// value -1 means "next partition round robin". Zero value (0) means
	// "produce to partition 0". So be careful with that. To see the
	// partition to which the batch ended up actually being produced (or
	// attempted) see the Exchanges (.Response.Topic).
	Partition         int32
	BuildEnqueued     time.Time
	BuildBegin        time.Time
	BuildComplete     time.Time
	BuildError        error
	CompressComplete  time.Time
	CompressError     error
	UncompressedBytes int32       // batch size can't be more than MaxInt32
	ProducerError     error       // for example ErrNoProducerForPartition
	Exchanges         []*Exchange // each exchange records a Produce api call and response
}

// Produced returns true if the batch has been successfuly produced (built, sent, and acked by a
// broker).
func (b *Batch) Produced() bool {
	return len(b.Exchanges) > 0 && b.Exchanges[len(b.Exchanges)-1].Error == nil
}

func (b *Batch) String() string {
	c, _ := json.Marshal(b)
	return string(c)
}

// Exchange records information about a single Produce api call and response. A
// batch will have one or more exchanges attached to it. If Response.ErrorCode
// != libkafka.ERR_NONE then Error will be set to corresponding libkafka.Error.
type Exchange struct {
	// Enqueued records the time the producer started "looking" for a
	// worker to make the produce request. Delta between Enqueued and Begin
	// shows how long it took to get a worker. This delta can be reduced by
	// increasing the number of produce workers (unless all requests are to
	// produce to a single partition, in which case the number of produce
	// workers doesn't matter).
	Enqueued time.Time
	// Begin records the time the kafka produce call began. Delta between
	// that and Complete shows how long it took to send the request over
	// the wire and for kafka to respond to it. Large delta indicates
	// problems on kafka side.
	Begin    time.Time
	Complete time.Time
	Response *producer.Response
	// Error indicates that exchange failed. This could be a "low level"
	// error such as network, or it could be a libkafka.Error, which means
	// response from the broker had ErrorCode other than ERR_NONE (0).
	Error error
}

// Async producer sends record batches to Kafka. Make sure to set public field values before calling
// Start. Do not change them after calling Start. Safe for concurrent use.
type Async struct {
	// Kafka bootstrap either host:port or SRV
	Bootstrap string
	TLS       *tls.Config
	Topic     string
	// Produce to these partitions.
	Partitions []int
	// Spin up this many workers. Each worker is synchronous. Each worker processes one batch at
	// a time trying to send it to a random partition. On error the worker retries up to
	// NumRetries each time trying to send the batch to a different partition. Details are
	// returned in Exchange structs. After each error the underlying connection to the kafka
	// topic partition leader is closed, and reopened on the next call to that leader. Because
	// workers are synchronous NumWorkers determines the maximum number of "in flight" batches.
	// It makes no sense to have more workers than partitions.  Setting NumWorkers=1 results in
	// Producer being synchronous. Must be >0.
	NumWorkers int
	// 1 means 1 initial attempt and no retries. 2 means 1 initial attempt and 1 more attempt on
	// error. Must be >0.
	NumAttempts int
	// Sleep this long between repeated produce calls for the same batch. This is a mixed
	// thing: if errors to produce are because partition leadership has moved, then it would
	// make sense to make next call immediately (because the leader would be found etc). But if
	// there is a problem with leadership / some other kind of slow down, then waiting for a bit
	// is good: keeps the producer from "needlessly" dropping the batch. This logic will become
	// even more complicated if data is partitioned (right now delivery is to random partition).
	// Anyway. Feel free to set to 0.
	SleepBetweenAttempts time.Duration
	// StrictPartitioning, when true, attempts to write each batch into the
	// partition which was set for it by the batch builder. This means that
	// if the partition is not available for some reason, the producer will
	// keep trying (up to NumAttempts) and then possibly lose data. If
	// StrictPartitioning is false, then after the first error the
	// partition for the batch will be set to -1 (meaning "first available"
	// round robin). If the batch builder does not partition (all batches
	// have partition set to -1) then this setting does nothing.
	StrictPartitioning bool
	// ConnMaxIdle is passed down to libkafa client.PartitionClient. See
	// documentation there. Should be less than connections.max.idle.ms
	// specified in broker config.
	ConnMaxIdle time.Duration
	// Acks required. 0 no acks, 1 leader only, -1 all ISRs (as specified
	// by min.insync.replicas).
	Acks int
	// Timeout to set for kafka produce api calls. This determines how long
	// the broker waits to get all required acks from replicas.
	Timeout time.Duration
	//
	producers map[int]*producer.PartitionProducer
	next      chan int
	in        <-chan *Batch
	out       chan *Batch
	wg        sync.WaitGroup
}

var ErrNilResponse = fmt.Errorf("nil response from broker")

// conveninence function to make produce call and parse + format errors
func produce(p *producer.PartitionProducer, b *libkafka.Batch) *Exchange {
	t := time.Now().UTC()
	resp, err := p.Produce(b)
	switch {
	case err != nil:
	case resp == nil:
		err = ErrNilResponse
	case resp.ErrorCode != libkafka.ERR_NONE:
		err = &libkafka.Error{Code: resp.ErrorCode}
	}
	if err != nil {
		err = errors.Format(
			"error producing topic %s partition %d: %w",
			p.Topic, p.Partition, err)
	}
	return &Exchange{
		Begin:    t,
		Complete: time.Now().UTC(),
		Response: resp,
		Error:    err,
	}
}

func (p *Async) produce(b *Batch) {
	t := time.Now().UTC()
	var partition int
	for {
		// there is one producer per partition, and all workers share
		// the producer pool. "next" acts as a semaphore, ensuring only
		// one worker is producing to a given partition at a time. so
		// here we claim a partition. if the required partition is "-1"
		// then we pick the first available partition. otherwise, if
		// specific partition is requested, we keep claiming and
		// releasing partitions until we get the one we want. relative
		// inefficiency of this approach doesn't matter given realistic
		// number of partitions and batch production rate. we guard
		// against infinite loop (where the requested partition is not
		// in the list of partitions) in the calling function, which
		// checks if the producer is configured for the requested
		// partition.
		partition = <-p.next
		if b.Partition == -1 || b.Partition == int32(partition) {
			break
		}
		p.next <- partition
	}
	defer func() { p.next <- partition }()
	partitionProducer := p.producers[partition]
	exchange := produce(partitionProducer, &b.Batch)
	exchange.Enqueued = t
	if exchange.Error != nil {
		partitionProducer.Close()
		// if partitioning is strict, then all subsequent calls will
		// attempt to write to the original partition. this may fail if
		// the partition is for some reason not available, and result
		// in data loss. if partitioning is not strict, then following
		// the first failure batch partition is set to -1 meaning
		// "first available" partition
		if !p.StrictPartitioning {
			b.Partition = -1
		}
	}
	b.Exchanges = append(b.Exchanges, exchange)
}

// ErrNoProducerForPartition is set for batch.ProducerError when the producer
// is not configured to produce to partition specified in batch.Partition. This
// does not necessarily mean that the partition does not exist, just that the
// producer was not configured to write to it.
var ErrNoProducerForPartition = errors.New("no producer for partition")

func (p *Async) run() {
	for b := range p.in {
		b.Topic = p.Topic
		// it is possible that a batch is sent from the batch builder
		// specifying partition for which there is no producer (this
		// SHOULD not happen, but could). check for that here.
		if _, ok := p.producers[int(b.Partition)]; b.Partition != -1 && !ok {
			b.ProducerError = ErrNoProducerForPartition
		}
		// if there is any error building the batch, or there is no
		// producer for the partition, don't even attempt an exchange
		if b.BuildError != nil || b.CompressError != nil || b.ProducerError != nil {
			p.out <- b
			continue
		}
		for i := 0; i < p.NumAttempts; i++ {
			p.produce(b)
			if b.Produced() {
				break
			}
			time.Sleep(p.SleepBetweenAttempts)
		}
		p.out <- b
	}
}

// Start sending batches to Kafka. Returns a channel on which all produced (success or no) batches
// are sent. You need to read from that channel or production will be blocked. When input channel is
// closed the workers drain it, send any remaining batches to kafka, output the final batches,
// exit, and close the output channel. You should call Start only once.
func (p *Async) Start(input <-chan *Batch) (<-chan *Batch, error) {
	if len(p.Partitions) == 0 {
		return nil, fmt.Errorf("no partitions")
	}
	p.producers = make(map[int]*producer.PartitionProducer)
	p.next = make(chan int, len(p.Partitions))
	for _, partition := range p.Partitions {
		p.producers[int(partition)] = &producer.PartitionProducer{
			PartitionClient: client.PartitionClient{
				Bootstrap:   p.Bootstrap,
				TLS:         p.TLS,
				Topic:       p.Topic,
				Partition:   int32(partition),
				ConnMaxIdle: p.ConnMaxIdle,
			},
			Acks:      int16(p.Acks),
			TimeoutMs: int32(p.Timeout / time.Millisecond),
		}
		p.next <- int(partition)
	}
	p.in = input
	p.out = make(chan *Batch)
	for i := 0; i < p.NumWorkers; i++ {
		p.wg.Add(1)
		go func() {
			p.run()
			p.wg.Done()
		}()
	}
	go func() {
		p.wg.Wait()
		close(p.out)
	}()
	return p.out, nil
}

// Wait until all outstanding batches have been produced and the producer has cleanly shut down.
// Calling Wait before Start is a nop.
func (p *Async) Wait() {
	p.wg.Wait()
}
