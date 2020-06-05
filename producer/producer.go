// Package producer impelments an asynchronous kafka producer.
package producer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient"
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
	Topic             string
	Partition         int32
	BuildBegin        time.Time
	BuildComplete     time.Time
	BuildError        error
	CompressComplete  time.Time
	CompressError     error
	UncompressedBytes int32       // batch size can't be more than MaxInt32
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

var ErrNilResponse = fmt.Errorf("nil response from broker")

// Munge through the various produce failure modes and come up with "error / no error" for this
// particular exchange. There could be a "low level" error (network etc), there could be nil or
// incomplete response response, and there could could be complete response that carries a kafka
// error code. Combine all these into single Exchange.Error.
func parseResponse(begin time.Time, resp *producer.Response, err error) *Exchange {
	switch {
	case err != nil:
	case resp == nil:
		err = ErrNilResponse
	case resp.ErrorCode != libkafka.ERR_NONE:
		err = &libkafka.Error{Code: resp.ErrorCode}
	}
	return &Exchange{
		Begin:    begin,
		Complete: time.Now().UTC(),
		Response: resp,
		Error:    err,
	}
}

// Exchange records information about a single Produce api call and response. A
// batch will have one or more exchanges attached to it. If Response.ErrorCode
// != libkafka.ERR_NONE then Error will be set to corresponding libkafka.Error.
type Exchange struct {
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
	Acks                 int
	Timeout              time.Duration
	//
	producers map[int]*producer.PartitionProducer
	next      chan int
	in        <-chan *Batch
	out       chan *Batch
	wg        sync.WaitGroup
}

func (p *Async) produce(b *Batch) {
	partition := <-p.next // TODO: time how long the wait is here
	defer func() { p.next <- partition }()
	t := time.Now().UTC()
	partitionProducer := p.producers[partition]
	resp, err := partitionProducer.Produce(&b.Batch)
	exchange := parseResponse(t, resp, err)
	if exchange.Error != nil {
		partitionProducer.Close()
		// wrap error in kafkaclient.Error so that it is serialized correctly
		exchange.Error = kafkaclient.Errorf("error producing to partition %d: %w", partition, exchange.Error)
	}
	b.Exchanges = append(b.Exchanges, exchange)
}

func (p *Async) run() {
	for b := range p.in {
		if (b.BuildError != nil) || (b.CompressError != nil) {
			p.out <- b // don't even attempt to send
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
	for partition, _ := range p.Partitions {
		p.producers[int(partition)] = &producer.PartitionProducer{
			PartitionClient: client.PartitionClient{
				Bootstrap: p.Bootstrap,
				Topic:     p.Topic,
				Partition: int32(partition),
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
