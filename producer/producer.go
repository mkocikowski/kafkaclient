// Package producer impelments an asynchronous kafka producer.
package producer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/errors"
)

// Batch is the unit on which the producer operates. In addition to fields inherited from
// libkafka.Batch (related to the wire protocol), the producer Batch records the entire "life cycle"
// of a batch: from recording timings on batch production, errors, through multiple (possibly)
// Produce api calls.  Batches are created by builders (in the batch package) and are passed along
// to methods the mutate them recording additional information.
type Batch struct {
	*libkafka.Batch
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
	for _, e := range b.Exchanges {
		if e.Err() == nil {
			return true
		}
	}
	return false
}

func (b *Batch) String() string {
	c, _ := json.Marshal(b)
	return string(c)
}

// Exchange records information about a single Produce api call and response. A batch will have one
// or more exchanges attached to it.
type Exchange struct {
	Begin    time.Time
	Complete time.Time
	Response *producer.Response
	// This is populated if there was an error getting response from the broker. So things like
	// network timeouts, broker down, bad protocol version, etc. This being nil means that
	// response was successfuly read and parsed. But, response itself may contain an error code.
	Error error
}

var ErrNilResponse = fmt.Errorf("nil response from broker")

// Err returns an error if: Exchange.Error is not nil; Exchange.Error is nil but Exchange.Response
// is nil; Exchange.Error is nil and Exchange.Response.ErrorCode is not NONE.
func (e *Exchange) Err() error {
	if e.Error != nil {
		return e.Error
	}
	if e.Response == nil {
		return ErrNilResponse
	}
	if e.Response.ErrorCode != errors.NONE {
		return &errors.KafkaError{Code: e.Response.ErrorCode}
	}
	return nil
}

// Async producer sends record batches to Kafka. Make sure to set public field values before calling
// Start. Do not change them after calling Start. Safe for concurrent use.
type Async struct {
	// Kafka bootstrap either host:port or SRV
	Bootstrap string
	Topic     string
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
	Acks        int
	Timeout     time.Duration
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
	resp, err := partitionProducer.Produce(b.Batch)
	exchange := &Exchange{
		Begin:    t,
		Complete: time.Now().UTC(),
		Response: resp,
		Error:    err,
	}
	if exchange.Err() != nil {
		// not getting into the specifics of what the problem is. close the connection, it
		// will be reopened on the next produce attempt. the expense is not prohibitive if
		// this is a one-off (say NOT_LEADER_FOR_PARTITION) but if the problem is severe we
		// are hosed anyway
		partitionProducer.Close()
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
		}
		p.out <- b
	}
}

// Start sending batches to Kafka. Returns a channel on which all produced (success or no) batches
// are sent. You need to read from that channel or production will be blocked. When input channel is
// closed the workers drain it, send any remaining batches to kafka, output the final batches,
// exit, and close the output channel. You should call Start only once.
func (p *Async) Start(input <-chan *Batch) (<-chan *Batch, error) {
	leaders, err := client.GetPartitionLeaders(p.Bootstrap, p.Topic)
	if err != nil {
		return nil, err
	}
	if len(leaders) == 0 {
		return nil, fmt.Errorf("no leaders for topic %v", p.Topic)
	}
	p.producers = make(map[int]*producer.PartitionProducer)
	p.next = make(chan int, len(leaders))
	for partition, _ := range leaders {
		p.producers[int(partition)] = &producer.PartitionProducer{
			PartitionClient: client.PartitionClient{
				Bootstrap: p.Bootstrap,
				Topic:     p.Topic,
				Partition: partition,
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
