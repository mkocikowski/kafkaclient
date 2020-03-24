// Package producer impelments an asynchronous kafka producer.
package producer

import (
	"fmt"
	"sync"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/errors"
)

// Exchange struct records information on multiple errors/responses for a
// single record batch. Only success response is recorded. Errors are recorded
// in order. There will be at most Producer.NumAttempts errors. It is safe to
// alter Batch (for example: set Batch.MarshaledRecords to nil) if you do not
// intend to reuse it (which you could: you could put it back in the producer
// input).
type Exchange struct {
	Batch   *batch.Batch
	Success *producer.Response
	Errors  []error
}

// Producer sends record batches to Kafka. Make sure to set public field values
// before calling Start. Do not change them after calling Start. Safe for
// concurrent use.
type Producer struct {
	// Kafka bootstrap either host:port or SRV
	Bootstrap string
	Topic     string
	// Spin up this many workers. Each worker is synchronous. Each worker
	// processes one batch at a time trying to send it to a random
	// partition. On error the worker retries up to NumRetries each time
	// trying to send the batch to a different partition. Details are
	// returned in Exchange structs. After each error the underlying
	// connection to the kafka topic partition leader is closed, and
	// reopened on the next call to that leader. Because workers are
	// synchronous NumWorkers determines the maximum number of "in flight"
	// batches. It makes no sense to have more workers than partitions.
	// Setting NumWorkers=1 results in Producer being synchronous. Must be
	// >0.
	NumWorkers int
	// 1 means 1 initial attempt and no retries. 2 means 1 initial attempt
	// and 1 more attempt on error. Must be >0.
	NumAttempts int
	//
	producers map[int]*producer.PartitionProducer
	next      chan int
	in        <-chan *batch.Batch
	out       chan *Exchange
	wg        sync.WaitGroup
}

func (p *Producer) produce(e *Exchange) {
	partition := <-p.next
	defer func() { p.next <- partition }()
	partitionProducer := p.producers[partition]
	resp, err := partitionProducer.Produce(e.Batch)
	if err != nil {
		partitionProducer.Close()
		e.Errors = append(e.Errors, err)
		return
	}
	if resp.ErrorCode != errors.NONE {
		partitionProducer.Close()
		e.Errors = append(e.Errors, &errors.KafkaError{Code: resp.ErrorCode})
		return
	}
	e.Success = resp
}

func (p *Producer) run() {
	for b := range p.in {
		e := &Exchange{Batch: b}
		for i := 0; i < p.NumAttempts; i++ {
			p.produce(e)
			if e.Success != nil {
				break
			}
		}
		p.out <- e
	}
}

// Start sending batches to Kafka. When input channel is closed the workers
// drain it, send any remaining batches to kafka, output the final Exchanges,
// exit, and close the output channel. You should call Start only once.
func (p *Producer) Start(input <-chan *batch.Batch) (<-chan *Exchange, error) {
	leaders, err := client.PartitionLeaders(p.Bootstrap, p.Topic)
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
		}
		p.next <- int(partition)
	}
	p.in = input
	p.out = make(chan *Exchange)
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
