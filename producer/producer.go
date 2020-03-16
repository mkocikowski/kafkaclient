package producer

import (
	"fmt"
	"sync"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/errors"
)

type Exchange struct {
	Batch    *batch.Batch
	Response *producer.Response
	Errors   []error
}

type Producer struct {
	Bootstrap string
	Topic     string
	Input     <-chan *batch.Batch
	//
	producers map[int]*producer.PartitionProducer
	next      chan int
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
	e.Response = resp
}

const maxRetries = 3

func (p *Producer) run() {
	for b := range p.Input {
		e := &Exchange{Batch: b}
		for i := 0; i < maxRetries; i++ {
			p.produce(e)
			if e.Response != nil {
				break
			}
		}
		p.out <- e
	}
}

func (p *Producer) Start(numWorkers int) (<-chan *Exchange, error) {
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
	p.out = make(chan *Exchange)
	for i := 0; i < numWorkers; i++ {
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
