package consumer

import (
	"log"
	"sync"

	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/errors"
)

type Static struct {
	// Kafka bootstrap either host:port or SRV
	Bootstrap      string
	Topic          string
	NumWorkers     int
	HandleResponse ResponseHandlerFunc
	MinBytes       int32
	MaxBytes       int32
	MaxWaitTimeMs  int32
	//
	//fetchers map[int]*fetcher.PartitionFetcher
	fetchers map[int]fetcher.Fetcher
	next     chan int
	out      chan *Exchange
	done     chan struct{}
	wg       sync.WaitGroup
}

type ResponseHandlerFunc func(fetcher.Seeker, *Exchange)

func DefaultHandleFetchResponse(s fetcher.Seeker, e *Exchange) {
	if e.RequestError != nil {
		// TODO
		return
	}
	if e.ErrorCode == errors.OFFSET_OUT_OF_RANGE {
		if err := s.Seek(fetcher.MessageNewest); err != nil {
			// TODO
		}
		return
	}
	offset := e.InitialOffset
	for _, batch := range e.Batches {
		if batch.Error != nil {
			log.Printf("error parsing batch: %v", batch.Error)
			continue
		}
		offset = batch.LastOffset() + 1
		// if the last batch fail it will be retried next time (offset
		// will not be advanced past it). if a batch "in the middle"
		// fails it will be skipped (offset will be advanced past it).
	}
	s.SetOffset(offset)
	e.FinalOffset = offset
}

func (c *Static) consume() *Exchange {
	partition := <-c.next
	defer func() { c.next <- partition }()
	f := c.fetchers[partition]
	e := &Exchange{InitialOffset: f.Offset()}
	e.parseFetchResponse(f.Fetch())
	c.HandleResponse(f, e)
	return e
}

func (c *Static) run() {
	for {
		select {
		case <-c.done:
			return
		default:
		}
		c.out <- c.consume()
	}
}

func (c *Static) Start(partitionOffsets map[int32]int64) (<-chan *Exchange, error) {
	c.fetchers = make(map[int]fetcher.Fetcher)
	c.next = make(chan int, len(partitionOffsets))
	for partition, offset := range partitionOffsets {
		p := int(partition)
		f := &fetcher.PartitionFetcher{
			PartitionClient: client.PartitionClient{
				Bootstrap: c.Bootstrap,
				Topic:     c.Topic,
				Partition: partition,
			},
			MinBytes:      c.MinBytes,
			MaxBytes:      c.MaxBytes,
			MaxWaitTimeMs: c.MaxWaitTimeMs,
		}
		f.SetOffset(offset)
		c.fetchers[p] = f
		//
		c.next <- p
	}
	c.done = make(chan struct{})
	c.out = make(chan *Exchange)
	for i := 0; i < c.NumWorkers; i++ {
		c.wg.Add(1)
		go func() {
			c.run()
			c.wg.Done()
		}()
	}
	go func() {
		c.wg.Wait()
		close(c.out)
	}()
	return c.out, nil
}

func (p *Static) Stop() {
	close(p.done)
}

func (p *Static) Wait() {
	p.wg.Wait()
}
