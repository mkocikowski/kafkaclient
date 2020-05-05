package consumer

import (
	"log"
	"sync"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/fetcher"
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

type ResponseHandlerFunc func(fetcher.SeekCloser, *Exchange)

func DefaultHandleFetchResponse(s fetcher.SeekCloser, e *Exchange) {
	if e.RequestError != nil {
		// connection has been closed in libkafka
		return
	}
	if e.ErrorCode == libkafka.ERR_OFFSET_OUT_OF_RANGE {
		if err := s.Seek(fetcher.MessageNewest); err != nil {
			s.Close()
		}
		return
	}
	if e.ErrorCode != libkafka.ERR_NONE {
		s.Close()
		return
	}
	offset := e.InitialOffset
	for _, batch := range e.Batches {
		if batch.Error != nil {
			// TODO: DO NOT log from library
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
	// topic and partition are not populated when there is a wire error so setting them here so
	// that errors can be analyzed better
	e.Topic = c.Topic
	e.Partition = int32(partition)
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
