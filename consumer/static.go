package consumer

import (
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

// ResponseHandlerFunc is a signature of function that handles (mostly) failure logic for exchanges,
// and the logic for advancing (and maybe committing) offsets. There are many failure scenarios and
// many ways to handle them; instead of getting into config hell, I decided to make the logic
// injectable, provide a default implementation (DefaultHandleFetchResponse), but then let users
// provide their own logic if they want to.
//
// The handler func gets a FetcherSeekerCloser (implemented by libkafka fetcher.PartitionFetcher),
// and the exchange (after the response has been parsed and batches have been unmarshaled). It is
// expected that exchange be mutated by the function.
//
// The response handler must advance the fetcher offset (Seeker.SetOffset) or every fetch call will
// read from the same offset. This is different from "commiting" offsets (which is storing them in
// kafka to survive client restarts).
type ResponseHandlerFunc func(FetcherSeekerCloser, *Exchange)

// Static consumer consumes from a static list of topic partitions.
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
	fetchers map[int]FetcherSeekerCloser
	next     chan int
	out      chan *Exchange
	done     chan struct{}
	wg       sync.WaitGroup
}

func (c *Static) consume() *Exchange {
	var partition int
	select {
	case partition = <-c.next:
	case <-c.done:
		return nil
	}
	defer func() { c.next <- partition }()
	f := c.fetchers[partition]
	e := &Exchange{
		Response: fetcher.Response{
			Topic:     c.Topic,
			Partition: int32(partition),
		},
		InitialOffset: f.Offset(),
		RequestBegin:  time.Now().UTC(),
	}
	e.parseResponse(f.Fetch())
	c.HandleResponse(f, e)
	return e
}

func (c *Static) run() {
	for {
		exchange := c.consume()
		if exchange == nil {
			return
		}
		c.out <- exchange
	}
}

// Start consuming. Pass in map[partition]offset. You must read exchanges from the returned channel
// or consumer will block. You should only call Start once.
func (c *Static) Start(partitionOffsets map[int32]int64) (<-chan *Exchange, error) {
	c.fetchers = make(map[int]FetcherSeekerCloser)
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

// Stop consuming. Any request-response currently in flight will continue. You should drain the
// output exchanges channel, which will be closed once it has been drained.
func (p *Static) Stop() {
	close(p.done)
}

// Wait for the consumer to fully stop. You need to drain the output exchanges channel.
func (p *Static) Wait() {
	p.wg.Wait()
}
