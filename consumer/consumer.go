package consumer

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/mkocikowski/kafkaclient/groups"
	"github.com/mkocikowski/kafkaclient/groups/assigners"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/consumer"
	"github.com/mkocikowski/libkafka/errors"
	"github.com/mkocikowski/libkafka/record"
)

type Dynamic struct {
	Bootstrap string
	Topic     string
	GroupId   string
	//
	manager    *groups.GroupMembershipManager
	consumer   *Static
	partitions chan []int32
	out        chan *batch.Batch
}

func (c *Dynamic) Start() (<-chan *batch.Batch, error) {
	c.manager = &groups.GroupMembershipManager{
		Bootstrap: c.Bootstrap,
		GroupId:   c.GroupId,
		Assigner: &assigners.RandomPartition{
			Bootstrap: c.Bootstrap,
			Topic:     c.Topic,
		},
	}
	assignments := c.manager.Start()
	c.partitions = make(chan []int32, 1)
	go func() {
		for a := range assignments {
			var p []int32
			if err := json.Unmarshal(a, &p); err != nil {
				log.Println(err)
			}
			log.Printf("%+v\n", p)
			c.partitions <- p
		}
	}()
	c.out = make(chan *batch.Batch, 1)
	go func() {
		for p := range c.partitions {
			log.Println("xxxxxxxxxxxxxx", p)
			if c.consumer != nil {
				log.Println("stopping consumer...")
				c.consumer.Stop()
				c.consumer.Wait()
				log.Println("consumer stopped")
			}
			m := map[int32]int64{}
			for _, n := range p {
				m[n] = 0
			}
			c.consumer = &Static{
				Bootstrap:  c.Bootstrap,
				Topic:      c.Topic,
				NumWorkers: 1,
			}
			responses, err := c.consumer.Start(m)
			if err != nil {
				log.Fatal(err)
			}
			go func() {
				for r := range responses {
					if r.Error != nil {
						log.Printf("%+v\n", r)
						continue
					}
					for _, b := range r.Success.RecordBatches {
						c.out <- b
					}
				}
			}()
		}
	}()
	return c.out, nil
}

type Response struct {
	Success *consumer.Response
	Error   error
}

func (r *Response) Batches() []*batch.Batch {
	if r.Success == nil {
		return nil
	}
	return r.Success.RecordBatches
}

func recordsFromBatch(b *batch.Batch, d batch.Decompressor) ([]*record.Record, error) {
	marshaledRecords, err := b.Records(d)
	if err != nil {
		return nil, err
	}
	records := make([]*record.Record, len(marshaledRecords))
	for i, marshaledRecord := range marshaledRecords {
		r, err := record.Unmarshal(marshaledRecord)
		if err != nil {
			return nil, err
		}
		records[i] = r
	}
	return records, nil
}

func (r *Response) Records(d batch.Decompressor) ([]*record.Record, error) {
	var records []*record.Record
	for _, b := range r.Batches() {
		r, err := recordsFromBatch(b, d)
		if err != nil {
			return nil, err
		}
		records = append(records, r...)
	}
	return records, nil
}

type Static struct {
	// Kafka bootstrap either host:port or SRV
	Bootstrap  string
	Topic      string
	NumWorkers int
	//
	consumers map[int]*consumer.PartitionConsumer
	next      chan int
	out       chan *Response
	done      chan struct{}
	wg        sync.WaitGroup
}

func (c *Static) fetch() *Response {
	partition := <-c.next
	defer func() { c.next <- partition }()
	partitionConsumer := c.consumers[partition]
	resp, err := partitionConsumer.Fetch()
	if err != nil {
		return &Response{Error: err}
	}
	if resp.ErrorCode != errors.NONE {
		if resp.ErrorCode == errors.OFFSET_OUT_OF_RANGE {
			partitionConsumer.Seek(consumer.MessageNewest)
		}
		return &Response{Error: &errors.KafkaError{Code: resp.ErrorCode}}
	}
	return &Response{Success: resp}
}

func (c *Static) run() {
	for {
		select {
		case <-c.done:
			return
		default:
		}
		c.out <- c.fetch()
	}
}

func (c *Static) Start(partitionOffsets map[int32]int64) (<-chan *Response, error) {
	c.consumers = make(map[int]*consumer.PartitionConsumer)
	c.next = make(chan int, len(partitionOffsets))
	for partition, offset := range partitionOffsets {
		c.consumers[int(partition)] = &consumer.PartitionConsumer{
			PartitionClient: client.PartitionClient{
				Bootstrap: c.Bootstrap,
				Topic:     c.Topic,
				Partition: partition,
			},
			Offset: offset,
		}
		c.next <- int(partition)
	}
	c.done = make(chan struct{})
	c.out = make(chan *Response)
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
