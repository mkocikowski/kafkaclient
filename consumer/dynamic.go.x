package consumer

import (
	"encoding/json"
	"log"

	"github.com/mkocikowski/kafkaclient/groups"
	"github.com/mkocikowski/kafkaclient/groups/assigners"
	"github.com/mkocikowski/libkafka/batch"
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
					batches, err := r.Success.Batches()
					if err != nil {
						log.Printf("%+v\n", r)
						continue
					}
					for _, b := range batches {
						c.out <- b
					}
				}
			}()
		}
	}()
	return c.out, nil
}
