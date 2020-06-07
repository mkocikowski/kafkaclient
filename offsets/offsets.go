package offsets

import (
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka/client"
)

type DumbOffsetsManager struct {
	Bootstrap string
	GroupId   string
	Retention time.Duration
	client    *client.GroupClient
	sync.Mutex
}

func (c *DumbOffsetsManager) init() {
	c.Lock()
	defer c.Unlock()
	if c.client != nil {
		return
	}
	c.client = &client.GroupClient{
		Bootstrap: c.Bootstrap,
		GroupId:   c.GroupId,
	}
}

func (c *DumbOffsetsManager) Fetch(topic string, partition int32) (int64, error) {
	c.init() // idempotent
	// if partition does not exist or there are no offsets commited
	// for it there is no error and returned offset is -1. error
	// here if for things like connection problems, error response
	// codes from kafka, etc
	offset, err := c.client.FetchOffset(topic, partition)
	if err != nil {
		err = kafkaclient.Errorf("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return offset, err
}

func (c DumbOffsetsManager) Commit(topic string, partition int32, offset int64) error {
	c.init() // idempotent
	err := c.client.CommitOffset(topic, partition, offset, -1)
	if err != nil {
		err = kafkaclient.Errorf("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return err
}
