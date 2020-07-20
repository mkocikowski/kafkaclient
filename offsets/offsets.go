package offsets

import (
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
)

type DumbOffsetsManager struct {
	Bootstrap  string
	GroupId    string
	Retention  time.Duration
	initClient func(bootstrap, group string) groupClient
	client     groupClient
	sync.Mutex
}

// groupClient represents interface of libkafka's GroupClient methods, and here
// mostly for testability
type groupClient interface {
	FetchOffset(topic string, partition int32) (int64, error)
	CommitOffset(topic string, partition int32, offset, retentionMs int64) error
}

func (c *DumbOffsetsManager) init() {
	c.Lock()
	defer c.Unlock()
	if c.client != nil {
		return
	}
	if c.initClient == nil {
		c.client = &client.GroupClient{
			Bootstrap: c.Bootstrap,
			GroupId:   c.GroupId,
		}
		return
	}
	c.client = c.initClient(c.Bootstrap, c.GroupId)
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

// Commit sends CommitOffset API request to Kafka, which sets offset for a
// specific partition. If the response of API call has NOT_COORDINATOR error
// code, request will be retried after reconnecting to Kafka. NOT_COORDINATOR
// error happens when coordinator for the current group was re-assigned by some
// reasons and client is trying to use "old/inactive". In this case client
// should re-discover new coordinator and continue communication.
func (c *DumbOffsetsManager) Commit(topic string, partition int32, offset int64) error {
	c.init() // idempotent
	err := c.client.CommitOffset(topic, partition, offset, -1)
	if err != nil {
		returnErr := kafkaclient.Errorf("error for topic %s partition %d: %w",
			topic, partition, err)

		libkafkaError, ok := err.(*libkafka.Error)
		if !ok {
			return returnErr
		}
		if libkafkaError.Code == libkafka.ERR_NOT_COORDINATOR {
			c.Lock()
			c.client = nil
			c.Unlock()
		}
		return returnErr
	}
	return nil
}
