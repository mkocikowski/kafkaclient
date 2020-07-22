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

// Fetch makes a single FetchOffset api call. If there is no active connection
// to the group coordinator, it will first look up the coordinator and connect
// to it (or return en error if unable to do so). Once there is an open
// connection to the group coordinator, if there is an error making the request
// or there is an error response (error response code from kafka), the
// connection to the group coordinator will be closed, and error will be
// returned. There is no retry logic, it is up to the user (connection will be
// automatically re-opened on the next call).
func (c *DumbOffsetsManager) Fetch(topic string, partition int32) (int64, error) {
	c.init() // idempotent
	// if partition does not exist or there are no offsets commited
	// for it there is no error and returned offset is -1. error
	// here if for things like connection problems, error response
	// codes from kafka, etc
	offset, err := c.client.FetchOffset(topic, partition)
	if err != nil {
		c.client.Close() // will reconnect on next call
		err = kafkaclient.Errorf("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return offset, err
}

// Commit makes a single CommitOffset api call. See Fetch documentation for
// info on error handling.
func (c *DumbOffsetsManager) Commit(topic string, partition int32, offset int64) error {
	c.init() // idempotent
	err := c.client.CommitOffset(topic, partition, offset, -1)
	if err != nil {
		c.client.Close() // will reconnect on next call
		err = kafkaclient.Errorf("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return err
}

func (c *DumbOffsetsManager) Close() error {
	// nop
	return nil
}
