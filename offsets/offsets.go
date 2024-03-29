package offsets

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/mkocikowski/kafkaclient/errors"
	"github.com/mkocikowski/libkafka/client"
)

type DumbOffsetsManager struct {
	Bootstrap string
	TLS       *tls.Config
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
		TLS:       c.TLS,
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
		err = errors.Format("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return offset, err
}

// Commit makes a single CommitOffset api call for a single partition. See Fetch
// documentation for info on error handling.
func (c *DumbOffsetsManager) Commit(topic string, partition int32, offset int64) error {
	c.init() // idempotent
	err := c.client.CommitOffset(topic, partition, offset, -1)
	if err != nil {
		c.client.Close() // will reconnect on next call
		err = errors.Format("error for topic %s partition %d: %w",
			topic, partition, err)
	}
	return err
}

// CommitAll makes a single CommitOffsets api call for a set of partitions. See
// Fetch documentation for info on error handling.
func (c *DumbOffsetsManager) CommitAll(topic string, offsets map[int32]int64) error {
	c.init()
	err := c.client.CommitMultiplePartitionsOffsets(topic, offsets, -1)
	if err != nil {
		c.client.Close()
		err = errors.Format("error for topic %s and offsets map %v: %w",
			topic, offsets, err)
	}
	return err
}

func (c *DumbOffsetsManager) Close() error {
	// nop
	return nil
}
