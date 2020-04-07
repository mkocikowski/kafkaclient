package kafkaclient

import "github.com/mkocikowski/libkafka"

type Batch struct {
	libkafka.Batch
	Topic     string
	Partition int32
}
