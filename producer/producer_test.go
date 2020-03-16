package producer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"code.cfops.it/data/kafkaclient/compression"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const bootstrap = "localhost:9092"

func createTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CreateTopic(bootstrap, topic, 2, 1); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestIntergationProducerX(t *testing.T) {
	batches := make(chan *batch.Batch, 10)
	topic := createTopic(t)
	p := &Producer{
		Bootstrap: "localhost:9092",
		Topic:     topic,
		Input:     batches,
	}
	exchanges, err := p.Start(5)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b, _ := batch.NewBuilder(now).AddStrings("foo", "bar").Build(now, &compression.Lz4{})
	batches <- b
	batches <- b
	close(batches)
	n := 0
	for e := range exchanges {
		if len(e.Errors) != 0 {
			t.Fatal(e.Errors)
		}
		t.Logf("%+v", e)
		n++
	}
	if n != 2 {
		t.Fatal(n)
	}
}

func TestIntergationProducerBadTopic(t *testing.T) {
	batches := make(chan *batch.Batch, 10)
	topic := createTopic(t)
	p := &Producer{
		Bootstrap: "localhost:9092",
		Topic:     topic,
		Input:     batches,
	}
	exchanges, err := p.Start(5)
	if err != nil {
		t.Fatal(err)
	}
	for i, _ := range p.producers {
		p.producers[i].Topic = "nosuchtopic"
	}
	now := time.Now()
	b, _ := batch.NewBuilder(now).AddStrings("foo", "bar").Build(now, &compression.None{})
	batches <- b
	batches <- b
	close(batches)
	for e := range exchanges {
		if n := len(e.Errors); n != maxRetries {
			t.Fatal(n)
		}
		t.Logf("%+v", e)
	}
}
