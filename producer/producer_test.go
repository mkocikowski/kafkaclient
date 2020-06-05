package producer

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/compression"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const bootstrap = "localhost:9092"

func createTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestIntegrationProducerSuccess(t *testing.T) {
	topic := createTopic(t)
	p := &Async{
		Bootstrap:   "localhost:9092",
		Topic:       topic,
		Partitions:  []int{0},
		NumWorkers:  10,
		NumAttempts: 3,
		Acks:        1,
		Timeout:     time.Second,
	}
	p.Wait() // calling Wait before Start should be a nop
	in := make(chan *Batch, 10)
	out, err := p.Start(in)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b, _ := batch.NewBuilder(now).AddStrings("foo", "bar").Build(now)
	if err := b.Compress(&compression.Nop{}); err != nil {
		t.Fatal(err)
	}
	in <- &Batch{Batch: *b}
	in <- &Batch{Batch: *b}
	close(in)
	n := 0
	for b := range out {
		t.Log(b)
		if !b.Produced() {
			t.Fatalf("%+v", b)
		}
		t.Logf("%+v", b)
		n++
	}
	if n != 2 {
		t.Fatal(n)
	}
	p.Wait()
}

func TestIntegrationProducerBadTopic(t *testing.T) {
	topic := createTopic(t)
	p := &Async{
		Bootstrap:   "localhost:9092",
		Topic:       topic,
		Partitions:  []int{0},
		NumWorkers:  10,
		NumAttempts: 3,
		Acks:        1,
		Timeout:     time.Second,
	}
	in := make(chan *Batch, 10)
	out, err := p.Start(in)
	if err != nil {
		t.Fatal(err)
	}
	for i, _ := range p.producers {
		p.producers[i].Topic = "nosuchtopic"
	}
	now := time.Now()
	b, _ := batch.NewBuilder(now).AddStrings("foo", "bar").Build(now)
	in <- &Batch{Batch: *b}
	in <- &Batch{Batch: *b}
	close(in)
	for b := range out {
		if n := len(b.Exchanges); n != p.NumAttempts {
			t.Fatal(n)
		}
		t.Logf("%+v", b)
	}
}

func TestUnitBatchProduced(t *testing.T) {
	tests := []struct {
		e    []*Exchange
		want bool
	}{
		{e: nil,
			want: false},
		{e: []*Exchange{parseResponse(time.Now(), nil, nil)},
			want: false},
		{e: []*Exchange{parseResponse(time.Now(), nil, errors.New("foo"))},
			want: false},
		{e: []*Exchange{parseResponse(time.Now(), &producer.Response{ErrorCode: 1}, nil)},
			want: false},
		{e: []*Exchange{parseResponse(time.Now(), &producer.Response{}, nil)},
			want: true},
		{e: []*Exchange{
			parseResponse(time.Now(), &producer.Response{ErrorCode: 1}, nil),
			parseResponse(time.Now(), &producer.Response{}, nil)},
			want: true},
	}
	for i, test := range tests {
		b := &Batch{Exchanges: test.e}
		if got := b.Produced(); got != test.want {
			t.Fatal(i, got, test.want)
		}
	}
}

func TestUnitEmptyBatch(t *testing.T) {
	b := &Batch{}
	if n := b.NumRecords; n != 0 {
		t.Fatal(n)
	}
}
