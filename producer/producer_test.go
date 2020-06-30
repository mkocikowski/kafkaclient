package producer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/compression"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const bootstrap = "localhost:9092"

func createTopic(t *testing.T, numPartitions int32) string {
	t.Helper()
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, topic, numPartitions, 1); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestIntegrationProducerSuccess(t *testing.T) {
	topic := createTopic(t, 1)
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
		//t.Log(b)
		if !b.Produced() {
			t.Fatalf("%+v", b)
		}
		//t.Logf("%+v", b)
		n++
	}
	if n != 2 {
		t.Fatal(n)
	}
	p.Wait()
}

func TestIntegrationProducerPartitioned(t *testing.T) {
	topic := createTopic(t, 10)
	p := &Async{
		Bootstrap:   "localhost:9092",
		Topic:       topic,
		Partitions:  []int{9, 3, 1},
		NumWorkers:  1,
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
	// "random" partition, should be first configured, which is 9
	in <- &Batch{Batch: *b, Partition: -1}
	resp := <-out
	if !resp.Produced() || resp.Exchanges[0].Response.Partition != 9 {
		t.Logf("%+v", resp)
	}
	// repeat, now expect next partition, which is 3
	in <- &Batch{Batch: *b, Partition: -1}
	resp = <-out
	if !resp.Produced() || resp.Exchanges[0].Response.Partition != 3 {
		t.Logf("%+v", resp)
	}
	// now explicit assign partition, 9
	in <- &Batch{Batch: *b, Partition: 9}
	resp = <-out
	if !resp.Produced() || resp.Exchanges[0].Response.Partition != 9 {
		t.Logf("%+v", resp)
	}
	// now "break" the connection for 1 partition. here error is caused by
	// a "dirty" close of the underlying connection. since
	// StrictPartitioning==false, i expect the partition to be set to -1
	p.producers[9].Conn().Close()
	in <- &Batch{Batch: *b, Partition: 9}
	resp = <-out
	if !resp.Produced() || resp.Partition != -1 || len(resp.Exchanges) != 2 {
		t.Logf("%+v", resp)
	}
	//
	// now partition which exist, but for which there is no producer
	in <- &Batch{Batch: *b, Partition: 8}
	resp = <-out
	if resp.Produced() || resp.ProducerError != ErrNoProducerForPartition {
		t.Logf("%+v", resp)
	}
	close(in)
	p.Wait()
}

func TestIntegrationProducerTopicPartitionDoesNotExist(t *testing.T) {
	topic := createTopic(t, 1)
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
		if b.Produced() {
			t.Fatalf("%+v", b)
		}
		//t.Logf("%+v", b)
	}
}

func TestUnitBatchProduced(t *testing.T) {
	tests := []struct {
		e    []*Exchange
		want bool
	}{
		{e: nil,
			want: false},
		{e: []*Exchange{&Exchange{Error: fmt.Errorf("foo")}},
			want: false},
		{e: []*Exchange{&Exchange{Error: fmt.Errorf("foo")}, &Exchange{}},
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
