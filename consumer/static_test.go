package consumer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const bootstrap = "localhost:9092"

func createTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestIntergationStaticConsumer(t *testing.T) {
	topic := createTopic(t)
	p := &producer.PartitionProducer{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
		Acks:      1,
		TimeoutMs: 1000,
	}
	if _, err := p.ProduceStrings(time.Now(), "foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceStrings(time.Now(), "monkey", "banana"); err != nil {
		t.Fatal(err)
	}
	//
	c := Static{
		Bootstrap:      bootstrap,
		Topic:          topic,
		NumWorkers:     1,
		HandleResponse: DefaultHandleFetchResponse,
		MinBytes:       1 << 20,
		MaxBytes:       1 << 20,
		MaxWaitTimeMs:  100,
	}
	exchanges, err := c.Start(map[int32]int64{0: 0})
	if err != nil {
		t.Fatal(err)
	}
	e := <-exchanges
	if err := e.RequestError; err != nil {
		t.Fatal(err)
	}
	//t.Logf("%+v", e)
	//fmt.Println(base64.StdEncoding.EncodeToString(e.RecordSet))
	if n := len(e.Batches); n != 2 {
		t.Fatal(n)
	}
	if n := e.Batches[0].NumRecords; n != 2 {
		t.Fatal(n)
	}
	c.Stop()
	for _ = range exchanges { // drain
	}
	c.Wait()
}
