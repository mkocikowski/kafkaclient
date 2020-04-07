package consumer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

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
	}
	if _, err := p.ProduceStrings(time.Now(), "foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceStrings(time.Now(), "monkey", "banana"); err != nil {
		t.Fatal(err)
	}
	//
	c := Static{
		Bootstrap:  bootstrap,
		Topic:      topic,
		NumWorkers: 1,
	}
	responses, err := c.Start(map[int32]int64{0: 0})
	if err != nil {
		t.Fatal(err)
	}
	r := <-responses
	if r.Error != nil {
		t.Fatal(err)
	}
	records, err := r.Records(&compression.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(records); n != 4 {
		t.Fatal(n)
	}
	c.Stop()
	for _ = range responses { // drain
	}
	c.Wait()
}
