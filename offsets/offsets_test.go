package offsets

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntegrationOffsets(t *testing.T) {
	m := &DumbOffsetsManager{
		Bootstrap: "localhost:9092",
		GroupId:   fmt.Sprintf("test-%x", rand.Uint32()),
	}
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	// topic does not exist
	offset, err := m.Fetch(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != -1 {
		t.Fatal(offset)
	}
	// topic partition must exist to commit offset
	err = m.Commit(topic, 0, 100)
	var e *libkafka.Error
	if !errors.As(err, &e) {
		t.Fatal("expected libkafka.Error")
	}
	if e.Code != libkafka.ERR_UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(e)
	}
	//
	if _, err := client.CallCreateTopic("localhost:9092", topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	// topic exists but no offsets commited
	offset, err = m.Fetch(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != -1 {
		t.Fatal(offset)
	}
	if err := m.Commit(topic, 0, 100); err != nil {
		t.Fatal(err)
	}
	// now the topic is there and offset has been commited
	offset, err = m.Fetch(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal(offset)
	}
}
