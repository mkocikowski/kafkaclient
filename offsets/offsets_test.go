package offsets

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
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

type dummyGroupClient struct {
	offset            int64
	commitOffsetError error
}

func (d *dummyGroupClient) FetchOffset(topic string, partition int32) (int64, error) {
	return d.offset, nil
}
func (d *dummyGroupClient) CommitOffset(topic string, partition int32, offset, retentionMs int64) error {
	return d.commitOffsetError
}

func TestUnitCommitOffet(t *testing.T) {
	cl := &dummyGroupClient{}

	initClient := func(bootstrap, group string) groupClient {
		return &dummyGroupClient{}
	}

	om := &DumbOffsetsManager{
		client:     cl,
		initClient: initClient,
	}

	cl.commitOffsetError = &libkafka.Error{
		Code: libkafka.ERR_BROKER_NOT_AVAILABLE,
	}
	// we should get error as is
	e := om.Commit("topic", 0, 1)
	if e == nil {
		t.Fatal("we didn't get expected error")
	}
	if !strings.HasSuffix(e.Error(), "(BROKER_NOT_AVAILABLE)") {
		t.Fatalf("we got unexpected error: %v", e)
	}

	// we should get error as is
	cl.commitOffsetError = fmt.Errorf("some sort of error")
	e = om.Commit("topic", 0, 1)
	if e == nil {
		t.Fatal("we didn't get expected error")
	}
	if !strings.HasSuffix(e.Error(), "some sort of error") {
		t.Fatalf("we got unexpected error: %v", e)
	}

	// clean commit, no error
	cl.commitOffsetError = nil
	e = om.Commit("topic", 0, 1)
	if e != nil {
		t.Fatalf("we got unexpected error: %v", e)
	}

	// coordinator was re-assigned, we should see this in error, next commit
	// should be okay
	cl.commitOffsetError = &libkafka.Error{
		Code: libkafka.ERR_NOT_COORDINATOR,
	}
	e = om.Commit("topic", 0, 1)
	if e == nil {
		t.Fatal("we didn't get expected error")
	}
	if !strings.HasSuffix(e.Error(), "(NOT_COORDINATOR)") {
		t.Fatalf("we got unexpected error: %v", e)
	}
	if om.client != nil {
		t.Fatal("client should be reset")
	}
	e = om.Commit("topic", 0, 1)
	if e != nil {
		t.Fatalf("we got unexpected error: %v", e)
	}
}
