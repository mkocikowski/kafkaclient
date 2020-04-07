package assigners

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/api/JoinGroup"
	"github.com/mkocikowski/libkafka/client"
)

func TestUnitAssignRandomPartitions(t *testing.T) {
	members := []JoinGroup.Member{
		JoinGroup.Member{MemberId: "a"},
		JoinGroup.Member{MemberId: "b"},
		JoinGroup.Member{MemberId: "c"},
	}
	partitions := []int32{1, 3, 2, 4}
	assignments := assignRandomPartitions(members, partitions)
	if assignments["c"][0] != 2 {
		t.Fatalf("%+v", assignments)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

const bootstrap = "localhost:9092"

func createTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CreateTopic(bootstrap, topic, 3, 1); err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestIntegrationAssignRandomPartitions(t *testing.T) {
	topic := createTopic(t)
	a := &RandomPartition{
		Bootstrap: bootstrap,
		Topic:     topic,
	}
	members := []JoinGroup.Member{JoinGroup.Member{MemberId: "foo"}}
	assignments, err := a.Assign(members)
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 1 {
		t.Fatalf("%+v", assignments)
	}
	var partitions []int32
	if err := json.Unmarshal(assignments[0].Assignment, &partitions); err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 3 {
		t.Fatalf("%+v", assignments)
	}
}
