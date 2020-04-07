package groups

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/api/JoinGroup"
	"github.com/mkocikowski/libkafka/api/SyncGroup"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type echo struct{}

func (*echo) Type() string          { return "partition" }
func (*echo) Name() string          { return "echo" }
func (*echo) Meta(id string) []byte { return []byte("banana") }

func (*echo) Assign(members []JoinGroup.Member) ([]SyncGroup.Assignment, error) {
	assignments := []SyncGroup.Assignment{}
	for _, m := range members {
		a := SyncGroup.Assignment{
			MemberId:   m.MemberId,
			Assignment: m.Metadata[:],
		}
		assignments = append(assignments, a)
	}
	return assignments, nil
}

func TestIntegrationJoinAndSync(t *testing.T) {
	c := &GroupMembershipManager{
		Bootstrap: "localhost:9092",
		Assigner:  &echo{},
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	c.init()
	for i := 0; i < 10; i++ {
		if err := c.join(); err != nil {
			t.Log(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := c.sync(); err != nil {
			t.Log(err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if c.generationId != 1 {
			t.Fatalf("%+v", c)
		}
		if string(c.assignment) != "banana" {
			t.Fatalf("%+v", c)
		}
		return
	}
	t.Fatal()
}

func TestIntegrationRun(t *testing.T) {
	c := &GroupMembershipManager{
		Bootstrap: "localhost:9092",
		Assigner:  &echo{},
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	assignments := c.Start()
	if a := <-assignments; string(a) != "banana" {
		t.Fatalf("%+s", a)
	}
}
