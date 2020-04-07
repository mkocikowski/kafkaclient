package assigners

import (
	"encoding/json"

	"github.com/mkocikowski/libkafka/api/JoinGroup"
	"github.com/mkocikowski/libkafka/api/SyncGroup"
	"github.com/mkocikowski/libkafka/client"
)

type RandomPartition struct {
	Bootstrap string
	Topic     string
}

func (*RandomPartition) Type() string       { return "partitioner" }
func (*RandomPartition) Name() string       { return "random-23" }
func (*RandomPartition) Meta(string) []byte { return []byte{} }

func assignRandomPartitions(members []JoinGroup.Member, partitions []int32) map[string][]int32 {
	assignments := map[string][]int32{}
	for i, p := range partitions {
		n := i % len(members)
		m := members[n]
		assignments[m.MemberId] = append(assignments[m.MemberId], p)
	}
	return assignments
}

func (p *RandomPartition) Assign(members []JoinGroup.Member) ([]SyncGroup.Assignment, error) {
	if len(members) == 0 { // not leader
		return []SyncGroup.Assignment{}, nil
	}
	leaders, err := client.GetPartitionLeaders(p.Bootstrap, p.Topic)
	if err != nil {
		return nil, err
	}
	var partitions []int32
	for p, _ := range leaders {
		partitions = append(partitions, p)
	}
	assignments := []SyncGroup.Assignment{}
	for m, p := range assignRandomPartitions(members, partitions) {
		b, _ := json.Marshal(p)
		a := SyncGroup.Assignment{
			MemberId:   m,
			Assignment: b,
		}
		assignments = append(assignments, a)
	}
	return assignments, nil
}
