package batch

import (
	"hash/fnv"
)

// Always sets partition to -1.
type NopPartitioner struct{}

func (*NopPartitioner) Partition([]byte) int32 { return -1 }

func (*NopPartitioner) SetNumPartitions(int32) {}

// Uses Fnv32a.
type HashPartitioner struct {
	// Must be greater than 0.
	numPartitions int32
}

func (p *HashPartitioner) Partition(key []byte) int32 {
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(p.numPartitions))
}

func (p *HashPartitioner) SetNumPartitions(n int32) {
	p.numPartitions = n
}
