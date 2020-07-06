package builder

import "testing"

func TestHashPartitioner(t *testing.T) {
	p := &HashPartitioner{numPartitions: 10}
	if n := p.Partition([]byte("foo")); n != 3 {
		t.Error(n)
	}
	if n := p.Partition([]byte("bar")); n != 2 {
		t.Error(n)
	}
	if n := p.Partition(nil); n != 1 {
		t.Error(n)
	}
}
