package builder

import (
	"testing"

	"code.cfops.it/data/kafkaclient/compression"
	"github.com/mkocikowski/libkafka/record"
)

func TestUnitBuilder(t *testing.T) {
	records := make(chan *record.Record, 10)
	records <- record.New(nil, []byte("foo"))
	records <- record.New(nil, []byte("bar"))
	close(records)
	builder := &Builder{
		Input:      records,
		MaxRecords: 1,
		Compressor: &compression.None{},
	}
	batches := builder.Start(5)
	n := 0
	for _ = range batches {
		n++
	}
	if n != 2 {
		t.Fatal(n)
	}
}
