package batches

import (
	"testing"

	"github.com/mkocikowski/kafkaclient/compression"
	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/record"
)

func TestUnitBuilderStartStop(t *testing.T) {
	builder := &Builder{
		Compressor: &compression.None{},
		MinRecords: 1,
		NumWorkers: 1,
	}
	records := make(chan []*libkafka.Record)
	batches := builder.Start(records)
	records <- []*libkafka.Record{record.New(nil, []byte("foo"))}
	if b := <-batches; b.NumRecords != 1 {
		t.Fatal(b.NumRecords)
	}
	close(records)
	if _, ok := <-batches; ok {
		t.Fatal("expected output to be closed")
	}
}

func TestUnitBuilderBigBatch(t *testing.T) {
	// setting MinRecords=1 but calling Add with 2 records. expect a single
	// batch of 2 records.
	builder := &Builder{
		Compressor: &compression.None{},
		MinRecords: 1,
		NumWorkers: 1,
	}
	records := make(chan []*libkafka.Record)
	batches := builder.Start(records)
	records <- []*libkafka.Record{
		record.New(nil, []byte("foo")),
		record.New(nil, []byte("bar")),
	}
	if b := <-batches; b.NumRecords != 2 {
		t.Fatal(b.NumRecords)
	}
}

func TestUnitBuilderSmallBatchFlush(t *testing.T) {
	// setting MinRecords=2 but calling Add with 1 record. then closing the
	// builder to "flush" expect a batch with only 1 record.
	builder := &Builder{
		Compressor: &compression.None{},
		MinRecords: 2,
		NumWorkers: 1,
	}
	records := make(chan []*libkafka.Record)
	batches := builder.Start(records)
	records <- []*libkafka.Record{record.New(nil, []byte("foo"))}
	close(records)
	if b := <-batches; b.NumRecords != 1 {
		t.Fatal(b.NumRecords)
	}
}
