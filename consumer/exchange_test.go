package consumer

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

const recordSetFixture = `AAAAAAAAAAAAAABFAAAAAAKWOefaAAAAAAABAAABcVrvssgAAAFxWu+yyP////////////8AAAAAAAAAAhIAAAAABmZvbwASAAACAAZiYXIAAAAAAAAAAAIAAABLAAAAAAJkxR4UAAAAAAABAAABcVrvsssAAAFxWu+yy/////////////8AAAAAAAAAAhgAAAAADG1vbmtleQAYAAACAAxiYW5hbmEA`

func TestUnitParseResponse(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{Topic: "foo", RecordSet: recordSet}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	if len(e.Batches) != 2 {
		t.Fatalf("%+v", e)
	}
	if topic := e.Topic; topic != "foo" {
		t.Fatal(topic)
	}
	if topic := e.Batches[1].Topic; topic != "foo" {
		t.Fatal(topic)
	}
	if n := e.Batches[1].NumRecords; n != 2 {
		t.Fatal(n)
	}
	if n := e.Batches[1].BaseOffset; n != 2 {
		t.Fatal(n)
	}
	b, _ := json.Marshal(e)
	t.Log(string(b))
}

// corrupt one of the 2 batches and make sure that still 2 batches are returned
// and that one of them has error
func TestUnitParseResponseBadBatch(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	recordSet[len(recordSet)-2] = 0 // corrupt second batch
	resp := &fetcher.Response{Topic: "foo", RecordSet: recordSet}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	if len(e.Batches) != 2 {
		t.Fatalf("%+v", e)
	}
	if err := e.Batches[0].Error; err != nil {
		t.Fatal(err)
	}
	if err := e.Batches[1].Error; !errors.Is(err, batch.CorruptedBatchError) {
		t.Fatal(err)
	}
	if n := e.Batches[0].NumRecords; n != 2 {
		t.Fatal(n)
	}
	if n := e.Batches[1].NumRecords; n != 0 {
		t.Fatal(n)
	}
	b, _ := json.Marshal(e)
	t.Log(string(b))
}

// no error no records. expect Batches to be nil, not empty slice
func TestUnitParseResponseNoRecords(t *testing.T) {
	resp := &fetcher.Response{Topic: "foo", RecordSet: nil}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	if e.Batches != nil {
		t.Fatalf("%+v", e.Batches)
	}
}

func TestUnitParseResponseNil(t *testing.T) {
	e := &Exchange{}
	e.parseResponse(nil, nil)
	if e.RequestError != ErrNilResponse {
		t.Fatal(e.RequestError)
	}
}
