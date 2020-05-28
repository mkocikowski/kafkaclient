package consumer

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/compression"
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

func TestUnitParseResponseNil(t *testing.T) {
	e := &Exchange{}
	e.parseResponse(nil, nil)
	if e.RequestError != ErrNilResponse {
		t.Fatal(e.RequestError)
	}
}

func TestUnitGetRecords(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{RecordSet: recordSet}
	e := &Exchange{}
	e.parseResponse(resp, nil)
	b := e.Batches[1]
	decompressors := map[int16]batch.Decompressor{compression.None: &compression.Nop{}}
	records, err := b.Records(decompressors)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("%+v", records)
	}
	r := records[1]
	if s := string(r.Value); s != "banana" {
		t.Fatal(s)
	}
	//
	delete(decompressors, compression.None)
	if _, err := b.Records(decompressors); err == nil {
		t.Fatal(`expected "no decompressor" error`)
	}
}
