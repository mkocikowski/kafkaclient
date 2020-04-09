package consumer

import (
	"encoding/base64"
	"testing"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client/fetcher"
	"github.com/mkocikowski/libkafka/compression"
)

const recordSetFixture = `AAAAAAAAAAAAAABFAAAAAAKWOefaAAAAAAABAAABcVrvssgAAAFxWu+yyP////////////8AAAAAAAAAAhIAAAAABmZvbwASAAACAAZiYXIAAAAAAAAAAAIAAABLAAAAAAJkxR4UAAAAAAABAAABcVrvsssAAAFxWu+yy/////////////8AAAAAAAAAAhgAAAAADG1vbmtleQAYAAACAAxiYW5hbmEA`

func TestUnitParseFetchResponse(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{RecordSet: recordSet}
	e := &Exchange{}
	e.parseFetchResponse(resp, nil)
	if len(e.Batches) != 2 {
		t.Fatalf("%+v", e)
	}
	t.Log(e.Batches[1].MaxTimestamp())
}

func TestUnitGetRecords(t *testing.T) {
	recordSet, _ := base64.StdEncoding.DecodeString(recordSetFixture)
	resp := &fetcher.Response{RecordSet: recordSet}
	e := &Exchange{}
	e.parseFetchResponse(resp, nil)
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
