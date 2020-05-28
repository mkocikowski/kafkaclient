package consumer

import (
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

type mockFetcher struct {
	closed bool
	offset int64
}

func (*mockFetcher) Fetch() (*fetcher.Response, error) { return nil, nil }
func (*mockFetcher) Seek(time.Time) error              { return nil }
func (*mockFetcher) Offset() int64                     { return 0 }
func (f *mockFetcher) SetOffset(i int64)               { f.offset = i }
func (f *mockFetcher) Close() error                    { f.closed = true; return nil }

func TestUnitDefaultHandleFetchResponse(t *testing.T) {
	e := &Exchange{
		Response:      fetcher.Response{},
		RequestError:  nil,
		InitialOffset: 1,
		Batches: []*Batch{
			&Batch{Batch: libkafka.Batch{BaseOffset: 1, LastOffsetDelta: 0}}, // 1 record
			&Batch{Batch: libkafka.Batch{BaseOffset: 2, LastOffsetDelta: 0}}, // 1 record
		},
	}
	f := &mockFetcher{}
	DefaultHandleFetchResponse(f, e)
	if e.FinalOffset != 2 {
		t.Fatal(e.FinalOffset)
	}
	if f.offset != 3 {
		t.Fatal(f.offset)
	}
	if f.closed {
		t.Fatal("expected open")
	}
	// now simulate and error that should result in connection getting closed
	e.ErrorCode = libkafka.ERR_LEADER_NOT_AVAILABLE
	DefaultHandleFetchResponse(f, e)
	if !f.closed {
		t.Fatal("expected closed")
	}
}
