package consumer

import (
	"io"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

// FetcherSeekerCloser is implemented by libkafka fetcher.PartitionFetcher. You actually don't get
// to use a different fetcher, the reason it is an interface is to make mocking out tests for
// ResponseHandlerfunc easier.
type FetcherSeekerCloser interface {
	Fetcher
	Seeker
	io.Closer
}

type Fetcher interface {
	Fetch() (*fetcher.Response, error)
}

type Seeker interface {
	Seek(time.Time) error
	Offset() int64
	SetOffset(int64)
}

// DefaultHandleFetchResponse implements basic exchange handling logic. Read through the code.
func DefaultHandleFetchResponse(f FetcherSeekerCloser, e *Exchange) {
	if e.RequestError != nil {
		// connection has been closed in libkafka
		return
	}
	if e.ErrorCode == libkafka.ERR_OFFSET_OUT_OF_RANGE {
		// if offset out of range (on either end) then go to "newest". this is one of the
		// places where you could apply a lot of custom logic
		if err := f.Seek(fetcher.MessageNewest); err != nil {
			// my default of any "more complicated" errors is to close the connection
			// for the partition client. this will force starting everything "from
			// scratch" for this partition: looking up the leader and establishing the
			// connection to the leader. but! the current offset will stay the same
			f.Close()
		}
		// if there was no error, offset has been set to the currently "newest". this
		// exchange will still be marked as failed, but the next fetch request from this
		// partition should be successful
		return
	}
	if e.ErrorCode != libkafka.ERR_NONE {
		f.Close()
		return
	}
	nextOffset := e.InitialOffset
	for _, batch := range e.Batches {
		if batch.Error != nil {
			continue
		}
		nextOffset = batch.LastOffset() + 1
		// if the last batch fail it will be retried next time (offset
		// will not be advanced past it). if a batch "in the middle"
		// fails it will be skipped (offset will be advanced past it).
	}
	// this is not "commiting" offset. this is just moving the fetcher's offset to the end of
	// the last batch, so that on the next call to Fetch it will start from the right place. if
	// you don't do this here, you will always read from the same offset.
	f.SetOffset(nextOffset)
	e.FinalOffset = nextOffset - 1
}
