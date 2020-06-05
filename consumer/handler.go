package consumer

import (
	"io"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

// FetcherSeekerCloser is implemented by libkafka fetcher.PartitionFetcher. You
// actually don't get to use a different fetcher, the reason it is an interface
// is to make mocking out tests for ResponseHandlerfunc easier.
type FetcherSeekerCloser interface {
	Fetcher
	Seeker
	io.Closer
}

type Fetcher interface {
	Fetch() (*fetcher.Response, error)
}

type Seeker interface {
	// Seek sets the offset to close to the specified timestamp. On error
	// offset is not changed. Offset is not guaranteed to be aligned on
	// batch boundaries.
	Seek(time.Time) error
	Offset() int64
	SetOffset(int64)
}

// DefaultHandleFetchResponse implements basic exchange handling logic. Read
// through the code to understand the logic.
func DefaultHandleFetchResponse(f FetcherSeekerCloser, e *Exchange) {
	if e.RequestError != nil {
		// connection has been closed in libkafka
		return
	}
	if e.ErrorCode == libkafka.ERR_OFFSET_OUT_OF_RANGE {
		// if offset out of range (on either end) then go to "newest".
		// this is one of the places where you could apply a lot of
		// custom logic
		if err := f.Seek(fetcher.MessageNewest); err != nil {
			// my default of any "more complicated" errors is to
			// close the connection for the partition client. this
			// will force starting everything "from scratch" for
			// this partition: looking up the leader and
			// establishing the connection to the leader. but! the
			// current offset will stay the same
			f.Close()
		}
		// if there was no error, offset has been set to the currently
		// "newest". this exchange will still be marked as failed, but
		// the next fetch request from this partition should be
		// successful
		return
	}
	if e.ErrorCode != libkafka.ERR_NONE {
		f.Close()
		return
	}
	if len(e.Batches) == 0 {
		// this was an empty response where the consumer is at the head
		// but there are no new records to fetch. so here do not change
		// the offset, try again at the same place.
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
		// NOTE: there is an edge case where all batches in a fetch
		// response are "bad". in that case, the initial offset will
		// not move at all, and, if errors persist on subsequent calls,
		// the consumer will effectively be "stuck". TODO: figure out
		// how to get around this
	}
	if nextOffset == e.InitialOffset {
		// this happens when the response was successful, there were
		// one or more batches, but none of these batches had records
		// (because they had errors). this would leave the consumer
		// stuck at this offset forever. so set to -1 which is
		// guaranteed to raise ERR_OFFSET_OUT_OF_RANGE on the next
		// fetch, and then normal "out of range" handling can happen.
		f.SetOffset(-1)
		// TODO: set e.FinalOffset ?
		return
	}
	// this is not "commiting" offset. this is just moving the fetcher's
	// offset to the end of the last batch, so that on the next call to
	// Fetch it will start from the right place. if you don't do this here,
	// you will always read from the same offset.
	f.SetOffset(nextOffset)
	e.FinalOffset = nextOffset - 1
}
