package consumer

import (
	"time"

	"github.com/mkocikowski/kafkaclient"
	"github.com/mkocikowski/libkafka/client/fetcher"
)

// Exchange carries all information relevant to a single "fetch"
// request-response. This is what the consumer outputs to the user.
type Exchange struct {
	fetcher.Response
	RequestBegin time.Time
	ResponseEnd  time.Time
	// RequestError records "low level" errors. Things like network
	// timeouts, unexpected EOF reading response, etc. Even when
	// RequestError is nil, the exchange may have failed or partially
	// failed: the fetcher.ResponseError code may be other than
	// libkafka.ERR_NONE, which shows that even though request-response
	// completed successfuly, Kafka returned an error code. Or, individual
	// batches may have errors (which counts as "partial" failure).
	RequestError error
	// Offset of the fetch request
	InitialOffset int64
	// Offset of the last record in the batch. The initial offset of the
	// next fetch should be the FinalOffset+1 (this is handled by the
	// ResponseHandlerFunc). I batch has only 1 record then InitialOffset
	// and FinalOffset are the same.
	FinalOffset int64
	// Batches are unmarshaled from fetcher.Response.RecordSet (see its
	// godoc for additional details). The unmarshaling is done by the
	// consumer (but batches, if they are compressed, are NOT
	// decompressed). A successful response will have ZERO or more batches
	// (when offset is at the head but there is no new records to be read
	// then the response will have no error but the RecordSet will be nil
	// and there will be no batches). If there is an error unmarshaling a
	// batch (such as when the CRC doesn't match) then batch's Error will
	// be other than nil, but the batch will still be appended to this
	// slice.
	Batches []*Batch
}

var ErrNilResponse = kafkaclient.Errorf("nil response from broker")

func (e *Exchange) parseResponse(resp *fetcher.Response, err error) {
	e.ResponseEnd = time.Now().UTC()
	if err != nil {
		e.RequestError = kafkaclient.Errorf("%w", err)
		return
	}
	if resp == nil {
		e.RequestError = ErrNilResponse
		return
	}
	e.Response = *resp
	if resp.RecordSet != nil {
		for _, b := range resp.RecordSet.Batches() { // this is cheap
			batch := parseResponseBatch(b)
			// batches will, at some point, likely move around on
			// their own, so important we keep topic partition
			// information with them
			batch.Topic, batch.Partition = resp.Topic, resp.Partition
			// all batches, even ones with errors
			e.Batches = append(e.Batches, batch)
		}
	}
}
