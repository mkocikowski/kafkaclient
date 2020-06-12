// Package consumer implements a kafka consumer. A consumer (in my
// nomenclature) is different from a fetcher in that it includes logic for
// manipulating fetch offsets.
//
// Currently only a Static consumer is implemented (consumes from a static list
// of topic partitions). Call the Start method of a Static instance and read
// from the returned output channel.
//
// Each request-response pair and its metadata (timings, errors) are captured
// in an Exchange struct.  An exchange, if successful, will have one or more
// batches, and each batch will have one or more records.
//
// The logic for handling errors when multiple batches are fetched can be
// complex. That logic is implemented in DefaultHandleFetchResponse function,
// which is of type ResponseHandlerFunc. Read up on these if you want to
// implement your own error handling logic.
//
// Aspects of consumption that are currently not covered: offset storage and
// retrieval, and dynamic partition assignment through consumer group
// membership.
package consumer
