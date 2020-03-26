/*
Package kafkaclient implements a high-level kafka producer on top on libkafka.

Kafkaclient operates on record batches. Record batches are built using a
Builder. Record batches are sent to Kafka ("produced") using a Producer.
Connect a Builder to a Producer and send records to the Builder. See
cmd/producer for example implementation.
*/
package kafkaclient
