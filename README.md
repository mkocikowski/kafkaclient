Overview
===
This is a high-level kafka client based on the low level
[libkafka](https://github.com/mkocikowski/libkafka) library. It is meant as an
alternative to Sarama for producing high volumes of messages.

For use see https://godoc.org/github.com/mkocikowski/kafkaclient

The main design point is that the library operates on record batches, not on
individual records. This makes it more efficient at high volumes.

Marshaling records into batches (building batches) is a separate step from
sending (producing) batches to Kafka. This means that resource use (memory,
cpu) can be controlled more precisely.

Compression is applied at batch level. Libkafka does not implement compression.
Codecs are provided by kafkaclient or by the user (codec is provided to batch
builder instance by the user). This makes it possible to have multiple
implementations of a compression scheme (for example: DataDog and KlausP for
zstd).

The underlying libkafka library implements synchronous single partition
producer and consumer. This library implements a multi-core batch builder and
an asynchronous multi-partition producer on top of that.

There are many ways in which the primitives from libkafka can be wrapped and
exposed to the end user. This is an example of how it can be done, but
definitely not the only way.
