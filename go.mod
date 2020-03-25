module github.com/mkocikowski/kafkaclient

go 1.13

require (
	github.com/DataDog/zstd v1.4.4
	github.com/frankban/quicktest v1.8.1 // indirect
	github.com/mkocikowski/libkafka v0.0.2
	github.com/pierrec/lz4 v2.4.1+incompatible
)

//replace github.com/mkocikowski/libkafka => /home/mik/src/github.com/mkocikowski/libkafka
