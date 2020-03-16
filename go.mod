module code.cfops.it/data/kafkaclient

go 1.13

require (
	github.com/DataDog/zstd v1.4.4
	github.com/frankban/quicktest v1.8.1 // indirect
	github.com/mkocikowski/libkafka v0.0.1
	github.com/pierrec/lz4 v2.4.1+incompatible
)

//replace github.com/mkocikowski/libkafka => ../../../github.com/mkocikowski/libkafka
