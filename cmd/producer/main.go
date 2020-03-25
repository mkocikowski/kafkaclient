// Producer is a synchronous kafka producer. It reads strings from stdin one
// line at a time and sends them to kafka one record at a time with specified
// compression. Sending records one at a time is inefficient. This is meant as
// an example of how to use the library.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/mkocikowski/kafkaclient/batches"
	"github.com/mkocikowski/kafkaclient/compression"
	"github.com/mkocikowski/kafkaclient/producer"
	"github.com/mkocikowski/libkafka"
)

var (
	projectName  string
	buildVersion string
	buildTime    string
)

func main() {
	rand.Seed(time.Now().UnixNano())
	bootstrap := flag.String("bootstrap", "localhost:9092", "host:port or SRV")
	topic := flag.String("topic", fmt.Sprintf("test-%x", rand.Uint32()), "")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC | log.Lmicroseconds)
	log.Printf("%s %s %s %s", projectName, buildVersion, buildTime, runtime.Version())
	//
	records := make(chan []*libkafka.Record)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			records <- []*libkafka.Record{libkafka.NewRecord(nil, scanner.Bytes())}
		}
	}()
	b := &batches.Builder{
		Compressor: &compression.None{}, // could be &compression.Zstd{Level: 3} etc
		MinRecords: 1,
		NumWorkers: 1,
	}
	batches := b.Start(records)
	p := &producer.Async{
		Bootstrap:   *bootstrap,
		Topic:       *topic,
		NumWorkers:  1, // remember to have this >0
		NumAttempts: 3, // ditto
	}
	exchanges, err := p.Start(batches)
	if err != nil {
		log.Fatal(err)
	}
	for e := range exchanges {
		log.Printf("%+v", e)
	}
}
