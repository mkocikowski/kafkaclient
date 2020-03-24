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

	"github.com/mkocikowski/kafkaclient/builder"
	"github.com/mkocikowski/kafkaclient/compression"
	"github.com/mkocikowski/kafkaclient/producer"
	"github.com/mkocikowski/libkafka/record"
)

var (
	projectName  string
	buildVersion string
	buildTime    string
)

func main() {
	rand.Seed(time.Now().UnixNano())
	bootstrap := flag.String("bootstrap", "localhost:9092", "_lab-kafka._tcp.in.pdx.cfdata.org")
	topic := flag.String("topic", fmt.Sprintf("test-%x", rand.Uint32()), "")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC | log.Lmicroseconds)
	log.Printf("%s %s %s %s", projectName, buildVersion, buildTime, runtime.Version())
	//
	records := make(chan []*record.Record)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			records <- []*record.Record{record.New(nil, scanner.Bytes())}
		}
	}()
	b := &builder.Builder{
		Compressor: &compression.Zstd{},
		MinRecords: 1,
		NumWorkers: 1,
	}
	batches := b.Start(records)
	p := &producer.Producer{
		Bootstrap:  *bootstrap,
		Topic:      *topic,
		NumWorkers: 1,
	}
	exchanges, err := p.Start(batches)
	if err != nil {
		log.Fatal(err)
	}
	for e := range exchanges {
		log.Printf("%+v", e)
	}
}
