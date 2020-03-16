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

	"code.cfops.it/data/kafkaclient/builder"
	"code.cfops.it/data/kafkaclient/compression"
	"code.cfops.it/data/kafkaclient/producer"
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
	records := make(chan *record.Record)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			records <- record.New(nil, scanner.Bytes())
		}
	}()
	b := &builder.Builder{
		MaxRecords: 2,
		Compressor: &compression.Zstd{},
		Input:      records,
	}
	batches := b.Start(1)
	p := &producer.Producer{
		Bootstrap: *bootstrap,
		Topic:     *topic,
		Input:     batches,
	}
	exchanges, err := p.Start(1)
	if err != nil {
		log.Fatal(err)
	}
	for e := range exchanges {
		log.Printf("%+v", e)
	}
}
