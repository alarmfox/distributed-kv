package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"gitlab.com/alarmfox/distributed-kv/internal/controller"
	"gitlab.com/alarmfox/distributed-kv/internal/controller/transport"
)

var (
	configLocation = flag.String("shards-file", "shards.json", "Configuration file for shards")
)

func main() {
	fContent, err := ioutil.ReadFile(*configLocation)

	if err != nil {
		log.Fatalf("could not read %q: %v", *configLocation, err)
	}

	config, err := controller.MakeConfig(bytes.NewReader(fContent))
	if err != nil {
		log.Fatalf("could not make config: %v", err)
	}

	shards := make(map[int]string)
	for _, shard := range config.Shards {
		shards[shard.Id] = shard.Address
	}

	h := transport.MakeHTTPHandler(controller.NewService(shards))
	log.Fatal(http.ListenAndServe("localhost:5555", h))
}
