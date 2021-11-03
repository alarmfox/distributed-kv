package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/alarmfox/distributed-kv/cluster"
	"github.com/alarmfox/distributed-kv/domain"
	"github.com/alarmfox/distributed-kv/storage"
	"github.com/alarmfox/distributed-kv/transport"
)

func main() {
	var (
		dbLocation    = flag.String("db-location", "", "Path where to create db file.")
		listenAddress = flag.String("listen-addr", "127.0.0.1:9999", "Listen address for the shard.")
		peerAddress   = flag.String("peer-addr", "239.0.0.0:9999", "Listen address for inter cluster communication. Must be a multicast address")
		shardID       = flag.Uint64("id", 0, "ID of the shard.")
		// name          = flag.String("name", "", "Shard name.")
		// configFile    = flag.String("config-file", "config.json", "Configuration file for shards.")
	)
	flag.Parse()

	// // fContent, err := os.ReadFile(*configFile)

	// // if err != nil {
	// // 	log.Fatalf("readfile(%q): %v", *configFile, err)
	// // }

	// shards, currShard, err := parseConfig(fContent, *name)
	// if err != nil {
	// 	log.Fatalf("[%s] config error: %v", *name, err)
	// }
	log.SetPrefix(fmt.Sprintf("[Shard-%d] ", *shardID))

	db, dispose, err := storage.New(*dbLocation)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer dispose()
	log.Printf("Listening on %s", *listenAddress)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	controller := domain.NewController(db, *shardID)
	client := cluster.Client{
		Self: domain.PeerMessage{
			ShardID: *shardID,
			Address: *listenAddress,
		},
		OnPeerMessage: controller.UpdateShardMap,
	}
	go client.Join(ctx, *peerAddress)

	http.ListenAndServe(*listenAddress, transport.MakeHTTPHandler(controller))

}

// type config struct {
// 	Shards []struct {
// 		ID      uint   `json:"id"`
// 		Name    string `json:"name"`
// 		Address string `json:"address"`
// 	} `json:"shards"`
// }

// func parseConfig(body []byte, currShardName string) (map[uint64]string, uint64, error) {
// 	var c config
// 	if err := json.Unmarshal(body, &c); err != nil {
// 		return nil, 0, err
// 	}

// 	shardMap := make(map[uint64]string)
// 	currShard := -1
// 	for _, shard := range c.Shards {
// 		shardMap[uint64(shard.ID)] = shard.Address
// 		if shard.Name == currShardName {
// 			currShard = int(shard.ID)
// 		}
// 	}
// 	if currShard < 0 {
// 		return nil, 0, fmt.Errorf("shard (%q) not found in config", currShardName)
// 	}
// 	return shardMap, uint64(currShard), nil
// }
