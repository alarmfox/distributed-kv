package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/alarmfox/distributed-kv/cluster"
	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	shards        map[uint64]string
	currShardID   uint64
	currStorage   *storage.Storage
	clusterClient *cluster.Client
	sync.Mutex
}

func NewController(currStorage *storage.Storage, currShardID uint64, shardAddress string) *Controller {
	return &Controller{
		shards:      map[uint64]string{currShardID: shardAddress},
		currStorage: currStorage,
		currShardID: currShardID,
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(len(c.shards)))
}

var (
	ErrClusterUnhealthy = errors.New("cluster is not ready")
)

func (c *Controller) Get(key string) ([]byte, error) {
	if !c.clusterClient.IsClusterReady() {
		return []byte{}, ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; redirecting to Shard-%d", key, shardID)
		return c.getRemoteKey(c.shards[shardID], key)
	}

	return c.currStorage.Get(key), nil
}

func (c *Controller) Set(key string, value []byte) error {
	if !c.clusterClient.IsClusterReady() {
		return ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; redirecting to Shard-%d", key, shardID)
		return c.setRemoteKey(c.shards[shardID], key, value)
	}

	return c.currStorage.Set(key, value)
}

func (c *Controller) Reshard() error {
	c.clusterClient.LockCluster()
	defer c.clusterClient.UnlockCluster()

	itemsToReshard := c.currStorage.GetExtraKeys(func(key string) bool { return c.currShardID != c.getShardID(key) })

	for _, item := range itemsToReshard {
		shardID := c.getShardID(item.Key)
		if err := c.setRemoteKey(c.shards[shardID], item.Key, item.Value); err != nil {
			return fmt.Errorf("cannot set key %s: %v", item.Key, err)
		}
	}
	return c.currStorage.DeleteKeys(itemsToReshard)

}

func (c *Controller) setRemoteKey(address, key string, value []byte) error {
	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(value))

	reqUrl := fmt.Sprintf("http://%s/set?%s", address, params.Encode())
	resp, err := http.Get(reqUrl)

	if err != nil {
		return fmt.Errorf("Set(%q) error: %v", address, err)
	} else if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Set(%q) status %s", address, resp.Status)
	}

	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Controller) getRemoteKey(address, key string) ([]byte, error) {
	params := url.Values{}
	params.Add("key", key)

	reqUrl := fmt.Sprintf("http://%s/get?%s", address, params.Encode())
	resp, err := http.Get(reqUrl)

	if err != nil {
		return nil, fmt.Errorf("Get(%q) error: %v", reqUrl, err)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Get(%q) status %s", reqUrl, resp.Status)
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *Controller) processPing(shardID uint64, shardAddress string) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if !c.clusterClient.IsClusterReady() {
		return
	}

	address := c.shards[shardID]

	if c.currShardID == shardID && shardAddress != c.shards[c.currShardID] {
		c.clusterClient.Broadcast(cluster.PeerEvent{Event: cluster.ConflictEvent, ShardID: shardID, Address: address})
		return
	}

	if address != shardAddress {
		c.shards[shardID] = shardAddress
		log.Printf("Got new peer: id: %d; address %s", shardID, shardAddress)
		if err := c.Reshard(); err != nil {
			log.Printf("Reshard error: %v", err)
		}
	}
}
