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
	shards        *storage.ShardMap
	currShardID   uint64
	currStorage   *storage.Storage
	clusterClient *cluster.Client
	sync.Mutex
}

func NewController(currStorage *storage.Storage, shards *storage.ShardMap, client *cluster.Client) *Controller {
	return &Controller{
		shards:        shards,
		currStorage:   currStorage,
		clusterClient: client,
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(c.shards.Count()))
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
		return c.getRemoteKey(c.shards.Get(shardID), key)
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
		return c.setRemoteKey(c.shards.Get(shardID), key, value)
	}

	return c.currStorage.Set(key, value)
}

func (c *Controller) Reshard() error {
	c.clusterClient.LockCluster()
	defer c.clusterClient.UnlockCluster()

	itemsToReshard := c.currStorage.GetExtraKeys(func(key string) bool { return c.currShardID != c.getShardID(key) })

	for _, item := range itemsToReshard {
		shardID := c.getShardID(item.Key)
		if err := c.setRemoteKey(c.shards.Get(shardID), item.Key, item.Value); err != nil {
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
