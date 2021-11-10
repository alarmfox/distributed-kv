package domain

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	shards         *ShardMap
	selfID         uint64
	selfAddress    string
	storage        *storage.Storage
	client         *http.Client
	broadcastQueue chan peerMessage
	clusterHealthy int32
	clusterLocked  int32
}

func NewController(currStorage *storage.Storage, shards *ShardMap, selfID uint64, selfAddress string) *Controller {
	return &Controller{
		shards:      shards,
		storage:     currStorage,
		selfID:      selfID,
		selfAddress: selfAddress,
		client: &http.Client{
			Timeout: time.Second,
		},
		broadcastQueue: make(chan peerMessage),
		clusterHealthy: 1,
		clusterLocked:  0,
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % c.shards.Count())
}

var (
	ErrClusterUnhealthy = errors.New("cluster is not ready")
)

func (c *Controller) Get(key string) ([]byte, error) {
	if !c.isClusterReady() {
		return []byte{}, ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.selfID {
		log.Printf("Key: %s; redirecting to Shard-%d", key, shardID)
		return c.getRemoteKey(c.shards.Get(shardID), key)
	}

	return c.storage.Get(key), nil
}

func (c *Controller) Set(key string, value []byte) error {
	if !c.isClusterReady() {
		return ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.selfID {
		log.Printf("Key: %s; redirecting to Shard-%d", key, shardID)
		return c.setRemoteKey(c.shards.Get(shardID), key, value)
	}

	return c.storage.Set(key, value)
}

func (c *Controller) Reshard() error {
	c.lockCluster()
	defer c.unlockCluster()

	itemsToReshard := c.storage.GetExtraKeys(func(key string) bool { return c.selfID != c.getShardID(key) })

	for _, item := range itemsToReshard {
		shardID := c.getShardID(item.Key)
		if err := c.setRemoteKey(c.shards.Get(shardID), item.Key, item.Value); err != nil {
			return fmt.Errorf("cannot set key %s: %v", item.Key, err)
		}
	}
	return c.storage.DeleteKeys(itemsToReshard)

}

func (c *Controller) setRemoteKey(address, key string, value []byte) error {
	params := url.Values{}
	params.Add("key", key)
	params.Add("value", string(value))

	reqUrl := fmt.Sprintf("http://%s/set?%s", address, params.Encode())
	resp, err := c.client.Get(reqUrl)

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
	resp, err := c.client.Get(reqUrl)

	if err != nil {
		return nil, fmt.Errorf("Get(%q) error: %v", reqUrl, err)
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Get(%q) status %s", reqUrl, resp.Status)
	}

	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

const (
	pingEvent = iota
	conflictEvent
	clusterLocked
	clusterUnlocked
)

type peerMessage struct {
	Event   uint8
	ShardID uint64
	Address string
}

func (c *Controller) processPeerMessage(pm peerMessage) {
	switch pm.Event {
	case pingEvent:
		c.processPing(pm.ShardID, pm.Address)
	case conflictEvent:
		c.processConflict(pm.ShardID)
	case clusterLocked:
		atomic.StoreInt32(&c.clusterLocked, 1)
	case clusterUnlocked:
		atomic.StoreInt32(&c.clusterLocked, 0)
	default:
		log.Printf("Unknown action: %d", pm.Event)
	}
}

func (c *Controller) JoinCluster(ctx context.Context, peerAddress string) error {
	go c.manageCluster(ctx, peerAddress)
	return c.listenForPeers(ctx, peerAddress)

}

func (c *Controller) processPing(shardID uint64, shardAddress string) {
	if !c.isClusterReady() {
		return
	}

	address := c.shards.Get(shardID)
	if c.selfID == shardID && shardAddress != c.shards.Get(c.selfID) {
		c.broadcastQueue <- peerMessage{Event: conflictEvent, ShardID: shardID, Address: address}
		return
	}

	if address != shardAddress {
		c.shards.Set(shardID, shardAddress)
		log.Printf("Got new peer: id: %d; address %s", shardID, shardAddress)
		if err := c.Reshard(); err != nil {
			log.Printf("Reshard error: %v", err)
		}
	}
}

func (c *Controller) processConflict(shardID uint64) {

	if atomic.CompareAndSwapInt32(&c.clusterHealthy, 1, 0) {
		log.Printf("Conflict for shard %d; cluster is unhealthy", shardID)
	}

}

const (
	bufferSize       = 8192
	socketBufferSize = 1024 * 1024
)

func (c *Controller) listenForPeers(ctx context.Context, address string) error {
	defer close(c.broadcastQueue)
	laddr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return fmt.Errorf("resolve(%q): %v", address, err)
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, laddr)
	if err != nil {
		return fmt.Errorf("listen(%q): %v", address, err)
	}
	defer conn.Close()

	conn.SetReadBuffer(socketBufferSize)

	log.Printf("Listening for peers on: %s", address)
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
			buffer := make([]byte, bufferSize)
			bytesRead, from, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Printf("Receive error: %v", err)
				continue
			}
			var msg peerMessage
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				log.Printf("Decode error from %s: %v", from.IP, err)
			}
			c.processPeerMessage(msg)
		}
	}
	return nil
}

const (
	pingInterval  = time.Second
	conflictRetry = 5 * time.Second
)

func (c *Controller) manageCluster(ctx context.Context, peerAddress string) {
	raddr, err := net.ResolveUDPAddr("udp4", peerAddress)
	if err != nil {
		log.Printf("Resolve(%q): %v", peerAddress, err)
		return
	}

	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		log.Printf("Dial error: %v", err)
		return
	}

	defer conn.Close()

	pingMessage := peerMessage{Event: pingEvent, ShardID: c.selfID, Address: c.shards.Get(c.selfID)}
	pingTicks := time.NewTicker(pingInterval)
	recoverConflictTicks := time.NewTicker(conflictRetry)

	defer func() {
		pingTicks.Stop()
		recoverConflictTicks.Stop()
	}()

LOOP:
	for {
		select {
		case <-recoverConflictTicks.C:
			if atomic.CompareAndSwapInt32(&c.clusterHealthy, 0, 1) {
				log.Printf("trying to recover cluster")
			}
		case <-pingTicks.C:
			broadcast(conn, pingMessage)
		case m := <-c.broadcastQueue:
			broadcast(conn, m)
		case <-ctx.Done():
			break LOOP
		}
	}
}

func (c *Controller) lockCluster() {
	atomic.StoreInt32(&c.clusterLocked, 1)
	c.broadcastQueue <- peerMessage{Event: clusterLocked}
	log.Printf("cluster locked")

}

func (c *Controller) unlockCluster() {
	atomic.StoreInt32(&c.clusterLocked, 0)
	c.broadcastQueue <- peerMessage{Event: clusterUnlocked}
	log.Printf("cluster unlocked")
}

func (c *Controller) isClusterReady() bool {
	return atomic.LoadInt32(&c.clusterHealthy) == 1 && atomic.LoadInt32(&c.clusterLocked) == 0
}

func broadcast(w io.Writer, pm peerMessage) {
	body, _ := json.Marshal(pm)
	if _, err := w.Write(body); err != nil {
		log.Printf("Write(%+v): %v", pm, err)
	}
}
