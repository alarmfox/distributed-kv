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
	"sync"
	"sync/atomic"
	"time"

	"github.com/alarmfox/distributed-kv/storage"
)

type Controller struct {
	shards         map[uint64]string
	currShardID    uint64
	currStorage    *storage.Storage
	client         *http.Client
	eventsQueue    chan peerMessage
	broadcastQueue chan peerMessage
	clusterHealthy int32
	sync.Mutex
}

func NewController(currStorage *storage.Storage, currShardID uint64, shardAddress string) *Controller {
	return &Controller{
		shards:      map[uint64]string{currShardID: shardAddress},
		currStorage: currStorage,
		currShardID: currShardID,
		client: &http.Client{
			Timeout: time.Second,
		},
		eventsQueue:    make(chan peerMessage),
		broadcastQueue: make(chan peerMessage, 1),
		clusterHealthy: 1,
	}
}

func (c *Controller) getShardID(key string) uint64 {
	return (binary.BigEndian.Uint64(fnv.New128().Sum([]byte(key))) % uint64(len(c.shards)))
}

var (
	ErrClusterUnhealthy = errors.New("cluster is not ready")
)

func (c *Controller) Get(key string) ([]byte, error) {
	if atomic.LoadInt32(&c.clusterHealthy) == 0 {
		return nil, ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; curShard: %d; redirecting to Shard-%d", key, c.currShardID, shardID)
		return c.getRemoteKey(c.shards[shardID], key)
	}

	return c.currStorage.Get(key), nil
}

func (c *Controller) Set(key string, value []byte) error {
	if atomic.LoadInt32(&c.clusterHealthy) == 0 {
		return ErrClusterUnhealthy
	}
	shardID := c.getShardID(key)
	if shardID != c.currShardID {
		log.Printf("Key: %s; curShard: %d; redirecting to Shard-%d", key, c.currShardID, shardID)
		return c.setRemoteKey(c.shards[shardID], key, value)
	}

	return c.currStorage.Set(key, value)
}

func (c *Controller) Reshard() error {
	itemsToReshard := c.currStorage.GetExtraKeys(func(key string) bool { return c.currShardID != c.getShardID(key) })

	atomic.StoreInt32(&c.clusterHealthy, 0)
	defer atomic.StoreInt32(&c.clusterHealthy, 1)

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
	clusterHealthy
)

type peerMessage struct {
	ActionID uint8
	ShardID  uint64
	Address  string
}

func (c *Controller) processPeerMessage(pm peerMessage) {
	switch pm.ActionID {
	case pingEvent:
		c.processPing(pm.ShardID, pm.Address)
	case conflictEvent:
		c.processConflict(pm.ShardID)
	default:
		log.Printf("unknown action: %d", pm.ActionID)
	}
}

func (c *Controller) JoinCluster(ctx context.Context, peerAddress string) error {
	go c.manageCluster(ctx, peerAddress)
	return listenForPeers(ctx, peerAddress, c.eventsQueue)

}

func (c *Controller) processPing(shardID uint64, shardAddress string) {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	address := c.shards[shardID]

	if c.currShardID == shardID && shardAddress != c.shards[c.currShardID] {
		c.broadcastQueue <- peerMessage{ActionID: conflictEvent, ShardID: shardID, Address: address}
		return
	}

	if atomic.LoadInt32(&c.clusterHealthy) == 0 {
		return
	}

	if address != shardAddress {
		c.shards[shardID] = shardAddress
		log.Printf("Got new peer %d with address %s", shardID, shardAddress)
		if err := c.Reshard(); err != nil {
			log.Printf("Reshard error: %v", err)
		}
	}
}

const conflictRetry = 5 * time.Second

func (c *Controller) processConflict(shardID uint64) {

	if atomic.CompareAndSwapInt32(&c.clusterHealthy, 1, 0) {
		log.Printf("Conflict for shard %d; cluster is unhealthy", shardID)
	}

	time.AfterFunc(conflictRetry, func() {
		if atomic.CompareAndSwapInt32(&c.clusterHealthy, 0, 1) {
			log.Printf("Cluster healthy")
		}
	})

}

const (
	bufferSize       = 8192
	socketBufferSize = 1024 * 1024
)

func listenForPeers(ctx context.Context, address string, events chan<- peerMessage) error {
	laddr, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		return fmt.Errorf("resolve(%q): %v", address, err)
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, laddr)
	if err != nil {
		return fmt.Errorf("listen(%q): %v", address, err)
	}
	defer conn.Close()
	defer close(events)

	conn.SetReadBuffer(socketBufferSize)

	log.Printf("Listening for peers on: %s", address)
	for {
		select {
		case <-ctx.Done():
			return nil
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
			events <- msg
		}
	}
}

const (
	pingInterval = time.Second
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

	defer func() {
		conn.Close()
		close(c.broadcastQueue)
	}()

	pingMessage := peerMessage{ActionID: pingEvent, ShardID: c.currShardID, Address: c.shards[c.currShardID]}
LOOP:
	for {
		select {
		case <-time.After(pingInterval):
			broadcast(conn, pingMessage)
		case m := <-c.broadcastQueue:
			broadcast(conn, m)
		case m := <-c.eventsQueue:
			c.processPeerMessage(m)
		case <-ctx.Done():
			break LOOP
		}
	}
}

func broadcast(w io.Writer, pm peerMessage) {
	body, _ := json.Marshal(pm)
	if _, err := w.Write(body); err != nil {
		log.Printf("Publish error: %v", err)
	}
}
