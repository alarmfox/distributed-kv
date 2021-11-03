package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	)
	flag.Parse()
	log.SetPrefix(fmt.Sprintf("[Shard-%d] ", *shardID))

	db, dispose, err := storage.New(*dbLocation)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer dispose()

	controller := domain.NewController(db, *shardID, *listenAddress)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go controller.JoinCluster(ctx, *peerAddress)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM)
		defer close(sig)
		<-sig
		cancel()

	}()
	if err := listenAndServe(ctx, *listenAddress, transport.MakeHTTPHandler(controller)); err != nil {
		log.Printf("listen(%q) error: %v", *listenAddress, err)
	}
}

func listenAndServe(ctx context.Context, address string, handler http.Handler) error {
	srv := http.Server{
		Addr:              address,
		Handler:           handler,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      time.Second,
		IdleTimeout:       time.Second,
		ReadHeaderTimeout: time.Second,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(timeoutCtx); err != nil {
			log.Printf("Server closed with errors: %v", err)
		}
	}()

	log.Printf("Listening on %s", address)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	wg.Wait()
	return nil
}
