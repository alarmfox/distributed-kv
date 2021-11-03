#!/bin/bash
set -e

trap 'killall shard' SIGINT

cd $(dirname $0)

killall shard || true
sleep 0.1

go build -v -o bin/shard cmd/shard/shard.go

bin/shard -db-location=/tmp/sh1.db -listen-addr=127.0.0.1:8080 -id=0 &
bin/shard -db-location=/tmp/sh2.db -listen-addr=127.0.0.2:8080 -id=1 &

wait