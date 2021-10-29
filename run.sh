#!/bin/bash
set -e

trap 'killall shard' SIGINT

cd $(dirname $0)

killall shard || true
sleep 0.1

go build -v -o ~/go/bin/shard cmd/shard/main.go

~/go/bin/shard -db-location=/tmp/sh1.db -listen-addr=127.0.0.1:8080 -name=sh1 -config-file=shards.json &

wait