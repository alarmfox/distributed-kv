#!/bin/bash
set -e

trap 'killall shard' SIGINT

cd $(dirname $0)

killall shard || true
sleep 0.1

go build -v -o bin/shard cmd/shard/main.go

bin/shard -db-location=/tmp/sh1.db -listen-addr=127.0.0.1:8080 -name=sh1 -config-file=shards.json &
bin/shard -db-location=/tmp/sh2.db -listen-addr=127.0.0.2:8080 -name=sh2 -config-file=shards.json &
# bin/shard -db-location=/tmp/sh3.db -listen-addr=127.0.0.1:8082 -name=sh3 -config-file=shards.json &
# bin/shard -db-location=/tmp/sh4.db -listen-addr=127.0.0.1:8083 -name=sh4 -config-file=shards.json &

wait