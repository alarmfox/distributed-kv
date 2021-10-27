#!/bin/bash
set -e

trap 'killall shard' SIGINT

cd $(dirname $0)

killall shard || true
sleep 0.1

go install -v cmd/shard/shard.go

~/go/bin/shard -db-location=/tmp/sh1.db -listen-addr=127.0.0.1:8080 -name=sh1 &
~/go/bin/shard -db-location=/tmp/sh2.db -listen-addr=127.0.0.1:8081 -name=sh2 &
~/go/bin/shard -db-location=/tmp/sh3.db -listen-addr=127.0.0.1:8082 -name=sh3 &

wait