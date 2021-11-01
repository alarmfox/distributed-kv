#! /bin/bash

addr="localhost:8080"
for i in {1..1000}; do
  key=$RANDOM
  curl "http://${addr}/set?key=key-${key}&value=value-${RANDOM}"
  echo "key-$key"
done