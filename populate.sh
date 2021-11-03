#! /bin/bash

addr="http://127.0.0.1:8080/set"
for i in {1..100}; do
  curl "${addr}?key=key-${RANDOM}&value=value-${RANDOM}"
done