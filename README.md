# Distributed-KV
Distributed Key-Value store

This project aims to create a simple distributed key-value database with replication.

## Static sharding
A cluster is made up by multiple instances of shard program. Every shard knows about each other thanks to config file. Running multiple instances on different servers with the the same configuration file allows to create the distributed approach.

Every shard will send and receive data from the appropriate shard when a requests comes in.

Shards must have sequential unique id starting from 0. For example, in a 4 nodes cluster, a shard will have 0, 1, 2 or 3 as an ID, and it must be specified when starting them.

## Inter-shard communication
Intershard communication is implemented with UDP multicast address.

## Resharding 
Resharding means redistributing keys when the cluster size changes. In order to reshard, user must stop the cluster and update the configuration file with new shards.

There can be two cases:
1. **Increase** of cluster size: in this case admin must hit all old shards on reshard endpoint. 
2. **Decrease** of cluster size: in this case one can save all the keys from a backup and use a script to fill the new cluster.
