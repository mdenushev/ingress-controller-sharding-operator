package controller

import (
	"fmt"

	"github.com/cespare/xxhash"
)

// hashShardSelector assigns a parent to a shard by hashing its namespace.
// All parents of one namespace land on the same shard.
type hashShardSelector struct {
	maxShards map[string]int
}

func (s *hashShardSelector) ShardsFor(sharded ShardedObject, useAllShards bool) ([]Shard, bool, error) {
	return getShardInfo(sharded.GetNamespace(), sharded.GetIngressClassName(), s.maxShards, useAllShards)
}

func getShardInfo(name, className string, shardSettings map[string]int, useAllShards bool) ([]Shard, bool, error) {
	maxShards, ok := shardSettings[className]
	if !ok {
		return []Shard{{Number: 0, Name: className}}, true, fmt.Errorf("shard type %s not found in config", className)
	}
	if maxShards == 0 {
		return []Shard{{Number: 0, Name: className}}, true, nil
	}

	var shards []Shard

	if useAllShards {
		for i := 0; i < maxShards; i++ {
			shards = append(shards, Shard{Number: i, Name: fmt.Sprintf("%s-%d", className, i)})
		}
		return shards, false, nil
	}

	hash := xxhash.Sum64String(name)
	shardNumber := int(hash % uint64(maxShards))
	shardName := fmt.Sprintf("%s-%d", className, shardNumber)
	return []Shard{{Number: shardNumber, Name: shardName}}, false, nil
}
