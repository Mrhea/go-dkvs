package shard

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type shard struct {
	Members []string
	numKeys int
}

type ShardView struct {
	ID             int //shard ID of current node...
	ShardDB        []*shard
	NumKeysInShard int
}

//Each Node has a shardView, where it can see all the shards, and the members of all the shards/
//It can also see it's own shardID, so we can access that data without a lookup.
func InitShards(owner, shardString, viewOfReplicas string) *ShardView {
	shardCount, err := strconv.Atoi(shardString)
	if err != nil {
		panic(err)
	}
	var S ShardView
	//S.shardDB = make(map[int]*shard)

	replicas := strings.Split(viewOfReplicas, ",")
	if 2*shardCount > len(replicas) { //check minimum length(each shard must have @ least 2)
		log.Println("Shard count too small, ERROR") //throw an error here?
		os.Exit(126)
	}

	shardLen := len(replicas) / shardCount
	//correct length, continue...
	for i := 1; i <= shardCount; i++ {
		if len(replicas) >= shardLen {
			shardIPs := replicas[:shardLen]
			replicas = replicas[shardLen:]
			temp := &shard{Members: shardIPs, numKeys: 0}
			S.ShardDB = append(S.ShardDB, temp)
			for _, IP := range shardIPs {
				if owner == IP {
					S.ID = i
				}
			}
		}
	}
	//if we have leftover replicas...
	if len(replicas) > 0 && len(replicas) < shardCount {
		for i, IP := range replicas {
			temp := &S.ShardDB[i].Members
			*temp = append(*temp, IP)
			if owner == IP {
				S.ID = i
			}
		}
	}
	return &S
}

func Reshard(shardCount int, s *ShardView) {
	/*
		How do we implement this? We'd have to decide which kvs values go where...
		It'd probably be easiest to figure out which IPs aren't in any shards, and
		append them one by one to the smallest shard. So:
		1. Locate smallest shard
		2. Append new IP
		3. Copy all KVS
		4. Repeat until all IP's are in a shard
		(Don't delete this ^, add to mechanisms.txt)
	*/
}

//gets all active shards in the form of an int list.
//easy to marshall into json data.
func GetAllShards(s *ShardView) []int {
	shardIDs := make([]int, 0) //apparently if you make a slice like this, it outputs correctly to json?
	//var shardIDs []int
	for i := 1; i <= len(s.ShardDB); i++ {
		if s.ShardDB[i] != nil {
			shardIDs = append(shardIDs, i)
		}
	}
	return shardIDs
}

func GetCurrentShard(s *ShardView) int {
	return s.ID
}

func GetMembersOfShard(ID int, s *ShardView) []string {
	return s.ShardDB[ID].Members
}

func GetNumKeys(s *ShardView) int {
	return 0
	//I'm not sure if I want to keep track of this data in the shard...
}
