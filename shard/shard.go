package shard

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type shard struct {
	Members []string
	NumKeys int
}

type ShardView struct {
	id      int //shard ID of current node...
	shardDB []*shard
}

//Each Node has a shardView, where it can see all the shards, and the members of all the shards/
//It can also see it's own shardID, so we can access that data without a lookup.
func InitShards(owner, shardString, viewOfReplicas string) *ShardView {
	if shardString == "" {
		log.Println("Node started to be added later...")
		return nil
	}
	shardCount, err := strconv.Atoi(shardString)
	if err != nil {
		panic(err)
	}

	var S ShardView
	S.id = -1
	//S.shardDB = make(map[int]*shard)

	replicas := strings.Split(viewOfReplicas, ",")
	if 2*shardCount > len(replicas) { //check minimum length(each shard must have @ least 2)
		log.Println("Shard count too small, ERROR") //throw an error here?
		os.Exit(126)
	}

	shardLen := len(replicas) / shardCount
	//correct length, continue...
	for i := 0; i < shardCount; i++ {
		if len(replicas) >= shardLen {
			shardIPs := replicas[:shardLen]
			replicas = replicas[shardLen:]
			temp := &shard{Members: shardIPs, NumKeys: 0}
			S.shardDB = append(S.shardDB, temp)
			for _, IP := range shardIPs {
				if owner == IP {
					S.id = i + 1
				}
			}
		}
	}
	//if we have leftover replicas...
	if len(replicas) > 0 && len(replicas) < shardCount {
		for i, IP := range replicas {
			temp := &S.shardDB[i].Members
			*temp = append(*temp, IP)
			if owner == IP {
				S.id = i + 1
			}
		}
	}

	log.Print(S.id)
	for _, shard := range S.shardDB{
		log.Println(shard.Members)
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

//gets all active shards in the form of a string
//easy to marshall into json data.

func GetShard(shardID int, s *ShardView) *shard {
	return s.shardDB[shardID-1]
}
func GetShardCount(s *ShardView) string {
	return strconv.Itoa(len(s.shardDB))

}
func GetAllShards(s *ShardView) string {
	shardIDs := make([]string, 0) //apparently if you make a slice like this, it outputs correctly to json?
	//var shardIDs []int
	for i := 0; i < len(s.shardDB); i++ {
		if s.shardDB[i] != nil {
			shardIDs = append(shardIDs, strconv.Itoa(i+1))
		}
	}
	return strings.Join(shardIDs, ",")
}

func GetCurrentShard(s *ShardView) int {
	return s.id
}

func GetMembersOfShard(ID int, s *ShardView) []string {
	return s.shardDB[ID-1].Members
}

func GetNumKeysInShard(ID int, s *ShardView) int {
	return s.shardDB[ID-1].NumKeys
}

func AddKeyToShard(shardID int, s *ShardView){
	log.Printf("ADDING A KEY TO SHARD: %v\n", shardID)
	s.shardDB[shardID-1].NumKeys = s.shardDB[shardID-1].NumKeys + 1
	log.Printf("KEYCOUNT FOR SHARD %v: %v\n", shardID, s.shardDB[shardID-1].NumKeys)
}

func RemoveKeyFromShard(shardID int, s *ShardView){
	s.shardDB[shardID-1].NumKeys = s.shardDB[shardID-1].NumKeys - 1
}

func CopyKeyCount(shardID int, s *ShardView, i int){
	s.shardDB[shardID-1].NumKeys = i
}

func AddNodeToShard(owner string, address string, shardID int, s *ShardView) {
	s.shardDB[shardID-1].Members = append(s.shardDB[shardID-1].Members, address)
	if owner == address {
		s.id = shardID
	}

}


func DoesShardExist(shardID int, s *ShardView) bool {
	if shardID <= len(s.shardDB) {
		if s.shardDB[shardID-1] != nil {
			return true
		}
	}
	return false
}

func GetRandomIPShard(shardID int, s *ShardView) string {
	rand.Seed(time.Now().Unix())
	randRange := len(s.shardDB[shardID-1].Members)
	nodeToGossipWith := s.shardDB[shardID-1].Members[rand.Intn(randRange)]
	return nodeToGossipWith
}
