package shard

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type shard struct {
	members []string
	numKeys int //probably don't need this?
}

type ShardView struct {
	id int //shard ID of current node...
	shardDB []*shard
}
 //Each Node has a shardView, where it can see all the shards, and the members of all the shards/
 //It can also see it's own shardID, so we can access that data without a lookup.
func InitShards(owner, shardString, viewOfReplicas string) *ShardView {
	shardCount, err := strconv.Atoi(shardString)
	if err != nil{
		panic(err)
	}
	var S ShardView
	//S.shardDB = make(map[int]*shard)

	replicas := strings.Split(viewOfReplicas, ",")
	if 2*shardCount > len(replicas) { //check minimum length(each shard must have @ least 2)
		log.Println("Shard count too small, ERROR") //throw an error here?
		os.Exit(126)
	}
	//correct length, continue...
	for i := 1; i <= shardCount; i++ {
		if len(replicas) >= 2 {
			var ip1 string
			var ip2 string
			ip1, ip2, replicas = replicas[0], replicas[1], replicas[2:]
			temp := &shard{members:[], numKeys:0}
			temp.members = append(temp.members, ip1)
			temp.members = append(temp.members, ip2)
			S.shardDB = append(S.shardDB, temp) //is this the right way of doing this?
			if owner == ip1 || owner == ip2 {
				S.id = i
			}
		} else if len(replicas) == 1{
			ip3 := replicas[0]
			temp := &S.shardDB[i-1].members
			*temp = append(*temp, ip3)
			if owner == ip3 {
				S.id = i-1
			}
		}
	}
	//we are now correctly sharded in either groups of 2,
	//with one group of 3 for odd lengths
	//this can change, if we want larger default groups...
	return &S
}

func Reshard(shardCount int, s *ShardView){
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
	for i := 1; i <= len(s.shardDB); i++{
		if s.shardDB[i] != nil {
			shardIDs = append(shardIDs, i)
		}
	}
	return shardIDs
}

func GetCurrentShard(s *ShardView) int {
	return s.id
}

func GetMembersOfShard(ID int, s *ShardView) []string {
	return s.shardDB[ID].members
}

func GetNumKeys(s *ShardView) int {
	//I'm not sure if I want to keep track of this data in the shard...
}