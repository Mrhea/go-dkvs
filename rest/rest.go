package rest

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	gsp "github.com/mrhea/CMPS128_Assignment4/gossip"
	"github.com/mrhea/CMPS128_Assignment4/kvs"
	"github.com/mrhea/CMPS128_Assignment4/shard"
	"github.com/mrhea/CMPS128_Assignment4/structs"
	"github.com/mrhea/CMPS128_Assignment4/view"
)

//const NULL int = -999

type server struct {
	db      *kvs.Database
	V       *view.View
	S       *shard.ShardView
	stalled []*kvs.Entry
}

//======================================================================================================================
//==========================================KEY-VALUE-STORE OPERATIONS==================================================
//======================================================================================================================

// node is a server type that contains a database and view of
// the replicas in the subnet.
var node = new(server)

// Get an Entry.
func getEntry(w http.ResponseWriter, r *http.Request) {
	// May want to differentiate between getting the value of a key GET and
	// checking if the key exists GET
	log.Println("REST: Handling GET request")
	w.Header().Set("Content-Type", "application/json")

	// Extract key from url
	params := mux.Vars(r)
	// var e kvs.Entry
	// _ = json.NewDecoder(r.Body).Decode(&e)
	// e.Key = params["key"]
	// computeHashIDAndShardKey(e.Key, r.Method)

	// Handles if key exists in KVS
	// if true return the value associated with key
	// if false handle non-existing key

	if kvs.CheckIfKeyExists(params["key"], node.db) {
		e := kvs.GetEntryStruct(params["key"], node.db)
		log.Println("REST: GET -> Key exists returning key-value pair")
		exists := structs.Get{Message: "Retrieved successfully", Version: e.Version, Meta: e.Version, Value: e.Val}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(exists)
	} else {
		// key does not exist
		log.Println("REST: GET -> Key does not exist ... Returning error")
		exists := structs.GetError{Error: "Key does not exist", Message: "Error in GET"}
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(exists)

	}
}

// Put an Entry.
func putEntry(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling PUT request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	var e kvs.Entry
	_ = json.NewDecoder(r.Body).Decode(&e)
	e.Key = params["key"]

	// computeHashIDAndShardKey(e.Key, r.Method)

	// Missing value in key-val pair, returns error - 400
	if e.Val == "" { //not sure how to represent empty other than 0 for ints...
		log.Println("REST: PUT -> Value not found... Sending bad request")
		missing := structs.PutError{Error: "Value is missing", Message: "Error in PUT"}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(missing)
		return
	}
	// Key length too long in key-val pair, returns error - 400
	if len(e.Key) > 50 {
		log.Println("REST: PUT -> Key too long... Sending bad request")
		tooLong := structs.PutError{Error: "Key is too long", Message: "Error in PUT"}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(tooLong)
		return
	}
	//As of now, we assume our request is valid
	if len(e.Meta) == 0 {
		// Test script doesn't send back metadata, so we force it on them
		if kvs.GetVer(node.db) > 0 {
			e.Version = kvs.GetVer(node.db) + 1
		} else {
			e.Version = 1 //If metadata is empty, start version at 1
			log.Println("IS THIS CALLED")
		}
	} else {
		e.Version = e.Meta[len(e.Meta)-1] + 1 //last version # in the slice, incremented.
	}
	e.Meta = append(e.Meta, e.Version)

	//if the current request version is not the immediate next version, we simply queue it and move on...
	log.Printf("e.Version = %v\n", e.Version)
	log.Printf("database Latest Version = %v\n", kvs.GetVer(node.db))
	if !(e.Version-1 == kvs.GetVer(node.db)) {
		log.Println("REST: PUT -> Causality not met, stalling...")
		node.stalled = append(node.stalled, &e)
		failed := structs.Stall{Error: "Error in PUT", Message: "Causality not met"}
		w.WriteHeader(http.StatusFailedDependency)
		json.NewEncoder(w).Encode(failed)
		return
	}

	kvs.UpdateVer(e.Version, node.db)

	// Grab key shard id for responses
	keyShardID := shard.GetCurrentShard(node.S)

	// Replaces value in key-val pair, returns success - 200
	if kvs.CheckIfKeyExists(e.Key, node.db) {
		log.Println("REST: PUT -> Key already exits... Replacing")
		kvs.RemoveEntry(e.Key, node.db)
		kvs.InsertEntry(e, node.db)
		success := structs.Put{Message: "Updated successfully", Replaced: true, Version: e.Version, Meta: e.Meta, KeyShardID: strconv.Itoa(keyShardID)}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(success)
		goto Success

	} else {
		// Adds new key-value pair, returns success - 201
		log.Println("REST: PUT -> Key does not exist... Adding")
		kvs.InsertEntry(e, node.db)
		if len(e.Meta) == 1 {
			success := structs.Put{Message: "Added successfully", Replaced: false, Version: e.Version, Meta: e.Meta, KeyShardID: strconv.Itoa(keyShardID)}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(success)
		} else {
			success := structs.Put{Message: "Added successfully", Replaced: false, Version: e.Version, Meta: e.Meta, KeyShardID: strconv.Itoa(keyShardID)}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(success)
		}
		goto Success

	}

Success:
	shardID := shard.GetCurrentShard(node.S)
	shardIPs := shard.GetMembersOfShard(shardID, node.S)
	shard.AddKeyToShard(shardID, node.S)
	for _, IP := range shardIPs {
		if IP != node.V.Owner {
			log.Printf("REPLICATING TO: %v\n", IP)
			client := &http.Client{}
			url := "http://" + IP + "/replicate/" + e.Key
			reqData, _ := json.Marshal(e)
			req, err := http.NewRequest(r.Method, url, bytes.NewBuffer(reqData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			b, _ := ioutil.ReadAll(resp.Body)
			var rspStruct structs.ReplicaResponse
			_ = json.Unmarshal(b, &rspStruct)

			//We don't necessarily need to write this data to the client...
			log.Println(rspStruct.Message)
		}
	}
	for _, IP := range node.V.View {
		if IP != node.V.Owner {
			log.Printf("REPLICATING VERSION TO: %v\n", IP)
			client := &http.Client{}
			url := "http://" + IP + "/update"
			temp := structs.VersionCopy{Version:e.Version}
			reqData, _ := json.Marshal(temp)
			req, err := http.NewRequest("PUT", url, bytes.NewBuffer(reqData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			b, _ := ioutil.ReadAll(resp.Body)
			var rspStruct structs.ReplicaResponse
			_ = json.Unmarshal(b, &rspStruct)

			//We don't necessarily need to write this data to the client...

		}
	}
}

func putForward(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling PUT replication")
	w.Header().Set("Content-Type", "application/json")
	var e kvs.Entry
	_ = json.NewDecoder(r.Body).Decode(&e)

	if !(e.Version-1 == kvs.GetVer(node.db)) {
		log.Println("REST: PUTFORWARD -> Causality not met, stalling...")
		node.stalled = append(node.stalled, &e)
		failed := structs.Stall{Error: "Error in PUT", Message: "Causality not met"}
		w.WriteHeader(http.StatusFailedDependency)
		json.NewEncoder(w).Encode(failed)
		return
	}

	kvs.UpdateVer(e.Version, node.db)
	if kvs.CheckIfKeyExists(e.Key, node.db) {
		log.Println("REST: PUTFORWARD -> Key already exits... Replacing")
		kvs.RemoveEntry(e.Key, node.db)
		kvs.InsertEntry(e, node.db)
		success := structs.ReplicaResponse{Message: "Replicated successfully", Version: e.Version}
		w.WriteHeader(http.StatusOK)
		shard.AddKeyToShard(shard.GetCurrentShard(node.S), node.S)
		json.NewEncoder(w).Encode(success)
	} else {
		log.Println("REST: PUTFORWARD -> Key does not exist... Adding")
		kvs.InsertEntry(e, node.db)
		success := structs.ReplicaResponse{Message: "Replicated successfully", Version: e.Version}
		shard.AddKeyToShard(shard.GetCurrentShard(node.S), node.S)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(success)
	}

}

// Delete an entry.
func deleteEntry(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling DELETE request")
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r) // Get params

	var metadata kvs.Entry
	_ = json.NewDecoder(r.Body).Decode(&metadata)
	log.Println(metadata.Meta)

	e := kvs.GetEntryStruct(params["key"], node.db)

	// e.Key = params["key"]
	// computeHashIDAndShardKey(e.Key, r.Method)

	if kvs.CheckIfKeyExists(params["key"], node.db) {
		kvs.EraseEntry(params["key"], node.db)
		e.Version = e.Meta[len(e.Meta)-1] + 1
		e.Meta = append(e.Meta, e.Version)
		//copied from put requests
		if !(e.Version-1 == kvs.GetVer(node.db)) {
			log.Println("REST: PUT -> Causality not met, stalling...")
			node.stalled = append(node.stalled, &e)
			failed := structs.Stall{Error: "Error in DELETE", Message: "Causality not met"}
			w.WriteHeader(http.StatusFailedDependency)
			json.NewEncoder(w).Encode(failed)
			return
		}

		kvs.UpdateVer(e.Version, node.db)
		log.Println("REST: DELETE -> Key deleted from KVS... Sending success response!")
		success := structs.Delete{DoesExist: true, Message: "Deleted successfully",
			Version: e.Version, Meta: e.Meta}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(success)
		goto Success

	} else {
		log.Println("REST: DELETE -> Key does NOT Exist in KVS... Sending failed response!")
		failed := structs.DeleteError{DoesExist: false, Error: "Key does not exist",
			Message: "Error in DELETE"}
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(failed)
		return
	}
Success:
	shardID := shard.GetCurrentShard(node.S)
	shardIPs := shard.GetMembersOfShard(shardID, node.S)
	shard.RemoveKeyFromShard(shardID, node.S)
	for _, IP := range shardIPs {
		if IP != node.V.Owner {
			client := &http.Client{}
			url := "http://" + IP + "/replicate/" + params["key"]
			reqData, _ := json.Marshal(e)
			req, err := http.NewRequest(r.Method, url, bytes.NewBuffer(reqData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			b, _ := ioutil.ReadAll(resp.Body)
			var rspStruct structs.ReplicaResponse
			_ = json.Unmarshal(b, &rspStruct)

			//We don't necessarily need to write this data to the client...
			log.Println(rspStruct.Version)
			log.Println(rspStruct.Message)
		}
	}
	for _, IP := range node.V.View {
		if IP != node.V.Owner {
			client := &http.Client{}
			url := "http://" + IP + "/replicate/version"
			temp := structs.VersionCopy{Version:e.Version}
			reqData, _ := json.Marshal(temp)
			req, err := http.NewRequest("PUT", url, bytes.NewBuffer(reqData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			b, _ := ioutil.ReadAll(resp.Body)
			var rspStruct structs.ReplicaResponse
			_ = json.Unmarshal(b, &rspStruct)

			//We don't necessarily need to write this data to the client...
			log.Println(rspStruct.Version)
			log.Println(rspStruct.Message)

		}
	}
}

func deleteForward(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling DELETE replication")
	w.Header().Set("Content-Type", "application/json")
	var e kvs.Entry
	_ = json.NewDecoder(r.Body).Decode(&e)

	//copied from put requests
	if !(e.Version-1 == kvs.GetVer(node.db)) {
		log.Println("REST: PUT -> Causality not met, stalling...")
		node.stalled = append(node.stalled, &e)
		failed := structs.Stall{Error: "Error in PUT", Message: "Causality not met"}
		w.WriteHeader(http.StatusFailedDependency)
		json.NewEncoder(w).Encode(failed)
		return
	}

	kvs.UpdateVer(e.Version, node.db)
	kvs.EraseEntry(e.Key, node.db)
	log.Println("REST: DELETE -> Key deleted from KVS... Sending success response!")
	success := structs.ReplicaResponse{Message: "Replicated successfully", Version: e.Version}
	shard.RemoveKeyFromShard(shard.GetCurrentShard(node.S), node.S)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(success)

}

// GetAllEntries encodes every Entry.
func GetAllEntries(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	entries := kvs.ConvertMapToSlice(node.db)
	json.NewEncoder(w).Encode(entries)
}

//======================================================================================================================
//======================================================================================================================
//======================================================================================================================

//======================================================================================================================
//==================================================VIEW OPERATIONS=====================================================
//======================================================================================================================

// GetView reads it's receipient replica's view of the
// key-value store.
func getView(w http.ResponseWriter, r *http.Request) {
	log.Println("VIEW: Handling GET request")
	w.Header().Set("Content-Type", "application/json")

	viewString := strings.Join(node.V.View, ",")

	//viewData, err := json.Marshal(viewString)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//log.Println(string(viewData))
	viewResponse := structs.ViewGet{Message: "View Retrieved Successfully",
		View: viewString}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(viewResponse)
}

// PutView adds another replica to its receipient replica's
// view.
func putView(w http.ResponseWriter, r *http.Request) {
	log.Println("VIEW: Handling PUT request")

	w.Header().Set("Content-Type", "application/json")
	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	// Check if replica exists in view
	// If it does exist in local view do nothing and return error
	if view.CheckIfReplicaExists(rep.Address, node.V) {
		log.Println("VIEW ERROR: PUT -> Replica already exits... Error")

		exists := structs.PutError{Message: "Error in PUT", Error: "Socket address already exists in the view"}
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(exists)
	} else {
		// If it does not exist in local view, add it to view
		// Adds new replica to the view, returns success - 201
		log.Println("VIEW: PUT -> Replica does not exist... Adding")
		view.AddReplicaToView(rep.Address, node.V)
		success := structs.ViewPut{Message: "Replica added successfully to the view"}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(success)
		for _, IP := range node.V.View {
			if IP != node.V.Owner {
				client := &http.Client{}
				url := "http://" + IP + "/replicate/view/"
				reqData, _ := json.Marshal(rep)
				req, err := http.NewRequest(r.Method, url, bytes.NewBuffer(reqData))
				if err != nil {
					panic(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					panic(err)
				}
				b, _ := ioutil.ReadAll(resp.Body)
				var rspStruct structs.ReplicaResponse
				_ = json.Unmarshal(b, &rspStruct)

				//We don't necessarily need to write this data to the client...
				log.Println(rspStruct.Version)
				log.Println(rspStruct.Message)
			}
		}
	}

}

func putViewForward(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling VIEW-PUT replication")
	w.Header().Set("Content-Type", "application/json")

	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	view.AddReplicaToView(rep.Address, node.V)

	success := structs.ViewReplica{Message: "Replication Successful"}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(success)

}

// DeleteView requests that a replica is delete
// from it's receipient replica's view.
func deleteView(w http.ResponseWriter, r *http.Request) {
	log.Println("VIEW: Handling DELETE request")

	w.Header().Set("Content-Type", "application/json")

	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	log.Println(rep.Address)

	if view.CheckIfReplicaExists(rep.Address, node.V) {
		log.Println("VIEW: DELETE -> Replica exists... Deleting")
		view.DeleteReplica(rep.Address, node.V)
		success := structs.ViewDelete{Message: "Replica deleted successfully from view"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(success)
		for _, IP := range node.V.View {
			if IP != node.V.Owner {
				client := &http.Client{}
				url := "http://" + IP + "/replicate/view/"
				reqData, _ := json.Marshal(rep)
				req, err := http.NewRequest(r.Method, url, bytes.NewBuffer(reqData))
				if err != nil {
					panic(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					panic(err)
				}
				b, _ := ioutil.ReadAll(resp.Body)
				var rspStruct structs.ReplicaResponse
				_ = json.Unmarshal(b, &rspStruct)

				//We don't necessarily need to write this data to the client...
				log.Println(rspStruct.Version)
				log.Println(rspStruct.Message)
			}
		}
		return
	}
	log.Println("VIEW:DELETE -> Replica does not exist, can't delete!")
	failure := structs.ViewDeleteError{Error: "Socket address does not exist in view",
		Message: "Error in DELETE"}
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(failure)
	return
}

func putDeleteForward(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling VIEW-DELETE replication")
	w.Header().Set("Content-Type", "application/json")

	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	view.DeleteReplica(rep.Address, node.V)

	success := structs.ViewReplica{Message: "Replication Successful"}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(success)
}

//======================================================================================================================
//======================================================================================================================
//======================================================================================================================

//======================================================================================================================
//===============================================SHARDING OPERATIONS====================================================
//======================================================================================================================

func getShardIDsOfStore(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD-VIEW request")
	w.Header().Set("Content-Type", "application/json")

	shardIds := shard.GetAllShards(node.S)

	resp := structs.ShardIDs{Message: "Shard IDs retrieved successfully", ShardIDs: shardIds}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)

}

func getShardID(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD request")
	w.Header().Set("Content-Type", "application/json")

	shardID := strconv.Itoa(shard.GetCurrentShard(node.S))

	resp := structs.NodeShardID{Message: "Shard ID of the node retrived successfully", ShardID: shardID}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func getShardMembers(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD-MEMBERS request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	shardID, _ := strconv.Atoi(params["ID"])

	shardView := shard.GetMembersOfShard(shardID, node.S)
	viewString := strings.Join(shardView, ",")

	log.Printf("REST: Members of shard: %s", viewString)
	resp := structs.ShardMembers{Message: "Members of shard ID retrieved successfully", ShardIDMembers: viewString}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)
}

func getShardKeyCount(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD-KEY-COUNT request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	shardID, _ := strconv.Atoi(params["ID"])

	IP := shard.GetRandomIPShard(shardID, node.S)
	url := "http://" + IP + "/forward/numKeys/" + params["ID"]
	client := &http.Client{}
	req,err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	b, _ := ioutil.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	w.Write(b)
}

func forwardKeyCount(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD-KEY-COUNT request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	shardID, _ := strconv.Atoi(params["ID"])

	log.Println(shardID)

	temp := shard.GetShard(shardID, node.S)
	shardIDCount := temp.NumKeys

	log.Println(shardIDCount)

	resp := structs.ShardKeyCount{Message: "Key count of shard ID retrieved",
		ShardIDKeyCount: shardIDCount}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)

}

func addNodeToShard(w http.ResponseWriter, r *http.Request) {
	//THIS is a WIP it seems like it will be way more complicated than we thought
	log.Println("REST: Handling ADD-NODE-TO-SHARD request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	shardID, _ := strconv.Atoi(params["ID"])
	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	shard.AddNodeToShard(node.V.Owner, rep.Address, shardID, node.S)
	shardIPs := shard.GetMembersOfShard(shardID, node.S)
	if node.V.Owner == rep.Address {
		for _, IP := range shardIPs {
			if IP != node.V.Owner {
				client := &http.Client{Timeout: 25 * time.Second}
				url := "http://" + IP + "/key-value-store/"
				// Sends a GET request
				req, err := http.NewRequest("GET", url, nil)

				if err != nil {
					panic(err)
				}
				// The response should be a slice of entries
				resp, err := client.Do(req)

				if err != nil {
					panic(err)
				}
				//time.Sleep(5 * time.Second)
				// This adds entries to db
				b, _ := ioutil.ReadAll(resp.Body)
				entries := kvs.Transfer{}
				json.Unmarshal(b, &entries)
				kvs.AddAllKVPairs(entries, node.db)
				break
			}
		}
	}
	resp := structs.AddedNodeToShard{Message: "Node successfully added to shard"}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)
	for _, IP := range node.V.View {
		if IP != node.V.Owner {
			client := &http.Client{}
			url := "http://" + IP + "/replicate/add-member/" + params["ID"]
			reqData, _ := json.Marshal(rep)
			req, err := http.NewRequest(r.Method, url, bytes.NewBuffer(reqData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			b, _ := ioutil.ReadAll(resp.Body)
			var rspStruct structs.AddedNodeToShard
			_ = json.Unmarshal(b, &rspStruct)

			log.Println(rspStruct.Message)
		}
	}

}

func addNodeToShardForward(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling ADD-NODE-TO-SHARD request")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	shardID, _ := strconv.Atoi(params["ID"])
	var rep structs.Replica
	_ = json.NewDecoder(r.Body).Decode(&rep)

	shard.AddNodeToShard(node.V.Owner, rep.Address, shardID, node.S)

	shardIPs := shard.GetMembersOfShard(shardID, node.S)
	if node.V.Owner == rep.Address {
		for _, IP := range shardIPs {
			if IP != node.V.Owner {
				client := &http.Client{Timeout: 25 * time.Second}
				url := "http://" + IP + "/key-value-store/"
				// Sends a GET request
				req, err := http.NewRequest("GET", url, nil)

				if err != nil {
					panic(err)
				}
				// The response should be a slice of entries
				resp, err := client.Do(req)

				if err != nil {
					panic(err)
				}
				//time.Sleep(5 * time.Second)
				// This adds entries to db
				b, _ := ioutil.ReadAll(resp.Body)
				entries := kvs.Transfer{}
				json.Unmarshal(b, &entries)
				kvs.AddAllKVPairs(entries, node.db)
				break
			}
		}
	}

	resp := structs.AddedNodeToShard{Message: "Node successfully added to shard"}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)
}

func getShardInfo(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling GET-SHARD-COUNT request")
	w.Header().Set("Content-Type", "application/json")

	count := shard.GetShardCount(node.S)   //accessor
	view := strings.Join(node.V.View, ",") //non accessor

	resp := structs.GetShardInfo{ShardCount: count, ModifiedView: view}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)

}
func addForward(w http.ResponseWriter, r *http.Request) {
}

func reshard(w http.ResponseWriter, r *http.Request) {

}
func keyDistribute(w http.ResponseWriter, r *http.Request) {
	log.Println("REST: Handling Key Distribution")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	key := params["key"]
	shardCount, _ := strconv.Atoi(shard.GetShardCount(node.S))

	shardID := (int(crc32.ChecksumIEEE([]byte(key))) % shardCount) + 1 //returns 1, 2, 3, ... ShardCount

	log.Printf("SHARDID FOR THIS OPERATION: %v\n", shardID)

	if shard.DoesShardExist(shardID, node.S) {
		IP := shard.GetRandomIPShard(shardID, node.S)
		url := "http://" + IP + "/kvs/" + params["key"]
		client := &http.Client{}
		req, err := http.NewRequest(r.Method, url, r.Body)
		if err != nil {
			log.Println("THIS IS WHERE WE PANIC - 684")
			panic(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Forwarding shard request couldn't be fulfilled")
			panic(err)
		}
		b, _ := ioutil.ReadAll(resp.Body)
		w.WriteHeader(resp.StatusCode)
		w.Write(b)
		log.Printf("forwarded response: %v", b)
	} else {
		log.Println("Shard ID is invalid.")
		// If shard ID is invalid, return Internal Server Error
		fail := structs.InternalError{InternalServerError: "Unknown error. Retry connection."}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(fail)
	}
}

func putVersion(w http.ResponseWriter, r *http.Request){
	log.Println("REST: Handling Version Updates")
	w.Header().Set("Content-Type", "application/json")

	var rep structs.VersionCopy
	_ = json.NewDecoder(r.Body).Decode(&rep)

	kvs.UpdateVer(rep.Version, node.db)
	success := structs.ViewReplica{Message: "Replication Successful"}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(success)

}

//======================================================================================================================
//======================================================================================================================
//======================================================================================================================

//======================================================================================================================
//==============================================STARTUP OPERATIONS======================================================
//======================================================================================================================

func lateInitShard() {
	randomIP, _ := view.GetRandomNode(node.V) //grabs a random replica to copy shardVIEW from
	client := &http.Client{}
	log.Println(randomIP)
	//first we need shardCount...
	url1 := "http://" + randomIP + "/key-value-store-shard/get-info"
	req, err := http.NewRequest("GET", url1, nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	b, _ := ioutil.ReadAll(resp.Body)
	var respS structs.GetShardInfo
	_ = json.Unmarshal(b, &respS)

	shardCount := respS.ShardCount
	modifiedView := respS.ModifiedView

	log.Println("debug info")
	log.Println(shardCount)
	log.Println(modifiedView)

	node.S = shard.InitShards(node.V.Owner, shardCount, modifiedView)
}

// Announce should be called upon node startup. Broadcasts
// a view PUT request to subnet to enable other replicas to add
// the owner node to their view. Afterwards, perform a view Get
// request to retrive all key-value pairs and store them in its
// database.
func announce() {
	// --
	// This part is going through the view, sending a view-put request to each replica
	//time.Sleep(20 * time.Second)
	rep := structs.Replica{Address: node.V.Owner}
	IP, _ := view.GetRandomNode(node.V)

	client := &http.Client{}
	url := "http://" + IP + "/key-value-store-view"
	reqData, _ := json.Marshal(rep)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(reqData))
	if err != nil {
		log.Print("Will fail on startup.")
	}
	_, err = client.Do(req)

	// Now that view-puts are done we get all xkv pairs from one random node
	// --/*
	time.Sleep(1 * time.Second)
	// --
	// Now we move to get all kv pairs from a random replica
	// We go through the list of replicas just incase the one we pick is down
	// we immediately go to another replica, if it's not we just continue

	//THIS IS CURRENTLY NOT NEEDED FOR ASSIGNMENT 4...
	//for _, IP := range node.V.View {
	//	if IP != node.V.Owner {
	//		client := &http.Client{Timeout: 25 * time.Second}
	//		url := "http://" + IP + "/key-value-store/"
	//		// Sends a GET request
	//		req, err := http.NewRequest("GET", url, nil)
	//
	//		if err != nil {
	//			panic(err)
	//		}
	//		// The response should be a slice of entries
	//		resp, err := client.Do(req)
	//
	//		if err != nil {
	//			panic(err)
	//		}
	//		time.Sleep(5 * time.Second)
	//		// This adds entries to db
	//		b, _ := ioutil.ReadAll(resp.Body)
	//		entries := kvs.Transfer{}
	//		json.Unmarshal(b, &entries)
	//		kvs.AddAllKVPairs(entries, node.db)
	//		break
	//	}
	//}
	return
}

// Use this endpoint to test the get function in announce
// start up 8082, 8083, 8084
// Add a kv pair
// strat up 8085
// hit the fetch endpoint to trigger 8085 to get all kv pairs
// fetchEntries - a test endpoint that will trigger a GET call
// to a random replica to retrieve kv pairs
func fetchEntries(w http.ResponseWriter, r *http.Request) {
	log.Println("FETCH-TEST: Testing fetch endpoint")
	w.Header().Set("Content-Type", "application/json")

	exists := structs.PutError{Message: "Error in PUT", Error: "Socket address already exists in the view"}
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(exists)

	client := &http.Client{Timeout: 25 * time.Second}
	url := "http://10.10.0.3:8080/key-value-store/"
	// Sends a GET request
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {

	}
	log.Println("FETCH-TEST: Sending GET request to 8083 now")
	// The response should be a slice of entries
	resp, err := client.Do(req)

	if err != nil {
		panic(err)
	}
	time.Sleep(5 * time.Second)
	// this should print the slice of entries
	// log.Println("Response from FETCH-TEST: GET KVS request to a replica for keys: ", resp.Body)
	// ***************
	// This adds entries to db
	b, _ := ioutil.ReadAll(resp.Body)
	entries := kvs.Transfer{}
	json.Unmarshal(b, &entries)
	log.Println("Response from FETCH-TEST: GET KVS request to a replica for keys: ", entries.Entries)
	kvs.AddAllKVPairs(entries, node.db)
}

// InitServer setups a RESTful-accessible API.
func InitServer(socket, viewString, shardCount string) {
	time.Sleep(5 * time.Second)
	log.Println("REST: Initializing a new server node")
	// Init router
	log.Println("REST: Initializing a new router")
	r := mux.NewRouter()

	// Init view
	log.Println("REST: Initializing VIEW for router")
	node.V = view.InitView(socket, viewString)

	// Init shards
	log.Println("REST: Initializing SHARDS for router")
	node.S = shard.InitShards(socket, shardCount, viewString)
	if node.S == nil {
		lateInitShard()
	}

	// Init database
	log.Println("REST: Initializing DATABASE for router")
	node.db = kvs.InitDB()

	// Forwarding Handlers / Endpoints
	r.HandleFunc("/replicate/{key}", putForward).Methods("PUT")
	r.HandleFunc("/replicate/{key}", deleteForward).Methods("DELETE")

	r.HandleFunc("/replicate/view/", putViewForward).Methods("PUT")
	r.HandleFunc("/replicate/view/", putDeleteForward).Methods("DELETE")

	r.HandleFunc("/update", putVersion).Methods("PUT")

	r.HandleFunc("/replicate/add-member/{ID}", addNodeToShardForward).Methods("PUT")

	// Router Handlers / Endpoints
	r.HandleFunc("/key-value-store/{key}", keyDistribute).Methods("GET", "PUT", "DELETE")

	r.HandleFunc("/kvs/{key}", getEntry).Methods("GET")
	r.HandleFunc("/kvs/{key}", putEntry).Methods("PUT")
	r.HandleFunc("/kvs/{key}", deleteEntry).Methods("DELETE")

	// View Handlers / Endpoints
	r.HandleFunc("/key-value-store-view", getView).Methods("GET")
	r.HandleFunc("/key-value-store-view", putView).Methods("PUT")
	r.HandleFunc("/key-value-store-view", deleteView).Methods("DELETE")

	// Shard Handlers / Endpoints
	r.HandleFunc("/key-value-store-shard/shard-ids", getShardIDsOfStore).Methods("GET")
	r.HandleFunc("/key-value-store-shard/node-shard-id", getShardID).Methods("GET")
	r.HandleFunc("/key-value-store-shard/shard-id-members/{ID}", getShardMembers).Methods("GET")
	r.HandleFunc("/key-value-store-shard/shard-id-key-count/{ID}", getShardKeyCount).Methods("GET")
	r.HandleFunc("/key-value-store-shard/add-member/{ID}", addNodeToShard).Methods("PUT")
	r.HandleFunc("/key-value-store-shard/reshard", reshard).Methods("PUT")

	//helper functions for communication between shards...
	r.HandleFunc("/key-value-store-shard/get-info", getShardInfo).Methods("GET")
	r.HandleFunc("/key-value-store-shard/add-member-replicate/", addForward).Methods("PUT")
	r.HandleFunc("/forward/numKeys/{ID}", forwardKeyCount).Methods("GET")

	// Gossip Handler / Endpoint
	// Instantly responds "Alive" if replica is running
	r.HandleFunc("/gossip", gsp.HandleGossip).Methods("GET")

	////////////////////////////////////////
	// This is not for the assignment, but returns all entries so that they are viewable
	// via the /key-value-store endpoint (useful for testing)
	// kvs.InsertExampleData(node.db)
	r.HandleFunc("/key-value-store/", GetAllEntries).Methods("GET")
	///////////////////////////////////////

	r.HandleFunc("/key-value-store-fetch/", fetchEntries).Methods("PUT")

	// Begin gossiping with other replicas
	go gsp.Gossip(node.V)

	// Broadcast to subnet to add new node to views
	go announce()

	// Check for our goof somewhere
	//if view.ContainsDuplicate(node.V.View, node.V.Owner) {
	//	// Delete the second occurence of duplicate
	//	node.V.View = node.V.View[:len(node.V.View)-1]
	//}

	log.Println("REST: Exposing port 8080 --> 808X")
	log.Fatal(http.ListenAndServe(":8080", r)) // Blocks until terminated, so Gossip before
}

//======================================================================================================================
//======================================================================================================================
//======================================================================================================================
