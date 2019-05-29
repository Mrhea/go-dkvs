// Package view handles view operations between existing replicas
// in order to ensure causal consistency in key-value store.
package view

import (
	"log"
	"math/rand"
	"strings"
	"time"
)

// View holds the view available to a replica?
// Needs to store: IPs of other replicas and their ports
type View struct {
	Owner string   // The IP of the server that owns this view
	View  []string // Maps another replica's IP to it's port

}

// Replica holds the address of a replica.
// Used in: rest.go/PutViewv
type Replica struct {
	Address string `json:"socket-address"` // the address of a replica
}

// InitView returns a View struct containing a server's view of the subnet
// IPs and their ports.
func InitView(owner string, viewOfReplicas string) *View {
	var V View
	V.Owner = owner

	// Fill the map with replica's IPs and ports
	log.Println("CURRENT VIEW:")
	replicaIPsAndPorts := strings.Split(viewOfReplicas, ",")
	for _, item := range replicaIPsAndPorts {
		log.Println(item)
		V.View = append(V.View, item)
	}
	return &V
}

// AddReplicaToView adds a replica's IP and port to this server's view of the subnet
// address: the address of the replica to be added
// v: the view of the local server
func AddReplicaToView(address string, v *View) {
	log.Println("Key-Value-Store-View: Inserting Replica Address into view")
	v.View = append(v.View, address)
}

// ContainsDuplicate checks for whether a view of an node contains its own informatino
// more than once
func ContainsDuplicate(s []string, e string) bool {
	occurence := 0
	for _, a := range s {
		if a == e {
			occurence++
		}
	}
	if occurence > 1 { // Duplicated view
		return true
	}
	return false
}

// CheckIfReplicaExists checks if the given address of a replica exists within the
// local server's view
func CheckIfReplicaExists(address string, v *View) bool {
	log.Println("Key-Value-Store-View: Checking if replica exists within the view")
	for _, IP := range v.View {
		if IP == address {
			return true
		}
	}
	return false
}

func DeleteReplica(address string, v *View) {
	log.Println("Key-Value-Store-View: Removing Replica Address from view")
	for i := range v.View {
		if v.View[i] == address {
			v.View = append(v.View[:i], v.View[i+1:]...)
			return
		}
	}
}

func GetRandomNode(v *View) (string, error) {
	for {
		rand.Seed(time.Now().Unix())
		nodeToGossipWith := v.View[rand.Intn(len(v.View))]
		if nodeToGossipWith == v.Owner {
			continue
		}
		return nodeToGossipWith, nil
	}
}
