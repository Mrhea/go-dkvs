package gossip

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/mrhea/CMPS128_Assignment4/structs"
	"github.com/mrhea/CMPS128_Assignment4/view"
)

// Query health check message to nodes.
type gossipQuery struct {
	Query string `json:"query"`
}

// Response to a query.
type gossipResp struct {
	Response string `json:"response"`
}

type addr struct {
	url string
}

// Gossip is used by a second thread running in replicas.
// Initializes a Client that times out if no response is received
// within 5 seconds.
// If no response, delete replica from owner's view and broadcast
// this deletion to the rest of the replica in the subnet.
func Gossip(V *view.View) {
	log.Printf("GOSSIP: Node %s starts to gossip", V.Owner)

	// Init a client to perform gossip requests
	client := &http.Client{Timeout: 5 * time.Second}

	// Slice to store gossip response.
	// Had to do something mundane with the response
	var gspSlice []string

	// Sleep before gossiping. Avoids errors when starting up
	// replicas linearly.
	time.Sleep(130 * time.Second) // Lower setup sleep time for actual test script?

	// Gossip forever
	for {
		time.Sleep(1 * time.Second)

		// log.Println("GOSSIP: Gossiping with a gossipee")
		// Choose a node to gossip with
		gossipNode, err := view.GetRandomNode(V)
		if err != nil {
			panic(err)
		}
		// log.Printf("GOSSIP: Gossipee is: %s", gossipNode)

		route := formatRouteToReplica(gossipNode)
		// req := &addr{url: route}
		req, err := http.NewRequest("GET", route, nil)
		if err != nil {
			panic(err)
		}

		// Oh my god there are too many panics
		resp, err := client.Do(req)
		if err != nil {
			log.Println("GOSSIP: Gossip error. Deleting a node")

			nodeToDelete := structs.Replica{Address: gossipNode}
			nodeData, _ := json.Marshal(nodeToDelete)
			deleteViewRoute := "http://" + V.Owner + "/key-value-store-view"

			log.Print("Node being deleted: ")
			log.Println(nodeToDelete.Address)

			req, err := http.NewRequest("DELETE", deleteViewRoute, bytes.NewBuffer(nodeData))
			if err != nil {
				panic(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			gspSlice = append(gspSlice, resp.Status)
			continue
		}
		a := resp.Status
		gspSlice = append(gspSlice, a)
	}
}

func formatRouteToReplica(addr string) string {
	return "http://" + addr + "/gossip"
}

// HandleGossip responses to a gossip request between replicas
func HandleGossip(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	//log.Println("GOSSIP: Responding to gossiper")

	success := gossipResp{Response: "Alive"}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(success)
}
