// Initializes a forwarding instance of the REST API
// that forwards requests sent to its IP/Port to the main instance.
// Requests are handled by the main instance, and its response sent back
// through the forward and to the client.

package forwarding

import (
	"encoding/json"
	"io/ioutil"

	"github.com/gorilla/mux"
	"github.com/mrhea/CMPS128_Assignment4/structs"

	"log"
	"net/http"
)

// Addr holds the url that will be passed to init
type Addr struct {
	url string
}

// InitForward starts up forwarding instance using a proxy server
func InitForward(fwdAddr string) {
	log.Println("FORWARD: Initializing a new forwarding router")
	r := mux.NewRouter()

	// Set url
	route := replaceString(fwdAddr)
	req := &Addr{url: route}

	// Set forward handler
	r.HandleFunc("/key-value-store/{key}", req.forward).Methods("GET", "PUT", "DELETE")

	log.Println("FORWARD: Exposing port 8080 --> 8083")
	log.Fatal(http.ListenAndServe(":8080", r))
}

// proxy. Sends Get request from forward to Main.
func (addr *Addr) forward(w http.ResponseWriter, r *http.Request) {
	log.Println("FORWARD: Handling request to Main instance")
	//w.Header().Set("Content-Type", "application/json")

	// Incoming data to store
	data := mux.Vars(r)

	// Init client & send request
	client := &http.Client{}
	req, err := http.NewRequest(r.Method, addr.url+data["key"], r.Body)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		failed := structs.MainDownError{Message: "Error in " + r.Method, Error: "Main instance is down"}
		w.WriteHeader(503)
		json.NewEncoder(w).Encode(failed)
		return
	}
	b, _ := ioutil.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	w.Write(b)
	log.Printf("Response from Main: %v", b)

}

// Configures correct url path
func replaceString(addr string) string {
	return "http://" + addr + "/key-value-store/"
}
