// Package structs contains structures for HTTP request responses
package structs

// Put response format
type Put struct {
	Message  string `json:"message"`
	Replaced bool   `json:"replaced"`
	Version  int    `json:"version"`
	Meta     []int  `json:"causal-metadata"`
}

// Replica stores the address of a replica
type Replica struct {
	Address string `json:"socket-address"` // The address of a replica
}

// PutError response in case of PUT request error
type PutError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Get response format
type Get struct {
	Message string `json:"message"`
	Version int    `json:"version"`
	Value   string `json:"value"`
	Meta    int    `json:"causal-metadata"`
}

// GetError response in case of GET request error
type GetError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Delete response format
type Delete struct {
	DoesExist bool   `json:"doesExist"`
	Message   string `json:"message"`
	Version   int    `json:"version"`
	Meta      []int  `json:"causal-metadata"`
}

// DeleteError response in case of DELETE request error
type DeleteError struct {
	DoesExist bool   `json:"doesExist"`
	Error     string `json:"error"`
	Message   string `json:"message"`
}

type Stall struct {
	Error   string `json:"error"`
	Message string `json:"error"`
}

// MainDownError response in case of main instance down
type MainDownError struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

//ReplicaResponseFailure
type ReplicaResponseFailure struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

//ReplicaResponse
type ReplicaResponse struct {
	Message string `json:"message"`
	Version int    `json:"version"`
}

//ReplicaDownError response in case a replica does not exist in view
type ReplicaDownError struct {
	Message string `json:"message"`
	Error   string `json:"message"`
}

// ViewGet response in case of replica receiving GET view operation
type ViewGet struct {
	Message string `json:"message"`
	View    string `json:"view"`
}

// ViewPut response
// Use PutError struct in case of ViewPut error, same format
type ViewPut struct {
	Message string `json:"message"`
}

type ViewReplica struct {
	Message string `json:"message"`
}

// ViewDelete response
type ViewDelete struct {
	Message string `json:"message"`
}

// ViewDeleteError response
type ViewDeleteError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type NodeShard struct {
	Message string `json:"message"`
	ID      string `json:"shard-id"`
}