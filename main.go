package main

import (
	"io"
	"log"
	"os"

	"github.com/mrhea/distributed-key-value-store/rest"
)

// MultiLog streams logging to both server.log and stdout
var MultiLog io.Writer

func main() {
	// Setup logging to log file and stdout
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	MultiLog = io.MultiWriter(os.Stdout, logFile)
	log.SetFlags(log.Lshortfile | log.Ltime)
	log.SetOutput(MultiLog)

	log.Println("============================================")

	owner := os.Getenv("SOCKET_ADDRESS")

	viewString := os.Getenv("VIEW")

	shardCount := os.Getenv("SHARD_COUNT")

	log.Printf("Starting replica instance at IP: %s", owner)

	// Initialize endpoints, database, and view
	rest.InitServer(owner, viewString, shardCount)
}
