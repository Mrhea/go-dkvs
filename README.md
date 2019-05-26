# CMPS128_Assignment2B

Assignment 3 for CMPS 128: Fault tolerant key-value store with replication that provides causal consistency

## :rocket: Quickstart

```bash
# Create subnet
docker network create --subnet=10.10.0.0/16 mynet

# Build docker image
docker build -t assignment3-img .

# Run replicas docker containers
docker run -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="replica1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-img

docker run -p 8083:8080 --net=mynet --ip=10.10.0.3 --name="replica2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-img

docker run -p 8084:8080 --net=mynet --ip=10.10.0.4 --name="replica3" -e SOCKET_ADDRESS="10.10.0.4:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-img

# Test Key-Value Store endpoint with curl commands
curl --request PUT --header "Content-Type: application/json" --write-out "%{http_code}\n" --data '{"value": "Distributed Systems"}' http://127.0.0.1:8083/key-value-store/course1

# Test View endpoint with curl commands 
curl --request GET --header "Content-Type: application/json" --write-out "%{http_code}\n" http://`<replica-socket-address>`/key-value-store-view

# Alternatively, run script
sudo python3 test_assignment3_v1.py
```


