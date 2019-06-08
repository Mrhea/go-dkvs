#!/bin/sh

# Stop and remove containers
sudo docker container stop node1
sudo docker container stop node2
sudo docker container stop node3
sudo docker container stop node4
sudo docker container stop node5
sudo docker container stop node6
sudo docker container stop node7

sudo docker container rm node1
sudo docker container rm node2
sudo docker container rm node3
sudo docker container rm node4
sudo docker container rm node5
sudo docker container rm node6
sudo docker container rm node7


# Remove image
sudo docker image rm assignment4-image

# Remove network
sudo docker network rm mynet

# Re-Create network
sudo docker network create --subnet=10.10.0.0/16 mynet

# Re-Build Docker image
sudo docker build -t assignment4-image .

# Run Docker containers
sudo docker run -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="node1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image & sudo docker run -p 8083:8080 --net=mynet --ip=10.10.0.3 --name="node2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image & sudo docker run -p 8084:8080 --net=mynet --ip=10.10.0.4 --name="node3" -e SOCKET_ADDRESS="10.10.0.4:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image & sudo docker run -p 8085:8080 --net=mynet --ip=10.10.0.5 --name="node4" -e SOCKET_ADDRESS="10.10.0.5:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image & sudo docker run -p 8086:8080 --net=mynet --ip=10.10.0.6 --name="node5" -e SOCKET_ADDRESS="10.10.0.6:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image & sudo docker run -p 8087:8080 --net=mynet --ip=10.10.0.7 --name="node6" -e SOCKET_ADDRESS="10.10.0.7:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080,10.10.0.5:8080,10.10.0.6:8080,10.10.0.7:8080" -e SHARD_COUNT="2" assignment4-image


