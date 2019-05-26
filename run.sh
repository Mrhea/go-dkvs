#!/bin/sh

docker run -p 8082:8080 --net=mynet --ip=10.10.0.2 --name="replica1" -e SOCKET_ADDRESS="10.10.0.2:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-image &
docker run -p 8083:8080 --net=mynet --ip=10.10.0.3 --name="replica2" -e SOCKET_ADDRESS="10.10.0.3:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-image &
docker run -p 8084:8080 --net=mynet --ip=10.10.0.4 --name="replica3" -e SOCKET_ADDRESS="10.10.0.4:8080" -e VIEW="10.10.0.2:8080,10.10.0.3:8080,10.10.0.4:8080" assignment3-image &