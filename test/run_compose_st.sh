#!/bin/bash
docker-compose -p netmodules up -d
docker-compose scale slave=2
cd `dirname $0`

# Wait for the marathon container to come up
sleep 60
curl -X POST http://localhost:8080/v2/apps -d @sample-flask-app.json -H "Content-type: application/json"
curl -X POST http://localhost:8080/v2/apps -d @sample-flask-app-2.json -H "Content-type: application/json"

# Wait for calico-node image to download
sleep 90

curl -o state.json localhost:5050/master/state.json

docker exec netmodules_slave_1 ping -c 4 192.168.0.0
docker exec netmodules_slave_1 ping -c 4 192.168.1.0

docker exec netmodules_slave_2 ping -c 4 192.168.0.0
docker exec netmodules_slave_2 ping -c 4 192.168.1.0
