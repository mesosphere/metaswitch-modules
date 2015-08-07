#!/bin/bash
docker-compose up -d
cd `dirname $0`

# Wait for the marathon container to come up
sleep 60
curl -X POST http://localhost:8080/v2/apps -d @sample-flask-app.json -H "Content-type: application/json"
curl -X POST http://localhost:8080/v2/apps -d @sample-flask-app-2.json -H "Content-type: application/json"

# Wait for calico-node image to download
sleep 90

curl -o state.json localhost:5050/master/state.json

# Make sure slave1 can ping the IP of the app running on it
docker exec modules_slave1_1 ping -c 4 `python get_ip_on_app.py slave1`

# Make sure slave2 can ping the IP of the app running on it
docker exec modules_slave2_1 ping -c 4 `python get_ip_on_app.py slave2`

# Slave1 should be able to ping the hostname of the app running on it
docker exec modules_slave1_1 ping -c 4 container.`python get_app_on_slave.py slave1`.marathon.mesos

# Slave2 should be able to ping the hostname of the app running on it
docker exec modules_slave2_1 ping -c 4 container.`python get_app_on_slave.py slave2`.marathon.mesos
