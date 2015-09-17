#!/bin/sh
docker-compose up -d
sleep 15
MASTER_IP=`docker inspect --format '{{ .NetworkSettings.IPAddress }}' netmodules_mesosmaster_1`
docker exec netmodules_mesosmaster_1 python /framework/test_framework.py $MASTER_IP:5050
