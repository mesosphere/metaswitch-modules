#!/bin/bash -e

set -o errexit -o nounset -o pipefail

SLAVEIP=`docker inspect --format '{{ .NetworkSettings.IPAddress }}' netmodules_slave_1`
sudo ip route replace 192.168.0.0/16 via $SLAVEIP

