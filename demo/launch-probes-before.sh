#!/bin/bash -e

set -o errexit -o nounset -o pipefail

DEMO_DIR=`dirname $0`
MARATHON_DIR=$DEMO_DIR/marathon

echo "Launching group 'stars'"

curl -X PUT -H "Content-Type: application/json" http://localhost:8080/v2/groups/star-before -d @$MARATHON_DIR/stars-before.json

echo ""
echo "Done."
