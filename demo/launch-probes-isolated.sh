#!/bin/bash -e

set -o errexit -o nounset -o pipefail

DEMO_DIR=`dirname $0`
MARATHON_DIR=$DEMO_DIR/marathon

echo "Launching group 'stars-isolated'"

curl -X PUT -H "Content-Type: application/json" http://localhost:8080/v2/groups/star-iso -d @$MARATHON_DIR/stars-isolated.json

echo ""
echo "Done."
