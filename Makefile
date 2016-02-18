.PHONY: framework
CALICO_NODE_VERSION=v0.16.1
DOCKER_COMPOSE_URL=https://github.com/docker/compose/releases/download/1.4.0/docker-compose-`uname -s`-`uname -m`

default: images

docker-compose:
	  curl -L ${DOCKER_COMPOSE_URL} > docker-compose
	  chmod +x ./docker-compose

calico-node: calico/calico-node-$(CALICO_NODE_VERSION).tar

calico/calico-node-$(CALICO_NODE_VERSION).tar:
	docker pull calico/node:$(CALICO_NODE_VERSION)
	docker save -o calico/calico-node-$(CALICO_NODE_VERSION).tar calico/node:$(CALICO_NODE_VERSION)

images: calico-node docker-compose
	  ./docker-compose pull
	  ./docker-compose build

clean:
	./docker-compose kill
	./docker-compose rm --force

st: clean images
	test/run_compose_st.sh

cluster: images
	./docker-compose up -d
	./docker-compose scale slave=3

framework: cluster
	sleep 20
	docker exec netmodules_mesosmaster_1 python /framework/calico_framework.py

rpm: dist/mesos.rpm
dist/mesos.rpm: $(wildcard packages/*)
	docker build -t mesos-builder ./packages
	mkdir -p build
	docker run \
	-v `pwd`/build:/root/rpmbuild/RPMS/x86_64/ \
	-v `pwd`/isolator:/tmp/isolator:ro mesos-builder
