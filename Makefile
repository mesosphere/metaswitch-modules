.PHONY: framework
CALICO_NODE_VERSION=v0.8.0
DOCKER_COMPOSE_URL=https://github.com/docker/compose/releases/download/1.4.0/docker-compose-`uname -s`-`uname -m`

default: images

docker-compose:
	  curl -L ${DOCKER_COMPOSE_URL} > docker-compose
	  chmod +x ./docker-compose

images: docker-compose
	  ./docker-compose pull
	  ./docker-compose build

clean:
	./docker-compose kill
	./docker-compose rm --force
	docker rm -f calico-node

cluster: images
	./docker-compose up -d
	ETCD_AUTHORITY=localhost:4001 calicoctl node

framework: cluster
	sleep 20
	docker run calico/calico-mesos-framework `docker inspect --format '{{ .NetworkSettings.IPAddress }}' netmodules_master_1`:5050

rpm: dist/mesos.rpm
dist/mesos.rpm: $(wildcard packages/*)
	docker build -t mesos-builder ./packages
	mkdir -p build
	docker run \
	-v `pwd`/build:/root/rpmbuild/RPMS/x86_64/ \
	-v `pwd`/isolator:/tmp/isolator:ro mesos-builder
