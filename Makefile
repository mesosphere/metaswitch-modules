CALICO_NODE_VERSION=v0.5.4
DOCKER_COMPOSE_URL=https://github.com/docker/compose/releases/download/1.4.0/docker-compose-`uname -s`-`uname -m`

default: images

docker-compose:
	  curl -L ${DOCKER_COMPOSE_URL} > docker-compose
	  chmod +x ./docker-compose

images: calico-node docker-compose
	  ./docker-compose build;

calico-node: calico-node-$(CALICO_NODE_VERSION).tar

calico-node-$(CALICO_NODE_VERSION).tar:
	docker pull calico/node:v0.5.4
	docker save -o calico-node-v0.5.4.tar calico/node:v0.5.4

st: images
	for container in netmodules_marathon_1 netmodules_mesosmaster_1 netmodules_zookeeper_1 netmodules_etcd_1 netmodules_slave_1 netmodules_slave_2 ; do \
		docker rm -f $$container; true; \
	done
	test/run_compose_st.sh
