CALICO_NODE_VERSION=v0.6.0
DOCKER_COMPOSE_URL=https://github.com/docker/compose/releases/download/1.4.0/docker-compose-`uname -s`-`uname -m`

default: images

docker-compose:
	  curl -L ${DOCKER_COMPOSE_URL} > docker-compose
	  chmod +x ./docker-compose

images: calico-node docker-compose
	  ./docker-compose build;

calico-node: calico/calico-node-$(CALICO_NODE_VERSION).tar

calico/calico-node-$(CALICO_NODE_VERSION).tar:
	docker pull calico/node:$(CALICO_NODE_VERSION)
	docker save -o calico/calico-node-$(CALICO_NODE_VERSION).tar calico/node:$(CALICO_NODE_VERSION)

clean:
	docker-compose kill
	docker-compose rm --force
	docker-compose pull


st: clean images
	test/run_compose_st.sh

framework: clean images
	framework/run_framework_st.sh
	
