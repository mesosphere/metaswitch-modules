WD = $(shell pwd)
.PHONY: framework
CALICO_NODE_VERSION=v0.8.0
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
	docker exec netmodules_mesosmaster_1 python /framework/test_calico_mesos.py

builder-rpm:
	cd $(WD)/packages && docker build -t mesos-builder .
	mkdir -p $(WD)/packages/rpms
	cd $(WD)/packages && docker run --name=mesos-builder1 -v $(WD)/packages/rpms:/opt/rpms -t mesos-builder cp -r /root/rpmbuild/RPMS /opt/rpms
	cd $(WD)/packages && docker run --name=mesos-builder2 -v $(WD)/packages/rpms:/opt/rpms -t mesos-builder cp -r /root/rpmbuild/SRPMS /opt/rpms
	docker rm mesos-builder1
	docker rm mesos-builder2

builder-clean:
	cd $(WD)/packages
	docker rmi mesos-builder
	rm -rf $(WD)/packages/rpms
