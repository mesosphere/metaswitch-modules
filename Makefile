CALICO_NODE_VERSION=v0.5.4


images: calico-node
	docker-compose build

calico-node: calico-node-$(CALICO_NODE_VERSION).tar

calico-node-$(CALICO_NODE_VERSION).tar:
	docker pull calico/node:v0.5.4
	docker save -o calico-node-v0.5.4.tar calico/node:v0.5.4

st: images
	for container in netmodules_marathon_1 netmodules_mesosmaster_1 netmodules_zookeeper_1 netmodules_etcd_1 netmodules_slave1_1 netmodules_slave2_1 ; do \
		docker rm -f $$container; true; \
	done
	test/run_compose_st.sh
