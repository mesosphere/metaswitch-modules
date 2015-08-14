.PHONEY: images

images:
	docker pull calico/node:v0.5.1
	docker save -o calico-node.tar calico/node:v0.5.1
	docker build --no-cache -t mesosphere/slave .
