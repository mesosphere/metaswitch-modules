# Network isolation modules for [Apache Mesos](http://mesos.apache.org)

The first implementation in this repository showcases Apache Mesos using Project Calico as the networking solution.

To get started you will need either:
  - a Linux computer running a modern distribution
  - a Windows or Mac computer where you will run the demo in a Linux VM.

## Vagrant setup

Download and install VirtualBox and Vagrant.

Clone this repository.

    git clone https://github.com/mesosphere/net-modules.git

Start the Vagrant VM.  This will automatically provision the VM.  Sit back and relax, it takes a few minutes to pre-load the Docker images used for the demo.

    cd net-modules
    vagrant up

_Note: the shell provision step contains steps that must be performed each time the VM is booted.  Append the `--provision-with shell` flag when running `vagrant up` or `vagrant reload` when booting the VM subsequent times._

The commands below should all be run from inside the virtual machine.  Logging in is easy:

    vagrant ssh

## Linux setup

Install Docker: https://docs.docker.com/installation/

Install Docker-compose:  https://docs.docker.com/compose/install/

Load Kernel modules used by Project Calico:

    sudo modprobe ip6_tables
    sudo modprobe xt_set

Clone this repository.

    git clone https://github.com/mesosphere/net-modules.git

## Build the demo (Vagrant and Linux)

From the `net-modules` directory

    make images

## Run the "before" demo

This first demo shows what life is like with "vanilla" Mesos: port conflicts and no network isolation.

    ./demo/launch-cluster.sh

Wait until the cluster is up, then visit http://localhost:5050/

You should see a working Mesos status page with no tasks and two slaves.

Then

    ./demo/launch-probes-before.sh

Show the Mesos status page, and watch for the "collect" task to start.  Then visit http://localhost:9003/ to see the visualization.  You should see only two probes are running.

Tear down the cluster for your next demo.

    ./demo/stop-probes-before.sh

## Run the Calico w/o isolation demo

This demo shows Calico without network isolation.  All probes can reach one another.

    ./demo/launch-probes.sh

This brings up the test probes and targets with no isolation---everything can talk to everything else.  Verify by visiting the visualization page.

  - Linux: http://192.168.255.253:9001/
  - Vagrant (from the host OS): http://localhost:9001/

Bring up the test probes and targets with isolation.

    ./demo/launch-probes-isolated.sh

Verify by visiting the visualization page.

  - Linux: http://192.168.255.252:9002
  - Vagrant (from the host OS): http://localhost:9002

Tear down the test workloads.

    ./demo/stop-probes.sh
    ./demo/stop-probes-isolated.sh

Or, alternatively simply tear down the cluster.

    ./demo/stop-cluster.sh

## Build RPMs

1. To build RPMs just type:

        make builder-rpm

2. To clean build stuff:

        make builder-clean
