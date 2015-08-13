FROM mesosphere/mesos-modules-dev-phusion
MAINTAINER Spike Curtis <spike@projectcalico.org>

####################
# Mesos-DNS
####################
RUN wget https://dl.dropboxusercontent.com/u/4550074/mesos/mesos-dns -O /usr/bin/mesos-dns && \
    chmod +x /usr/bin/mesos-dns

####################
# Isolator
####################

ADD ./isolator /isolator/isolator/
ADD ./m4 /isolator/m4/
ADD ./bootstrap /isolator/
ADD ./calico_isolator.py /isolator/
ADD ./configure.ac /isolator/
ADD ./Makefile.am /isolator/
ADD ./requirements.txt /isolator/

WORKDIR /isolator

# Build the isolator.
# We need libmesos which is located in /usr/local/lib.
RUN ./bootstrap && \
    rm -rf build && \
    mkdir build && \
    cd build && \
    export LD_LIBRARY_PATH=LD_LIBRARY_PATH:/usr/local/lib && \
    ../configure --with-mesos=/usr/local --with-protobuf=/usr && \
    make all

# Add python module requirements
# https://transfer.sh/m8zh5/pycalico-0.4.8-cp27-none-linux-x86-64.whl

RUN apt-get install -y python-pip libffi-dev
RUN pip install -r requirements.txt
RUN pip install flask

###################
# Docker
###################
RUN apt-get update -qq && apt-get install -qqy \
    apt-transport-https \
    ca-certificates \
    curl \
    lxc \
    iptables

# Install Docker from Docker Inc. repositories.
RUN curl -sSL https://get.docker.com/ubuntu/ | sh

# Define additional metadata for our image.
VOLUME /var/lib/docker

###################
# Calico
###################
RUN wget https://github.com/Metaswitch/calico-docker/releases/download/v0.5.1/calicoctl && \
    chmod +x calicoctl && \
    mv calicoctl /usr/local/bin/

#######################
# Star (test workload)
#######################
WORKDIR /star

ADD http://downloads.mesosphere.io/demo/star/v0.5.0/star-collect-v0.5.0-linux-x86_64 /star/
RUN chmod +x star-collect-v0.5.0-linux-x86_64
ADD http://downloads.mesosphere.io/demo/star/v0.5.0/star-probe-v0.5.0-linux-x86_64 /star/
RUN chmod +x star-probe-v0.5.0-linux-x86_64

COPY ./demo/marathon/star-resources.json /star/star-resources.json
COPY ./demo/marathon/star-iso-resources.json /star/star-iso-resources.json

##################
# Sample Flask App
#################
COPY ./test/sampleflaskapp.tgz /mesos/sampleflaskapp.tgz

#################
# Init scripts
#################
ADD ./init_scripts/etc/service/mesos_slave/run /etc/service/mesos_slave/run
ADD ./init_scripts/etc/service/docker/run /etc/service/docker/run
ADD ./init_scripts/etc/service/calico/run /etc/service/calico/run
ADD ./init_scripts/etc/service/mesos-dns/run /etc/service/mesos-dns/run
ADD ./init_scripts/etc/config/mesos-dns.json /etc/config/mesos-dns.json
