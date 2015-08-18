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
ADD ./calico_isolator /isolator/
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
RUN wget https://github.com/Metaswitch/calico-docker/releases/download/v0.5.5/calicoctl && \
    chmod +x calicoctl && \
    mv calicoctl /usr/local/bin/

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
