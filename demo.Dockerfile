FROM djosborne/mesos-dockerized:0.26.0
MAINTAINER Dan Osborne <dan@projectcalico.org>

####################
# Isolator
####################
WORKDIR /isolator
ADD ./isolator/ /isolator/

# Build the isolator.
# We need libmesos which is located in /usr/local/lib.
RUN ./bootstrap && \
    mkdir build && \
    cd build && \
    export LD_LIBRARY_PATH=LD_LIBRARY_PATH:/usr/local/lib && \
    ../configure --with-mesos=/usr/local --with-protobuf=/usr && \
    make all

######################
# Calico
######################
COPY ./calico/ /calico/
RUN wget https://github.com/projectcalico/calico-docker/releases/download/v0.8.0/calicoctl && \
    chmod +x calicoctl && \
    mv calicoctl /usr/local/bin/
RUN wget https://github.com/projectcalico/calico-mesos/releases/download/v0.1.1/calico_mesos && \
    chmod +x calico_mesos && \
    mv calico_mesos /calico/

ENV LD_LIBRARY_PATH /usr/local/lib
