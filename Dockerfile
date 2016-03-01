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
    ../configure --with-mesos=/usr/local --with-protobuf=/usr && \
    make all && make install
