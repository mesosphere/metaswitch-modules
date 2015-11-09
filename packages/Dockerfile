FROM centos:7
MAINTAINER thibault.cohen@nuance.com

RUN yum install -y tar wget epel-release
RUN wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo

# Install Wandisco (for subversion-devel)
RUN echo '[WANdiscoSVN]' > /etc/yum.repos.d/wandisco-svn.repo 
RUN echo "name=WANdisco SVN Repo 1.9" >> /etc/yum.repos.d/wandisco-svn.repo
RUN echo "enabled=1" >> /etc/yum.repos.d/wandisco-svn.repo
RUN echo "baseurl=http://opensource.wandisco.com/centos/7/svn-1.9/RPMS/$basearch/" >> /etc/yum.repos.d/wandisco-svn.repo
RUN echo "gpgcheck=1" >> /etc/yum.repos.d/wandisco-svn.repo
RUN echo "gpgkey=http://opensource.wandisco.com/RPM-GPG-KEY-WANdisco" >> /etc/yum.repos.d/wandisco-svn.repo

# Mesos Deps
RUN yum groupinstall -y "Development Tools"
RUN yum install -y \
  apache-maven \
  python-devel \
  java-1.7.0-openjdk-devel \
  zlib-devel \
  libcurl-devel \
  openssl-devel \
  cyrus-sasl-devel \
  cyrus-sasl-md5 \
  apr-devel \
  subversion-devel \
  apr-util-devel \
  protobuf-devel \
  protobuf-python \
  boost-devel \
  python-setuptools \
  glog-devel \
  gflags-devel \
  http-parser-devel \
  gmock-devel \
  gtest-devel \
  gperftools-devel \
  libev-source \
  leveldb-devel \
  protobuf-java \
  zookeeper-devel \
  picojson-devel \
  python-pip \
  python-wheel \ 
  automake

# Install the picojson headers
RUN wget https://raw.githubusercontent.com/kazuho/picojson/v1.3.0/picojson.h -O /usr/local/include/picojson.h

# Build & install glog
#WORKDIR  /root
#RUN git clone https://github.com/google/glog.git
#WORKDIR /root/glog
#RUN git checkout v0.3.3
#RUN ./configure --prefix=/usr/local
#RUN make install
WORKDIR  /root

# Prepare to build Mesos
RUN mkdir mesos
RUN mkdir -p /tmp
RUN mkdir -p /usr/share/java/
RUN wget http://search.maven.org/remotecontent?filepath=com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar -O /usr/share/java/protobuf.jar

# MESOS Sources
RUN mkdir -p /root/rpmbuild/SOURCES/
WORKDIR /root/rpmbuild/SOURCES/
RUN wget https://github.com/apache/mesos/archive/0.25.0.tar.gz
ADD ./sources/mesos/mesos /root/rpmbuild/SOURCES/mesos
ADD ./sources/mesos/mesos-master /root/rpmbuild/SOURCES/mesos-master
ADD ./sources/mesos/mesos-slave /root/rpmbuild/SOURCES/mesos-slave
ADD ./sources/mesos/mesos-master.service /root/rpmbuild/SOURCES/mesos-master.service
ADD ./sources/mesos/mesos-slave.service /root/rpmbuild/SOURCES/mesos-slave.service
ADD ./sources/mesos/quorum /root/rpmbuild/SOURCES/quorum
ADD ./sources/mesos/work_dir /root/rpmbuild/SOURCES/work_dir
ADD ./sources/mesos/mesos-init-wrapper /root/rpmbuild/SOURCES/mesos-init-wrapper
ADD ./sources/mesos/zk /root/rpmbuild/SOURCES/zk
ADD ./sources/mesos/firewall-agent.service /root/rpmbuild/SOURCES/firewall-agent.service
ADD ./sources/mesos/firewall-master.service /root/rpmbuild/SOURCES/firewall-master.service

# MESOS
ADD ./mesos.spec /root/mesos.spec

RUN rpmbuild -ba /root/mesos.spec


# NET MODULES
RUN wget https://github.com/mesosphere/net-modules/archive/integration/0.25.zip
RUN yum localinstall -y /root/rpmbuild/RPMS/x86_64/mesos-0*.rpm /root/rpmbuild/RPMS/x86_64/mesos-devel-*.rpm

RUN cp -r /usr/include/mesos/* /usr/include/
ADD ./net-modules.spec /root/net-modules.spec
RUN rpmbuild -ba /root/net-modules.spec

RUN mkdir -p /opt/rpms
