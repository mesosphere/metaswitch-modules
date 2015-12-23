Name:          net-modules
Version:       0.26
Release:       1.custom
Summary:       Network isolation modules for Apache Mesos
License:       ASL 2.0
URL:           http://mesos.apache.org/

ExclusiveArch: x86_64

Source0:       netmodules.tar.gz
Source1:       isolation
Source2:       hooks

BuildRequires: libtool
BuildRequires: python-devel
BuildRequires: gcc-c++
BuildRequires: glog-devel
BuildRequires: gflags-devel
BuildRequires: boost-devel
BuildRequires: protobuf-devel
BuildRequires: curl-devel
BuildRequires: subversion-devel


%description
The first implementation in this repository showcases Apache Mesos using Project Calico as the networking solution.

%prep
%setup -q -n isolator


%build
./bootstrap

%configure --with-mesos=/usr/include/mesos/ --with-protobuf=/usr
make

%install
ls -R %{buildroot}
echo %{buildroot} 
mkdir -p %{buildroot}/opt/net-modules
cp -v .libs/* %{buildroot}/opt/net-modules/

mkdir -p %{buildroot}%{_sysconfdir}/mesos-slave
install %{SOURCE1} %{buildroot}%{_sysconfdir}/mesos-slave/
install %{SOURCE2} %{buildroot}%{_sysconfdir}/mesos-slave/

############################################
%files
/opt/net-modules/*
/opt/net-modules/libmesos_network_isolator.so
%{_sysconfdir}/mesos-slave/isolation
%{_sysconfdir}/mesos-slave/hooks

%changelog
* Tue Dec 22 2015 Dan Osborne <daniel.osborne@metaswitch.com> - 0.26-1.custom
- Build mesos 0.26.0

* Wed Oct 21 2015 Thibault Cohen <thibault.cohen@nuance.com> - 0.25.0-1.custom
- Build mesos 0.25.0
