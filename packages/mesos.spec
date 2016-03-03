Name:          mesos
Version:       0.27.0
Release:       1.custom
Summary:       Cluster manager for sharing distributed application frameworks
License:       ASL 2.0
URL:           http://mesos.apache.org/

ExclusiveArch: x86_64

%define mesos_github_tag 0.27.0

Source0:       %{mesos_github_tag}.tar.gz
Source1:       %{name}
Source2:       %{name}-master
Source3:       %{name}-slave
Source4:       %{name}-master.service
Source5:       %{name}-slave.service
Source6:       quorum
Source7:       work_dir
Source8:       mesos-init-wrapper
Source9:       zk

BuildRequires: libtool
BuildRequires: automake
BuildRequires: autoconf
BuildRequires: java-devel
BuildRequires: zlib-devel
BuildRequires: libcurl-devel
BuildRequires: python-setuptools
BuildRequires: python2-devel
BuildRequires: openssl-devel
BuildRequires: cyrus-sasl-devel
BuildRequires: cyrus-sasl-md5
BuildRequires: systemd

BuildRequires: boost-devel
BuildRequires: glog-devel
BuildRequires: gmock-devel
BuildRequires: gflags-devel
BuildRequires: gtest-devel
BuildRequires: gperftools-devel
BuildRequires: libev-source
BuildRequires: leveldb-devel
BuildRequires: protobuf-python
BuildRequires: protobuf-java
#BuildRequires: zookeeper-devel
BuildRequires: protobuf-devel
#BuildRequires: picojson-devel
BuildRequires: python-pip
BuildRequires: python-wheel

Requires: protobuf-python
Requires: python-boto
Requires: python-pip
Requires: python-wheel

BuildRequires: apr-devel
BuildRequires: subversion-devel
BuildRequires: http-parser-devel

Requires: python-boto
Requires: cyrus-sasl-md5

# The slaves will indirectly require time syncing with the master
# nodes so just call out the dependency.
Requires: ntpdate

%description
Apache Mesos is a cluster manager that provides efficient resource
isolation and sharing across distributed applications, or frameworks.
It can run Hadoop, MPI, Hypertable, Spark, and other applications on
a dynamically shared pool of nodes.

##############################################
%package devel
Summary:        Header files for Mesos development
Group:          Development/Libraries
Requires:       %{name}%{?_isa} = %{version}-%{release}

%description devel
Provides header and development files for %{name}.

##############################################
%package java
Summary:        Java interface for %{name}
Group:          Development/Libraries
Requires:       %{name}%{?_isa} = %{version}-%{release}

%description java
The %{name}-java package contains Java bindings for %{name}.

##############################################
%package -n python-%{name}
Summary:        Python support for %{name}
BuildRequires:  python2-devel
Requires:       %{name}%{?_isa} = %{version}-%{release}
Requires:       python2

%description -n python-%{name}
The python-%{name} package contains Python bindings for %{name}.

##############################################

%prep -n %{mesos_github_tag}
%setup -n %{name}-%{mesos_github_tag}


##%if %unbundled
# remove all bundled elements prior to build
#rm -f `find . | grep [.]tar`

######################################
# We need to rebuild libev and bind statically
# See https://bugzilla.redhat.com/show_bug.cgi?id=1049554 for details
######################################
#cp -r %{_datadir}/libev-source libev-%{libevver}
#cd libev-%{libevver}
#autoreconf -i
#%endif






%build

#./bootstrap
autoreconf -vfi

%ifarch x86_64
export LDFLAGS="$RPM_LD_FLAGS -std=c++11 -L$PWD/libev-%{libevver}/.libs"
%else
export LDFLAGS="$RPM_LD_FLAGS -L$PWD/libev-%{libevver}/.libs"
%endif

%configure --disable-java --disable-optimize --without-included-zookeeper --with-glog=/usr/local --with-protobuf=/usr --with-boost=/usr/local --prefix=/usr
make -j 2


%install
%make_install

######################################
# NOTE: https://issues.apache.org/jira/browse/MESOS-899
export CFLAGS="$RPM_OPT_FLAGS -DEV_CHILD_ENABLE=0 -I$PWD -std=c++11"
export CXXFLAGS="$RPM_OPT_FLAGS -DEV_CHILD_ENABLE=0 -I$PWD -std=c++11"
export LDFLAGS="$RPM_LD_FLAGS -L$PWD/libev-%{libevver}/.libs -std=c++11"


# Python
export PYTHONPATH=%{buildroot}%{python_sitearch}
mkdir -p %{buildroot}%{python_sitearch}
pushd src/python
python setup.py install --root=%{buildroot} --prefix=/usr
popd
#mkdir -p  %{buildroot}%{python_sitelib}
#cp -rf %{buildroot}%{_libexecdir}/%{name}/python/%{name}/* %{buildroot}%{python_sitelib}
#rm -rf %{buildroot}%{_libexecdir}/%{name}/python
pushd src/python/native
python setup.py install --root=%{buildroot} --prefix=/usr --install-lib=%{python_sitearch}
popd
rm -rf %{buildroot}%{python_sitearch}/*.pth
pushd src/python/interface
python setup.py install --root=%{buildroot} --prefix=/usr
popd

# fedora guidelines no .a|.la
rm -f %{buildroot}%{_libdir}/*.la
rm -f %{buildroot}%{_libdir}/libexamplemodule*
rm -f %{buildroot}%{_libdir}/libtest*

# Move the inclusions under mesos folder for developers
mv -f %{buildroot}%{_includedir}/stout %{buildroot}%{_includedir}/%{name}
mv -f %{buildroot}%{_includedir}/process %{buildroot}%{_includedir}/%{name}

# system integration sysconfig setting

mkdir -p %{buildroot}%{_sysconfdir}/default
install -m 0644 %{SOURCE1} %{buildroot}%{_sysconfdir}/default/
install -m 0644 %{SOURCE2} %{buildroot}%{_sysconfdir}/default/
install -m 0644 %{SOURCE3} %{buildroot}%{_sysconfdir}/default/

mkdir -p %{buildroot}%{_unitdir}
install -m 0644 %{SOURCE4} %{SOURCE5} %{buildroot}%{_unitdir}/

mkdir -p %{buildroot}%{_sysconfdir}/%{name}-master
install -m 0644 %{SOURCE6} %{SOURCE7} %{buildroot}%{_sysconfdir}/%{name}-master/
mkdir -p %{buildroot}%{_sysconfdir}/%{name}-slave

install -m 0755 %{SOURCE8} %{buildroot}%{_bindir}/

mkdir -p %{buildroot}%{_sysconfdir}/%{name}
install -m 0644 %{SOURCE9} %{buildroot}%{_sysconfdir}/%{name}/

mkdir -p -m0755 %{buildroot}/%{_var}/log/%{name}
mkdir -p -m0755 %{buildroot}/%{_var}/lib/%{name}

############################################
%files
%doc LICENSE NOTICE
%{_libdir}/libmesos*.so
%{_libdir}/libfixed_resource_estimator-%{version}.so
%{_libdir}/libload_qos_controller-0.27.0.so
%{_libdir}/libload_qos_controller.so
%{_libdir}/liblogrotate_container_logger-0.27.0.so
%{_libdir}/liblogrotate_container_logger.so
%{_bindir}/mesos*
%{_sbindir}/mesos-*
%{_datadir}/%{name}/
%{_libexecdir}/%{name}/
#system integration files
%{python_sitelib}/%{name}/
%attr(0755,mesos,mesos) %{_var}/log/%{name}/
%attr(0755,mesos,mesos) %{_var}/lib/%{name}/
%config(noreplace) %{_sysconfdir}/default/%{name}*
%{_unitdir}/%{name}*.service
%{_sysconfdir}/%{name}-master/*
%{_sysconfdir}/%{name}-slave
%{_sysconfdir}/%{name}/*
%{_includedir}/picojson.h

######################
%files devel
%doc LICENSE NOTICE*
%{_includedir}/mesos/
%{_libdir}/libmesos*.so
%{_libdir}/libfixed_resource_estimator.so
%{_libdir}/pkgconfig/%{name}.pc

######################
%files java
%doc LICENSE NOTICE
#%{_jnidir}/%{name}/%{name}.jar
%if 0%{?fedora} >= 21
#%{_datadir}/maven-metadata/%{name}.xml
#%{_datadir}/maven-poms/%{name}/%{name}.pom
%else
#%{_mavenpomdir}/JPP.%{name}-%{name}.pom
#%{_mavendepmapfragdir}/%{name}.xml
%endif

######################
%files -n python-%{name}
%doc LICENSE NOTICE
%{python_sitelib}/*
%{python_sitearch}/*
############################################

%pre
getent group mesos >/dev/null || groupadd -f -r mesos
if ! getent passwd mesos >/dev/null ; then
      useradd -r -g mesos -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
              -c "%{name} daemon account" mesos
fi
exit 0

%post
%systemd_post %{name}-slave.service %{name}-master.service
/sbin/ldconfig

%preun
%systemd_preun %{name}-slave.service %{name}-master.service

%postun
%systemd_postun_with_restart %{name}-slave.service %{name}-master.service
/sbin/ldconfig


%changelog
* Tue Feb 16 2016 Dan Osborne <daniel.osborne@metaswitch.com> - 0.27.0-1.custom
- Build mesos 0.27.0

* Tue Dec 22 2015 Dan Osborne <daniel.osborne@metaswitch.com> - 0.26.0-1.custom
- Build mesos 0.26.0

* Wed Oct 21 2015 Thibault Cohen <thibault.cohen@nuance.com> - 0.25.0-1.custom
- Build mesos 0.25.0

* Fri Aug 28 2015 Timothy St. Clair <tstclair@redhat.com> - 0.23.0-1.4ce5475.0
- change docker-io to just docker dependency BZ1257832

* Thu Aug 27 2015 Jonathan Wakely <jwakely@redhat.com> - 0.23.0-0.4ce5475.1
- Rebuilt for Boost 1.59

* Wed Aug 5 2015 Timothy St. Clair <tstclair@redhat.com> - 0.23.0-0.4ce5475.0
- Build for latest release

* Wed Jul 29 2015 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 0.22.1-0.d6309f9.3
- Rebuilt for https://fedoraproject.org/wiki/Changes/F23Boost159

* Wed Jul 22 2015 David Tardon <dtardon@redhat.com> - 0.22.1-0.d6309f9.2
- rebuild for Boost 1.58

* Wed Jun 17 2015 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 0.22.1-0.d6309f9.1
- Rebuilt for https://fedoraproject.org/wiki/Fedora_23_Mass_Rebuild

* Wed May 6 2015 Timothy St. Clair <tstclair@redhat.com> - 0.22.1-0.d6309f9
- Build for latest patch release

* Wed Apr 29 2015 Kalev Lember <kalevlember@gmail.com> - 0.22.0-4.e890e24.1
- Rebuilt for protobuf soname bump

* Mon Apr 20 2015 Timothy St. Clair <tstclair@redhat.com> - 0.22.0-3.e890e24
- Fix for .so build version

* Thu Mar 19 2015 Timothy St. Clair <tstclair@redhat.com> - 0.22.0-2.e890e24
- Update to 0.22.0 official release

* Sat Feb 14 2015 Timothy St. Clair <tstclair@redhat.com> - 0.22.0-1.SNAPSHOT.033c062
- Update to track next release

* Mon Jan 26 2015 Petr Machata <pmachata@redhat.com> - 0.22.0-SNAPSHOT.1.c513126.1
- Rebuild for boost 1.57.0

* Tue Dec 9 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-6.ab8fa65
- Fix for python bindings

* Fri Nov 21 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-5.ab8fa65
- Update to latest build

* Thu Oct 23 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-4.SNAPSHOT.e960cdf
- Update to include examples

* Thu Oct 9 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-3.SNAPSHOT.c96ba8f6
- Update and shifting configs to latest.

* Tue Sep 30 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-2.SNAPSHOT.3133734
- Removing scripts and updating systemd settings.

* Tue Sep 23 2014 Timothy St. Clair <tstclair@redhat.com> - 0.21.0-1.SNAPSHOT.3133734
- Initial prototyping

* Wed Aug 27 2014 Timothy St. Clair <tstclair@redhat.com> - 0.20.0-2.f421ffd
- Fixes for system integration

* Wed Aug 20 2014 Timothy St. Clair <tstclair@redhat.com> - 0.20.0-1.f421ffd
- Rebase to new release 0.20

* Sun Aug 17 2014 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 0.18.2-6.453b973
- Rebuilt for https://fedoraproject.org/wiki/Fedora_21_22_Mass_Rebuild

* Sat Jun 07 2014 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 0.18.2-5.453b973
- Rebuilt for https://fedoraproject.org/wiki/Fedora_21_Mass_Rebuild

* Tue May 27 2014 Dennis Gilmore <dennis@ausil.us> - 0.18.2-4.453b973
- add patch to enable building on all primary and secondary arches
- remove ExcludeArch %%{arm}

* Tue May 27 2014 Timothy St. Clair <tstclair@redhat.com> - 0.18.2-3.453b973
- Fixes for systemd

* Fri May 23 2014 Petr Machata <pmachata@redhat.com> - 0.18.2-2.453b973
- Rebuild for boost 1.55.0

* Wed May 14 2014 Timothy St. Clair <tstclair@redhat.com> - 0.18.2-1.453b973
- Rebase to latest 0.18.2-rc1

* Thu Apr 3 2014 Timothy St. Clair <tstclair@redhat.com> - 0.18.0-2.185dba5
- Updated to 0.18.0-rc6
- Fixed MESOS-1126 - dlopen libjvm.so

* Wed Mar 5 2014 Timothy St. Clair <tstclair@redhat.com> - 0.18.0-1.a411a4b
- Updated to 0.18.0-rc3
- Included sub-packaging around language bindings (Java & Python)
- Improved systemd integration
- Itegration to rebuild libev-source w/-DEV_CHILD_ENABLE=0

* Mon Jan 20 2014 Timothy St. Clair <tstclair@redhat.com> - 0.16.0-3.afe9947
- Updated to 0.16.0-rc3

* Mon Jan 13 2014 Timothy St. Clair <tstclair@redhat.com> - 0.16.0-2.d0cb03f
- Updating per review

* Tue Nov 19 2013 Timothy St. Clair <tstclair@redhat.com> - 0.16.0-1.d3557e8
- Update to latest upstream tip.

* Thu Oct 31 2013 Timothy St. Clair <tstclair@redhat.com> - 0.15.0-4.42f8640
- Merge in latest upstream developments

* Fri Oct 18 2013 Timothy St. Clair <tstclair@redhat.com> - 0.15.0-4.464661f
- Package restructuring for subsuming library dependencies dependencies.

* Thu Oct 3 2013 Timothy St. Clair <tstclair@redhat.com> - 0.15.0-3.8037f97
- Cleaning package for review

* Fri Sep 20 2013 Timothy St. Clair <tstclair@redhat.com> - 0.15.0-0.2.01ccdb
- Cleanup for system integration

* Tue Sep 17 2013 Timothy St. Clair <tstclair@redhat.com> - 0.15.0-0.1.1bc2941
- Update to the latest mesos HEAD

* Wed Aug 14 2013 Igor Gnatenko <i.gnatenko.brain@gmail.com> - 0.12.1-0.4.dff92ff
- spec: cleanups and fixes
- spec: fix systemd daemon

* Mon Aug 12 2013 Timothy St. Clair <tstclair@redhat.com> - 0.12.1-0.3.dff92ff
- Update and add install targets.

* Fri Aug  9 2013 Igor Gnatenko <i.gnatenko.brain@gmail.com> - 0.12.1-0.2.cba04c1
- Update to latest
- Add python-boto as BR
- other fixes

* Thu Aug  1 2013 Igor Gnatenko <i.gnatenko.brain@gmail.com> - 0.12.1-0.1.eb17018
- Initial release
