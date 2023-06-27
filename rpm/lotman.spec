#############################################
# Global macros that can be used throughout #
#############################################
%global srcname lotman-cpp

Name: %srcname
Version: 0.0.1
Release: 1%{?dist}
Summary: C++ Implementation of the LotMan Library
License: Apache-2.0
URL: https://github.com/PelicanPlatform/lotman

Source0: https://github.com/PelicanPlatform/lotman/releases/download/v%{version}/%{name}-%{version}.tar.gz

#############################################
# Build dependencies                        #
#############################################
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: cmake3 >= 3.18.4
BuildRequires: sqlite-devel
BuildRequires: libuuid-devel
BuildRequires: nlohmann-json-devel
BuildRequires: json-schema-validator-devel

%description
Public headers for the LotMan library, which tracks data usage in dHTC environments over the "lot" object.

#############################################
# To suppress some of the debug outputs     #
#############################################
%global debug_package %{nil}

#############################################
# RHEL 9 will try to build out of source,   #
# so that needs to be overridden.           #
#############################################
%if 0%{?rhel} > 8
%global __cmake_in_source_build 1
%endif

#############################################
# Beginning of the build + make workflow    #
#############################################
%prep
%autosetup -n %{srcname}-%{version}

%build
mkdir -p build
cd build
%cmake ..
%make_build

%install
cd build
%make_install

%files
%license LICENSE
%doc README.md
%{_libdir}/libLotMan.so.0*
%{_libdir}/libLotMan.so
%{_bindir}/lotman-*
%{_includedir}/lotman/lotman.h
%dir %{_includedir}/lotman

%changelog
* Tue June 27 2023 Justin Hiemstra <jhiemstra@morgridge.org> - 0.0.1-1
- Initial release of the LotMan C++ RPM.
