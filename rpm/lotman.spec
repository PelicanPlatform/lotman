Name: lotman-cpp
Version: 0.0.1
Release: 1%{?dist}
Summary: C++ Implementation of the LotMan Library
License: Apache-2.0
URL: https://github.com/PelicanPlatform/lotman

# Directions to generate a proper release:
# VER=0.3.3 # for example
# git archive --prefix "scitokens-cpp-$VER/" -o "scitokens-cpp-$VER.tar" v$VER
# git submodule update --init
# git submodule foreach --recursive "git archive --prefix=scitokens-cpp-$VER/\$path/ --output=\$sha1.tar HEAD && tar --concatenate --file=$(pwd)/scitokens-cpp-$VER.tar \$sha1.tar && rm \$sha1.tar"
# gzip "scitokens-cpp-$VER.tar"


Source0: https://github.com/PelicanPlatform/lotman/releases/download/v%{version}/%{name}-%{version}.tar.gz
Source1: json.hpp

BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: cmake3 >= 3.18.4
BuildRequires: sqlite-devel
BuildRequires: libuuid-devel
%if 0%{?el7}
# needed for ldconfig_scriptlets
BuildRequires: epel-rpm-macros
%endif

%description
%{summary}

%package devel
Summary: Header files for the lotman public interfaces

Requires: %{name}%{?_isa} = %{version}

%description devel
%{summary}

%prep
%setup -q

cp %{SOURCE1} .

%build
%cmake3
%cmake3_build

%install
%cmake3_install

# Run the ldconfig
%ldconfig_scriptlets

%files
%{_libdir}/libLotMan.so.0*
%{_bindir}/lotman-*
%license LICENSE
%doc README.md

%files devel
%{_libdir}/libLotMan.so
%{_includedir}/lotman/lotman.h
%dir %{_includedir}/lotman

%changelog
* Fri June 16 2023 Justin Hiemstra <jhiemstra@wisc.edu> - 0.0.1-1
- Initial release of the LotMan C++ RPM.
