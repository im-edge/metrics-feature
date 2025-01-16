%define revision 1
%define git_version %( git describe --tags | cut -c2- | tr -s '-' '+')
%define git_hash %( git rev-parse --short HEAD )
%define basedir         %{_datadir}/imedge-features/metrics
%define bindir          %{_bindir}
%undefine __brp_mangle_shebangs

Name:           imedge-feature-metrics
Version:        %{git_version}
Release:        %{revision}%{?dist}
Summary:        IMEdge Metrics Feature
Group:          Applications/System
License:        MIT
URL:            https://github.com/im-edge/metrics-feature
Source0:        https://github.com/im-edge/metrics-feature/archive/%{git_hash}.tar.gz
BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-%{git_version}-%{release}
Packager:       Thomas Gelf <thomas@gelf.net>
Requires:       imedge-node

%description
This IMEdge feature wants to offer an unexcited pleasantly relaxed performance
graphing experience. Implemented as a thin and modern abstraction layer based
on matured technology it puts its ma  in focus on robustness and ease of use.

Performance Graphing should "just work" out of the box. We do not assume that
our users carry a data science degree. Based on our field experience with Open
Source monitoring solutions, we make strong assumptions on what your preferences
might be. While it of course allows for customization, it ships with opinionated,
preconfigured data retention rules. You CAN care, but you do not have to.

%prep

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
mkdir -p %{buildroot}%{basedir}
cd - # ???
cp -pr bin lua src vendor feature.php %{buildroot}%{basedir}/

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%{basedir}

%changelog
* Mon Jan 13 2025 Thomas Gelf <thomas@gelf.net> 0.0.0
- Initial packaging
