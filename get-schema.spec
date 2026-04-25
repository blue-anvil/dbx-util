%define _topdir    %(echo $HOME)/rpmbuild
%define _tmppath   %{_topdir}/tmp
%define _buildpath %(pwd)
%define _label     cfpb

Name:           get-schema
Version:        1.1
Release:        1.%{_label}
Summary:        Infer the schema of a local data file in Spark StructType format
License:        MIT
BuildArch:      noarch

%description
Infers the schema of a local JSON, CSV, or Parquet file and outputs it
in Spark's StructType.json() format.

%install
install -D -m 755 %{_buildpath}/get-schema.py %{buildroot}%{_bindir}/get-schema
install -D -m 644 %{_buildpath}/doc/install.txt %{buildroot}%{_docdir}/%{name}/INSTALL

%files
%{_bindir}/get-schema
%doc %{_docdir}/%{name}/INSTALL

%changelog
