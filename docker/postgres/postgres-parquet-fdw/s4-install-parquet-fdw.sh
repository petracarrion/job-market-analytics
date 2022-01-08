#!/usr/bin/env bash

apt-get update
apt-get -y install \
     build-essential \
     cmake \
     postgresql-server-dev-14

cd parquet_fdw || exit
make install
