#!/usr/bin/env bash

apt-get update
apt-get install -y git

git clone https://github.com/adjust/parquet_fdw.git
