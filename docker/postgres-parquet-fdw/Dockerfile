FROM postgres:14.0 as postgres-parquet-fdw

COPY *.sh /usr/local/bin/

RUN s1-download-arrow.sh
RUN s2-install-arrow.sh
RUN s3-download-parquet-fdw.sh
RUN s4-install-parquet-fdw.sh
