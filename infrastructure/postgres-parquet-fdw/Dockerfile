FROM postgres:14.0 as postgres-parquet-fdw

COPY *.sh /usr/local/bin/

RUN s2-download-arrow.sh
RUN s3-install-arrow.sh
RUN s4-clone-parquet-fdw.sh
RUN s5-make-parquet-fdw.sh
