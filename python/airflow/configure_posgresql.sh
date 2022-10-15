#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

sudo -u postgres psql -c "CREATE DATABASE ${AIRFLOW_DATABASE_NAME};"

sudo -u postgres psql -c "CREATE USER ${AIRFLOW_DATABASE_USERNAME} WITH ENCRYPTED PASSWORD '${AIRFLOW_DATABASE_PASSWORD};'"

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DATABASE_NAME} TO ${AIRFLOW_DATABASE_USERNAME};"
sudo -u postgres psql -c "GRANT ALL ON SCHEMA public TO ${AIRFLOW_DATABASE_USERNAME};"
