#!/bin/bash

source .env

airflow users create \
        --role Admin \
        --username "${AIRFLOW_USERNAME}" \
        --password "${AIRFLOW_PASSWORD}" \
        --email "${AIRFLOW_EMAIL}" \
        --firstname "${AIRFLOW_FIRSTNAME}" \
        --lastname "${AIRFLOW_LASTNAME}"

airflow users delete -e admin
