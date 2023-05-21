#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

which pip | grep dashy || (echo "Wrong venv!!!" && exit)

if ! pip show pip-tools; then
  pip install pip-tools
fi

pip-compile requirements.in --allow-unsafe
pip-sync
# pip install "apache-airflow[celery]==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.8.txt"
# pip install dbt-postgres
