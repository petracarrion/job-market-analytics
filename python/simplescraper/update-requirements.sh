#!/bin/bash

pip-compile requirements.in --allow-unsafe
pip-sync
pip install "apache-airflow[celery]==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.8.txt"

