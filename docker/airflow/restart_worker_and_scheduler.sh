#!/usr/bin/env bash

for container in airflow-worker airflow-scheduler; do
  docker compose stop $container
  docker compose rm -f $container
  docker compose up $container -d
done
