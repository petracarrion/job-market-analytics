#!/usr/bin/env bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

for container in airflow-worker airflow-scheduler; do
  docker compose stop $container
  docker compose rm -f $container
  docker compose up $container -d
done
