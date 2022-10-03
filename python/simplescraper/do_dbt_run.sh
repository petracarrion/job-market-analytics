#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env
source "${DBT_VENV_ACTIVATE}"

cd "$DBT_DIR" || exit

dbt run
