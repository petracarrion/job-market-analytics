#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

ulimit -n 2048
flask run --host=0.0.0.0 --port=3001 --without-threads
