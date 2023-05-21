#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

source "${VENV_ACTIVATE}"

pip install -r requirements.in
pip freeze > requirements.txt
