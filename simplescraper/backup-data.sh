#!/bin/bash

source .env

filename=backup.$(date '+%Y-%m-%d').tar.gz
tar -zcvf "${filename}" "${DATA_DIR}"
mv "${filename}" "${BACKUP_FOLDER}"