#!/bin/bash

SECONDS=0

source .env

filename=backup.$(date '+%Y-%m-%d').tar.gz
tar -zcvf "${filename}" "${RAW_DIR}"
mv "${filename}" "${BACKUP_DIR}"

duration=$SECONDS
echo "${0##*/}: $((duration / 60)) minutes and $((duration % 60)) seconds elapsed."

say -v Zuzana A je to
