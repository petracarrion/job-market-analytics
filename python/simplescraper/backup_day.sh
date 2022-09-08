#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH" || exit

source .env

if [[ $# -ne 3 ]] ; then
    echo "Please provide a date as script parameters in the following format: year month day"
    echo "Example: $0 2022 12 01"
    exit 1
fi

for entity in job_description sitemap
do

  source=${RAW_DIR}/${DATA_SOURCE_NAME}/${entity}/$1/$2/$3

  if [ -d "$source" ]
  then

    target_dir=${BACKUP_DIR}/${DATA_SOURCE_NAME}/${entity}/$1/$2
    target_filename=${target_dir}/${entity}.$1$2$3.tar.gz
    mkdir -p "${target_dir}"
    tar -zcvf "${target_filename}" -C "${source}" .

  fi

done
