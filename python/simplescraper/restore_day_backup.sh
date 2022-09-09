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

  raw_day_dir=${RAW_DIR}/${DATA_SOURCE_NAME}/${entity}/$1/$2/$3

  if [ -d "$raw_day_dir" ]
  then

    echo "The raw day dir is not empty: $raw_day_dir"

  else

    backup_day_dir=${BACKUP_DIR}/${DATA_SOURCE_NAME}/${entity}/$1/$2
    backup_day_filename=${backup_day_dir}/${entity}.$1$2$3.tar.gz

    mkdir -p "$raw_day_dir"
    tar -xvzf "$backup_day_filename" -C "$raw_day_dir"

    echo "$1-$2-$3: Restored ${entity}"

  fi

done
