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
    diff <(cd "$source" && find . | grep -E '.xml$|.html$' | sort) <(tar -tf "$target_filename" | grep -E '.xml$|.html$' | sort)
    error_code=$?
    if [ $error_code -ne 0 ];
    then
      echo "$1-$2-$3: NOT OK" >&2
      exit 1
    fi

  fi

done

echo "$1-$2-$3: OK"
