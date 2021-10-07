#!/bin/bash

filename=backup.$(date '+%Y-%m-%d').tar.gz
tar -zcvf "$filename" data
mv "$filename" "$BACKUP_FOLDER"