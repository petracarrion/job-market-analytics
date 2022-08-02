#!/bin/bash

source .env

azcopy login --service-principal  --application-id "$AZCOPY_SPA_APPLICATION_ID" --tenant-id="$AZCOPY_TENANT_ID"

azcopy sync "${RAW_DIR}" "${AZURE_STORAGE_CONTAINER_RAW_DIR_URL}" --recursive --exclude-pattern=".*"
