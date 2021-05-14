#!/usr/bin/env bash

init() {
  aws_local configure set aws_access_key_id access_key_id
  aws_local configure set aws_secret_access_key secret_access_key
}

add_status_item() {
  add_item "$(status_item_id)"
}

add_item() {
  local id=${1:?Usage: ${FUNCNAME[0]} id}
  local status=${2:-Exporting}
  local files_exported=${3:-0}
  local files_sent=${4:-0}

  # shellcheck disable=SC2027
  # shellcheck disable=SC2086
  aws_local dynamodb delete-item \
    --table-name "$(data_pipeline_metadata_table)" \
    --key "{"$id"}" \
    --return-values "ALL_OLD"

  # shellcheck disable=SC2086
  aws_local dynamodb put-item \
    --table-name "$(data_pipeline_metadata_table)" \
    --item '{'$id', "CollectionStatus": {"S":"'$status'"}, "FilesExported":{"N":"'$files_exported'"},"FilesSent":{"N":"'$files_sent'"}}'
}

get_status_item() {
  get_item "$(status_item_id)"
}

get_item() {
  local id=${1:?Usage: ${FUNCNAME[0]} id}
  aws_local dynamodb get-item \
    --table-name "$(data_pipeline_metadata_table)" \
    --key "$id"
}

status_item_id() {
  echo '"CorrelationId":{"S":"s3-export"},"CollectionName":{"S":"db.database.collection"}'
}

data_pipeline_metadata_table() {
  echo data_pipeline_metadata
}

aws_local() {
  aws --endpoint-url http://aws:4566 --region=eu-west-2 "$@"
}
