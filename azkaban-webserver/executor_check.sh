#!/bin/bash
set -e

echo "Obtaining executor list..."
mysql -h $DB_HOST -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -e "SELECT DISTINCT host FROM $DB_NAME.executors;" > /executors.list

echo "Current Executors:"
cat /executors.list

# Executors list contains 'host' mysql header - ignore first line
for executor_host in $(tail -n +2 /executors.list); do
  killed_instances=0
  attempts=1
  echo "Executor host '${executor_host}'"
  echo "Executor port '${executor_port}'"

  while [[ $(nc -v -z -w 5 $executor_host $executor_port && echo $?) != 0 ]] && [[ $attempts -le 3 ]]; do
      if [[ $attempts = 3 ]]; then
        echo "db sql"
        (( attempts ++ ))
        (( killed_instances++ ))
      else
        echo "$executor_host failed to connect. Attempt '$attempts'. Retrying..."
        (( attempts ++ ))
        sleep 5
      fi
  done

  if [[ $attempts < 3 ]]; then
    echo "Successfully connected to '$executor_host' on attempt '$attempts'"
  fi
done

#    Kill the container after clearing all dead hosts - required because Azkaban doesn't read database after starting
if [[ "${killed_instances}" != 0 ]]; then
  echo "Killing container after clearing dead executor hosts from database"
  pkill -9 java
fi
