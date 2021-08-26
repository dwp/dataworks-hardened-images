#!/bin/bash
set -x
set +e

ps

echo "Obtaining executor list..."
mysql -h $DB_HOST -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -e "SELECT DISTINCT host FROM $DB_NAME.executors;" > /executors.list

echo "Current Executors:"
cat /executors.list
killed_instances=0

# Executors list contains 'host' mysql header - ignore first line
for executor_host in $(tail -n +2 /executors.list); do
  attempts=1
  echo "Executor host '${executor_host}'"
  echo "Executor port '${EXECUTOR_PORT}'"

  while [[ $(nc -v -z -w5 $executor_host $EXECUTOR_PORT && echo $?) != 0 ]] && [[ $attempts -le 3 ]]; do
      if [[ $attempts = 3 ]]; then
        echo "$executor_host failed to connect on attempt '$attempts'. Removing from executors list"
        mysql -h $DB_HOST -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -e "DELETE FROM $DB_NAME.executors WHERE host LIKE '${executor_host}';"
        (( attempts ++ ))
        (( killed_instances ++ ))
        echo "Killed instances: '$killed_instances'"
        break
      else
        echo "$executor_host failed to connect on attempt '$attempts'. Retrying..."
        (( attempts ++ ))
        sleep 5
      fi
  done

  if [[ $attempts -lt 3 ]]; then
    echo "Successfully connected to '$executor_host' on attempt '$attempts'"
  fi
done

echo "Removed instances '${killed_instances}'"

#    Kill the container after clearing all dead hosts - required because Azkaban doesn't read database after starting
if [[ "${killed_instances}" != 0 ]]; then
  echo "Killing container after clearing dead executor hosts from database"
#  Flush to CW
  sleep 5
  ps
  PID=$(pidof java)
  kill $PID
  ps
fi
