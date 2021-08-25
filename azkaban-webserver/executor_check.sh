#!/bin/bash
set -e

echo "Obtaining executor list..."
mysql -h $DB_HOST -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -e "SELECT DISTINCT host FROM $DB_NAME.executors;" > /executors.list

echo "Current Executors:"
cat /executors.list

# Executors list contains 'host' mysql header - ignore first line
for executor_host in $(tail -n +2 /executors.list);
do
  killed_instances=0
  echo "Executor host '${executor_host}'"
  echo "Executor port '${executor_port}'"

    if [[ $(nc -v -z -w5 $executor_host $EXECUTOR_PORT && echo $?) != 0 ]]; then
        echo "$executor_host appears to be down, removing from active list..."
        mysql -h $DB_HOST -u $DB_USERNAME -p$DB_PASSWORD $DB_NAME -e "DELETE FROM $DB_NAME.executors WHERE host LIKE '${executor_host}';"
        (( killed_instances++ ))
    else
        echo "$executor_host available"
    fi

#    Kill the container after clearing all dead hosts - required because Azkaban doesn't read database after starting
  if [[ "${killed_instances}" != 0 ]]; then
    echo "Killing container after clearing dead executor hosts from database"
    pkill -9 java
  fi
done
