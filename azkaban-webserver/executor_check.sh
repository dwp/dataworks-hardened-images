#!/bin/sh
set -e

mysql $DB_NAME -e < SELECT DISTINCT host FROM $DB_NAME.executors; > executors.list

for executor_host in $(cat executors.list);
do
    if [[ $(nc -v -z -w5 $executor_host 7082 && echo $?) != 0 ]]; then
        echo "$executor_host appears to be down, removing from active list..."
        mysql $DB_NAME -e DELETE FROM $DB_NAME.executors WHERE host LIKE \'${executor_host}\';
    else
        echo "$executor_host available"
    fi
done