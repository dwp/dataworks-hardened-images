#!/usr/bin/env bash

source ./environment.sh

init
terraform init
terraform apply -auto-approve
