#!/bin/bash

airflow connections add 'conn1' \
--conn-type 'postgres' \
--conn-host 'db' \
--conn-login 'posgres' \
--conn-password 'password' \
--conn-schema 'test' \
--conn-port 5432