#!/bin/bash

if [[ $1 = "--test" ]]
then
    echo "Running tests."
    exec pipenv run pytest -s -v
else
    echo "Starting loading script."
    exec pipenv run python main.py $@
fi