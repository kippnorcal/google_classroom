#!/bin/bash

if [[ $1 = "--test" ]]
then
    echo "Running tests."
    exec pipenv run py.test -s -v tests.py
else
    echo "Starting loading script."
    exec pipenv run python main.py $@
fi