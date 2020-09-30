#!/bin/bash

docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from app
TEST_RESULT=$?
docker-compose -f docker-compose.test.yml down --volumes
exit $TEST_RESULT