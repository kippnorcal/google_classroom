#!/bin/bash

docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from app
docker-compose -f docker-compose.test.yml down --volumes
