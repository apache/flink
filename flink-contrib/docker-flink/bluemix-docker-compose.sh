#!/bin/sh
export COMPOSE_HTTP_TIMEOUT=120
export IMAGENAME="registry.eu-gb.bluemix.net/"`cf ic namespace get`"/flink"
docker-compose  -f docker-compose-bluemix.yml up -d
