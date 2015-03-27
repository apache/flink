#!/usr/bin/env bash
#Kill cluster and remove all containers
docker-compose kill
#docker rm $(docker ps -a -q)

#make sure the config file script is executable
chmod +x flink/config-flink.sh

#rebuild images
docker build -t="base" base
docker build -t="flink" flink
