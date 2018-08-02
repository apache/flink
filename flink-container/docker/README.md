# Apache Flink job cluster deployment on docker using docker-compose

## Installation

Install the most recent stable version of docker
https://docs.docker.com/installation/

## Build

Images are based on the official Java Alpine (OpenJDK 8) image. If you want to
build the flink image run:

    build.sh --from-local-dist --job-jar /path/to/job/jar/job.jar --image-name flink:job

If you want to build the container for a specific version of flink/hadoop/scala
you can configure it in the respective args:

    docker build --build-arg FLINK_VERSION=1.6.0 --build-arg HADOOP_VERSION=28 --build-arg SCALA_VERSION=2.11 -t "flink:1.6.0-hadoop2.8-scala_2.11" flink

## Deploy

- Deploy cluster and see config/setup log output (best run in a screen session)

        docker-compose up

- Deploy as a daemon (and return)

        docker-compose up -d

- Scale the cluster up or down to *N* TaskManagers

        docker-compose scale taskmanager=<N>

- Access the Job Manager container

        docker exec -it $(docker ps --filter name=flink_jobmanager --format={{.ID}}) /bin/sh

- Kill the cluster

        docker-compose kill
