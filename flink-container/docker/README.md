# Apache Flink cluster deployment on docker using docker-compose

## Installation

Install the most recent stable version of docker
https://docs.docker.com/installation/

## Build

Images are based on the official Java Alpine (OpenJDK 8) image. If you want to
build the flink image run:

    sh build.sh --job-jar /path/to/job/jar/job.jar --image-name flink:job

or

    docker build -t flink .

If you want to build the container for a specific version of flink/hadoop/scala
you can configure it in the respective args:

    docker build --build-arg FLINK_VERSION=1.0.3 --build-arg HADOOP_VERSION=26 --build-arg SCALA_VERSION=2.10 -t "flink:1.0.3-hadoop2.6-scala_2.10" flink

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
