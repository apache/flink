# Apache Flink cluster deployment on Docker using Docker-Compose

## Installation
### Install Docker

https://docs.docker.com/installation/

if you have issues with Docker-Compose versions incompatible with your version
of Docker try:

    curl -sSL https://get.docker.com/ubuntu/ | sudo sh

### Install Docker-Compose

    curl -L https://github.com/docker/compose/releases/download/1.1.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

### Get the repo

### Build the images

Images are based on the official Java Alpine (OpenJDK 8) image and run
supervisord to stay alive when running containers. If you want to build the
flink image run:

    sh build.sh

If you want to build a specific version of flink/hadoop/scala you can configure
the respective args:

    docker build --build-arg FLINK_VERSION=1.0.3 --build-arg HADOOP_VERSION=26 --build-arg SCALA_VERSION=2.10 -t "flink:1.0.3-hadoop2.6-scala_2.10" flink

### Deploy

- Deploy cluster and see config/setup log output (best run in a screen session)

        docker-compose up

- Deploy as a daemon (and return)

        docker-compose up -d

- Scale the cluster up or down to *N* TaskManagers

        docker-compose scale taskmanager=<N>

- Access the JobManager container

        docker exec -it $(docker ps --filter name=flink_jobmanager --format={{.ID}}) /bin/sh

- Kill the cluster

        docker-compose kill

- Upload a jar to the cluster

        for i in $(docker ps --filter name=flink --format={{.ID}}); do
            docker cp <your_jar> $i:/<your_path>
        done

- Run a topology

        docker exec -it $(docker ps --filter name=flink_jobmanager --format={{.ID}}) sh -c '/usr/local/flink/bin/flink run -c <your_class> <your_jar> <your_params>'

### Ports

- The Web Client is on port `48081`
- JobManager RPC port `6123` (default, not exposed to host)
- TaskManagers RPC port `6122` (default, not exposed to host)
- TaskManagers Data port `6121` (default, not exposed to host)

Edit the `docker-compose.yml` file to edit port settings.
