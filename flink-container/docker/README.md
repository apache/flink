# Apache Flink job cluster Docker image

In order to deploy a job cluster on Docker, one needs to create an image which contains the Flink binaries as well as the user code jars of the job to execute.
This directory contains a `build.sh` which facilitates the process.
The script takes a Flink distribution either from an official release, an archive or a local distribution and combines it with the specified job jar.  

## Installing Docker

Install the most recent stable version of [Docker](https://docs.docker.com/installation/).

## Building the Docker image

Images are based on the official Java Alpine (OpenJDK 8) image.

Before building the image, one needs to build the user code jars for the job.
Assume that the job jar is stored under `<PATH_TO_JOB_JAR>` 

If you want to build the Flink image from the version you have checked out locally run:

    build.sh --from-local-dist --job-jar <PATH_TO_JOB_JAR> --image-name <IMAGE_NAME>
    
Note that you first need to call `mvn package -pl flink-dist -am` to build the Flink binaries.

If you want to build the Flink image from an archive stored under `<PATH_TO_ARCHIVE>` run:

    build.sh --from-archive <PATH_TO_ARCHIVE> --job-jar <PATH_TO_JOB_JAR> --image-name <IMAGE_NAME>

If you want to build the Flink image for a specific version of Flink/Hadoop/Scala run:

    build.sh --from-release --flink-version 1.6.0 --hadoop-version 2.8 --scala-version 2.11 --image-name <IMAGE_NAME>
    
The script will try to download the released version from the Apache archive.

## Deploying via Docker compose

The `docker-compose.yml` contains the following parameters:

* `FLINK_DOCKER_IMAGE_NAME` - Image name to use for the deployment (default: `flink-job:latest`)
* `FLINK_JOB` - Name of the Flink job to execute (default: none)
* `DEFAULT_PARALLELISM` - Default parallelism with which to start the job (default: 1)
* `FLINK_JOB_ARGUMENTS` - Additional arguments which will be passed to the job cluster (default: none)
* `SAVEPOINT_OPTIONS` - Savepoint options to start the cluster with (default: none)

The parameters can be set by exporting the corresponding environment variable.

Deploy cluster and see config/setup log output (best run in a screen session)

        FLINK_DOCKER_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB=<JOB_NAME> docker-compose up

Deploy as a daemon (and return)

        FLINK_DOCKER_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB=<JOB_NAME> docker-compose up -d
        
In order to start the job with a different default parallelism set `DEFAULT_PARALLELISM`. 
This will automatically start `DEFAULT_PARALLELISM` TaskManagers:
        
        FLINK_DOCKER_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB=<JOB_NAME> DEFAULT_PARALLELISM=<DEFAULT_PARALLELISM> docker-compose up
        
In order to resume the job from a savepoint set `SAVEPOINT_OPTIONS`.
Supported options are `--fromSavepoint <SAVEPOINT_PATH>` and `--allowNonRestoredState` where `<SAVEPOINT_PATH>` is accessible from all containers.

        FLINK_DOCKER_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB=<JOB_NAME> SAVEPOINT_OPTIONS="--fromSavepoint <SAVEPOINT_PATH> --allowNonRestoredState" docker-compose up 
        
One can also provide additional job arguments via `FLINK_JOB_ARGUMENTS` which are passed to the job:
        
        FLINK_DOCKER_IMAGE_NAME=<IMAGE_NAME> FLINK_JOB=<JOB_NAME> FLINK_JOB_ARGUMENTS=<JOB_ARGUMENTS> docker-compose up

Scale the cluster up or down to *N* TaskManagers

        docker-compose scale taskmanager=<N>

Access the Job Manager container

        docker exec -it $(docker ps --filter name=flink_jobmanager --format={{.ID}}) /bin/sh
        
Access the web UI by going to `<IP_DOCKER_MACHINE>:8081` in your web browser.

Kill the cluster

        docker-compose kill
