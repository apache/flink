---
title:  "Docker Setup"
nav-title: Docker
nav-parent_id: deployment
nav-pos: 6
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

[Docker](https://www.docker.com) is a popular container runtime.
There are Docker images for Apache Flink available [on Docker Hub](https://hub.docker.com/_/flink).
You can use the docker images to deploy a *Session* or *Job cluster* in a containerized environment, e.g.,
[standalone Kubernetes](kubernetes.html) or [native Kubernetes](native_kubernetes.html).

* This will be replaced by the TOC
{:toc}

## Docker Hub Flink images

The [Flink Docker repository](https://hub.docker.com/_/flink/) is hosted on
Docker Hub and serves images of Flink version 1.2.1 and later.

### Image tags

Images for each supported combination of Flink and Scala versions are available, and
[tag aliases](https://hub.docker.com/_/flink?tab=tags) are provided for convenience.

For example, you can use the following aliases:

* `flink:latest` → `flink:<latest-flink>-scala_<latest-scala>`
* `flink:1.11` → `flink:1.11.<latest-flink-1.11>-scala_2.11`

<span class="label label-info">Note</span> Prio to Flink 1.5 version, Hadoop dependencies were always bundled with Flink.
You can see that certain tags include the version of Hadoop, e.g. (e.g. `-hadoop28`).
Beginning with Flink 1.5, image tags that omit the Hadoop version correspond to Hadoop-free releases of Flink
that do not include a bundled Hadoop distribution.

## How to run Flink image

The Flink image contains a regular Flink distribution with its default configuration and a standard entry point script.
You can run its entry point in the following modes:
* [Flink Master]({{ site.baseurl }}/concepts/glossary.html#flink-master) for [a Session cluster](#start-a-session-cluster)
* *Flink Master* for [a Job cluster](#start-a-job-cluster)
* [TaskManager]({{ site.baseurl }}/concepts/glossary.html#flink-taskmanager) for any cluster

This allows you to deploy a standalone cluster (Session or Job) in any containerised environment, for example:
* manually in a local Docker setup,
* [in a Kubernetes cluster](kubernetes.html),
* [with Docker Compose](#flink-with-docker-compose),
* [with Docker swarm](#flink-with-docker-swarm).

<span class="label label-info">Note</span> [The native Kubernetes](native_kubernetes.html) also runs the same image by default
and deploys *TaskManagers* on demand so that you do not have to do it manually.

The next chapters describe how to start a single Flink Docker container for various purposes.

### Start a Session Cluster

A *Flink Session cluster* can be used to run multiple jobs. Each job needs to be submitted to the cluster after it has been deployed.
To deploy a *Flink Session cluster* with Docker, you need to start a *Flink Master* container:

```sh
docker run flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} jobmanager
```

and one or more *TaskManager* containers:

```sh
docker run flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} taskmanager
```

### Start a Job Cluster

A *Flink Job cluster* is a dedicated cluster which runs a single job.
In this case, you deploy the cluster with the job as one step, thus, there is no extra job submission needed.

The *job artifacts* are included into the class path of Flink's JVM process within the container and consist of:
* your job jar, which you would normally submit to a *Session cluster* and
* all other necessary dependencies or resources, not included into Flink.

To deploy a cluster for a single job with Docker, you need to
* make *job artifacts* available locally *in all containers* under `/opt/flink/usrlib`,
* start a *Flink Master* container in the *Job Cluster* mode
* start the required number of *TaskManager* containers.

To make the **job artifacts available** locally in the container, you can

* **either mount a volume** (or multiple volumes) with the artifacts to `/opt/flink/usrlib` when you start
the *Flink Master* and *TaskManagers*:

    ```sh
    docker run \
        --mount type=bind,src=/host/path/to/job/artifacts1,target=/opt/flink/usrlib/artifacts1 \
        --mount type=bind,src=/host/path/to/job/artifacts2,target=/opt/flink/usrlib/artifacts2 \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} standalone-job \
        --job-classname com.job.ClassName \
        --job-id <job id> \
        [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
        [job arguments]

    docker run \
        --mount type=bind,src=/host/path/to/job/artifacts1,target=/opt/flink/usrlib/artifacts1 \
        --mount type=bind,src=/host/path/to/job/artifacts2,target=/opt/flink/usrlib/artifacts2 \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} taskmanager
    ```

* **or extend the Flink image** by writing a custom `Dockerfile`, build it and use it for starting the *Flink Master* and *TaskManagers*:

    ```dockerfile
    FROM flink
    ADD /host/path/to/job/artifacts/1 /opt/flink/usrlib/artifacts/1
    ADD /host/path/to/job/artifacts/2 /opt/flink/usrlib/artifacts/2
    ```

    ```sh
    docker build -t flink_with_job_artifacts .
    docker run \
        flink_with_job_artifacts standalone-job \
        --job-classname com.job.ClassName \
        --job-id <job id> \
        [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
        [job arguments]

    docker run flink_with_job_artifacts taskmanager
    ```

The `standalone-job` argument starts a *Flink Master* container in the *Job Cluster* mode.

#### Flink Master additional command line arguments

You can provide the following additional command line arguments to the cluster entrypoint:

* `--job-classname <job class name>`: Class name of the job to run.

  By default, Flink scans its class path for a JAR with a Main-Class or program-class manifest entry and chooses it as the job class.
  Use this command line argument to manually set the job class.
  This argument is required in case that no or more than one JAR with such a manifest entry is available on the class path.

* `--job-id <job id>` (optional): Manually set a Flink job ID for the job (default: 00000000000000000000000000000000)

* `--fromSavepoint /path/to/savepoint` (optional): Restore from a savepoint

  In order to resume from a savepoint, you also need to pass the savepoint path.
  Note that `/path/to/savepoint` needs to be accessible in all Docker containers of the cluster
  (e.g., storing it on a DFS or from the mounted volume or adding it to the image).

* `--allowNonRestoredState` (optional): Skip broken savepoint state

  Additionally you can specify this argument to allow that savepoint state is skipped which cannot be restored.

If the main function of the user job main class accepts arguments, you can also pass them at the end of the `docker run` command.

## Customize Flink image

When you run the Flink containers, there may be a need to customize them.
The next chapters describe some how-tos of what you can usually customize.

### Configure options

When you run Flink image, you can also change its configuration options by setting the environment variable `FLINK_PROPERTIES`:

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: host
taskmanager.numberOfTaskSlots: 3
blob.server.port: 6124
"
docker run --env FLINK_PROPERTIES=${FLINK_PROPERTIES} flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
```

The [`jobmanager.rpc.address`](../config.html#jobmanager-rpc-address) option must be configured, others are optional to set.

The environment variable `FLINK_PROPERTIES` should contain a list of Flink cluster configuration options separated by new line,
the same way as in the `flink-conf.yaml`.

### Provide custom configuration

The configuration files (`flink-conf.yaml`, logging, hosts etc) are located in the `/opt/flink/conf` directory in the Flink image.
To provide a custom location for the Flink configuration files, you can

* **either mount a volume** with the custom configuration files to this path `/opt/flink/conf` when you run the Flink image:

    ```sh
    docker run \
        --mount type=bind,src=/host/path/to/custom/conf,target=/opt/flink/conf \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
    ```

* or add them to your **custom Flink image**, build and run it:

    ```dockerfile
    FROM flink
    ADD /host/path/to/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
    ADD /host/path/to/log4j.properties /opt/flink/conf/log4j.properties
    ```

<span class="label label-warning">Warning!</span> The mounted volume must contain all necessary configuration files.
The `flink-conf.yaml` file must have write permission so that the Docker entry point script can modify it in certain cases.

### Using plugins

As described in the [plugins]({{ site.baseurl }}/ops/plugins.html) documentation page: in order to use plugins they must be
copied to the correct location in the Flink installation for them to work.

If you want to enable plugins provided with Flink, you can pass the environment variable `ENABLE_BUILT_IN_PLUGINS`
when you run the Flink image.
The `ENABLE_BUILT_IN_PLUGINS` should contain a list of plugin jar file names separated by `;`.

    ```sh
    docker run \
        --env ENABLE_BUILT_IN_PLUGINS=flink-plugin1.jar;flink-plugin2.jar \
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} <jobmanager|standalone-job|taskmanager>
    ```

There are also more [advanced ways](#advanced-customization) for customizing the Flink image.

### Advanced customization

There are several ways in which you can further customize the Flink image:

* install custom software (e.g. python)
* enable (symlink) optional libraries or plugins from `/opt/flink/opt` into `/opt/flink/lib` or `/opt/flink/plugins`
* add other libraries to `/opt/flink/lib` (e.g. [hadoop](hadoop.html#adding-hadoop-to-lib))
* add other plugins to `/opt/flink/plugins`

you can achieve this in several ways:

* **override the container entry point** with a custom script where you can run any bootstrap actions.
At the end you can call the standard `/docker-entrypoint.sh` script of the Flink image with the same arguments
as described in [how to run the Flink image](#how-to-run-flink-image).

  The following example creates a custom entry point script which enables more libraries and plugins.
  The custom script, custom library and plugin are provided from a mounted volume.
  Then it runs the standard entry point script of the Flink image:

    ```sh
    # create custom_lib.jar
    # create custom_plugin.jar

    echo "
    ln -fs /opt/flink/opt/flink-queryable-state-runtime-*.jar /opt/flink/lib/.  # enable an optional library
    ln -fs /mnt/custom_lib.jar /opt/flink/lib/.  # enable a custom library

    mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
    ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.  # enable an optional plugin

    mkdir -p /opt/flink/plugins/custom_plugin
    ln -fs /mnt/custom_plugin.jar /opt/flink/plugins/custom_plugin/.  # enable a custom plugin

    /docker-entrypoint.sh <jobmanager|standalone-job|taskmanager>
    " > custom_entry_point_script.sh

    chmod 755 custom_entry_point_script.sh

    docker run \
        --mount type=bind,src=$(pwd),target=/mnt
        flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} /mnt/custom_entry_point_script.sh
    ```

* **extend the Flink image** by writing a custom `Dockerfile` and build a custom image:

    ```dockerfile
    FROM flink

    RUN set -ex; apt-get update; apt-get -y install python

    ADD /host/path/to/flink-conf.yaml /container/local/path/to/custom/conf/flink-conf.yaml
    ADD /host/path/to/log4j.properties /container/local/path/to/custom/conf/log4j.properties

    RUN ln -fs /opt/flink/opt/flink-queryable-state-runtime-*.jar /opt/flink/lib/.

    RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
    RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

    ENV VAR_NAME value
    ```

    ```sh
    docker build -t custom_flink_image .
    # optional push to your docker image registry if you have it,
    # e.g. to distribute the custom image to your cluster
    docker push custom_flink_image
    ```

{% top %}

## Flink with Docker Compose

[Docker Compose](https://docs.docker.com/compose/) is a way to run a group of Docker containers locally.
The next chapters show examples of configuration files to run Flink.

### Usage

* Create the `yaml` files with the container configuration, check examples for:
    * [Session cluster](#session-cluster-with-docker-compose)
    * [Job cluster](#job-cluster-with-docker-compose)

    See also [the Flink Docker image tags](#image-tags) and [how to customize the Flink Docker image](#advanced-customization)
    for usage in the configuration files.

* Launch a cluster in the foreground

    ```sh
    docker-compose up
    ```

* Launch a cluster in the background

    ```sh
    docker-compose up -d
    ```

* Scale the cluster up or down to *N TaskManagers*

    ```sh
    docker-compose scale taskmanager=<N>
    ```

* Access the *Flink Master* container

    ```sh
    docker exec -it $(docker ps --filter name=jobmanager --format={% raw %}{{.ID}}{% endraw %}) /bin/sh
    ```

* Kill the cluster

    ```sh
    docker-compose kill
    ```

* Access Web UI

    When the cluster is running, you can visit the web UI at [http://localhost:8081](http://localhost:8081).
    You can also use the web UI to submit a job to a *Session cluster*.

* To submit a job to a *Session cluster* via the command line, you can either

  * use [Flink CLI](..//cli.html) on the host if it is installed:

    ```sh
    flink run -d -c ${JOB_CLASS_NAME} /job.jar
    ```

  * or copy the JAR to the *Flink Master* container and submit the job using the [CLI](..//cli.html) from there, for example:

    ```sh
    JOB_CLASS_NAME="com.job.ClassName"
    MASTER_CONTAINER=$(docker ps --filter name=jobmanager --format={% raw %}{{.ID}}{% endraw %}))
    docker cp path/to/jar "${MASTER_CONTAINER}":/job.jar
    docker exec -t -i "${MASTER_CONTAINER}" flink run -d -c ${JOB_CLASS_NAME} /job.jar
    ```

### Session Cluster with Docker Compose

```yaml
version: "2.2"
services:
  jobmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
```

### Job Cluster with Docker Compose

The artifacts must be available in the Flink containers, check details [here](#start-a-job-cluster).
See also [how to specify the Flink Master arguments](#flink-master-additional-command-line-arguments)
in the `command` for the `jobmanager` service.

```yaml
version: "2.2"
services:
  jobmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    ports:
      - "8081:8081"
    command: standalone-job --job-classname com.job.ClassName [--job-id <job id>] [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] [job arguments]
    volumes:
      - /host/path/to/job/artifacts:/opt/flink/usrlib
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        parallelism.default: 2

  taskmanager:
    image: flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %}
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    volumes:
      - /host/path/to/job/artifacts:/opt/flink/usrlib
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        parallelism.default: 2
```

{% top %}

## Flink with Docker Swarm

The [Docker swarm](https://docs.docker.com/engine/swarm) is a container orchestration tool, that
allows you to manage multiple containers deployed across multiple host machines.

The following chapters contain examples of how to configure and start *Flink Master* and *TaskManager* containers.
You can adjust them accordingly to start a cluster.
See also [the Flink Docker image tags](#image-tags) and [how to customize the Flink Docker image](#advanced-customization) for usage in the provided scripts.

The port `8081` is exposed for the Flink Web UI access.
If you run the swarm locally, you can visit the web UI at [http://localhost:8081](http://localhost:8081) after starting the cluster.

### Session Cluster with Docker Swarm

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: flink-session-jobmanager
taskmanager.numberOfTaskSlots: 2
"

# Create overlay network
docker network create -d overlay flink-session

# Create the Flink Master service
docker service create \
  --name flink-session-jobmanager \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  -p 8081:8081 \
  --network flink-session \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    jobmanager

# Create the TaskManager service (scale this out as needed)
docker service create \
  --name flink-session-taskmanager \
  --replicas 2 \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --network flink-session \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    taskmanager
```

### Job Cluster with Docker Swarm

```sh
FLINK_PROPERTIES="jobmanager.rpc.address: flink-master
taskmanager.numberOfTaskSlots: 2
"

# Create overlay network
docker network create -d overlay flink-job

# Create the Flink Master service
docker service create \
  --name flink-master \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --mount type=bind,source=/host/path/to/job/artifacts,target=/opt/flink/usrlib \
  -p 8081:8081 \
  --network flink-job \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    standalone-job \
    --job-classname com.job.ClassName \
    [--job-id <job id>] \
    [--fromSavepoint /path/to/savepoint [--allowNonRestoredState]] \
    [job arguments]

# Create the TaskManager service (scale this out as needed)
docker service create \
  --name flink-job-taskmanager \
  --replicas 2 \
  --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
  --mount type=bind,source=/host/path/to/job/artifacts,target=/opt/flink/usrlib \
  --network flink-job \
  flink:{% if site.is_stable %}{{site.version}}-scala{{site.scala_version_suffix}}{% else %}latest{% endif %} \
    taskmanager
```

The *job artifacts* must be available in the *Flink Master* container, as outlined [here](#start-a-job-cluster).
See also [how to specify the Flink Master arguments](#flink-master-additional-command-line-arguments) to pass them
to the `flink-master` container.

The example assumes that you run the swarm locally and expects the *job artifacts* to be in `/host/path/to/job/artifacts`.
It also mounts the host path with the artifacts as a volume to the container's path `/opt/flink/usrlib`.

{% top %}
