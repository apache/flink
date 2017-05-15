---
title:  "Docker Setup"
nav-title: Docker
nav-parent_id: deployment
nav-pos: 4
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

[Docker](https://www.docker.com) is a popular container runtime. There are
official Flink Docker images available on Docker Hub which can be used directly
or extended to better integrate into a production environment.

* This will be replaced by the TOC
{:toc}

## Official Flink Docker Images

The [official Flink Docker repository](https://hub.docker.com/_/flink/) is
hosted on Docker Hub and serves images of Flink version 1.2.1 and later.

Images for each supported combination of Hadoop and Scala are available, and
tag aliases are provided for convenience.

For example, the following aliases can be used: *(`1.2.y` indicates the latest
release of Flink 1.2)*

* `flink:latest` →
`flink:<latest-flink>-hadoop<latest-hadoop>-scala_<latest-scala>`
* `flink:1.2` → `flink:1.2.y-hadoop27-scala_2.11`
* `flink:1.2.1-scala_2.10` → `flink:1.2.1-hadoop27-scala_2.10`
* `flink:1.2-hadoop26` → `flink:1.2.y-hadoop26-scala_2.11`

<!-- NOTE: uncomment when docker-flink/docker-flink/issues/14 is resolved. -->
<!--
Additionally, images based on Alpine Linux are available. Reference them by
appending `-alpine` to the tag. For the Alpine version of `flink:latest`, use
`flink:alpine`.

For example:

* `flink:alpine`
* `flink:1.2.1-alpine`
* `flink:1.2-scala_2.10-alpine`
-->

## Flink with Docker Compose

[Docker Compose](https://docs.docker.com/compose/) is a convenient way to run a
group of Docker containers locally.

An [example config file](https://github.com/docker-flink/examples) is available
on GitHub.

### Usage

* Launch a cluster in the foreground

        docker-compose up

* Launch a cluster in the background

        docker-compose up -d

* Scale the cluster up or down to *N* TaskManagers

        docker-compose scale taskmanager=<N>

When the cluster is running, you can visit the web UI at [http://localhost:8081
](http://localhost:8081) and submit a job.

To submit a job via the command line, you must copy the JAR to the Jobmanager
container and submit the job from there.

For example:

{% raw %}
    $ JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
    $ docker cp path/to/jar "$JOBMANAGER_CONTAINER":/job.jar
    $ docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar
{% endraw %}

{% top %}
