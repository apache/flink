---
title: Mesos
weight: 6
type: docs
aliases:
  - /zh/deployment/resource-providers/mesos.html
  - /zh/ops/deployment/mesos.html
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

# Flink on Mesos

{{< hint warning >}}
Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the future (see 
[FLINK-22352](https://issues.apache.org/jira/browse/FLINK-22352) for further details).
{{< /hint >}}

## Getting Started

This *Getting Started* section guides you through setting up a fully functional Flink Cluster on Mesos.

### Introduction

[Apache Mesos](http://mesos.apache.org/) is another resource provider supported by 
Apache Flink. Flink utilizes the worker's provided by Mesos to run its TaskManagers.
Apache Flink provides the script `bin/mesos-appmaster.sh` to initiate the Flink 
on Mesos cluster.

### Preparation

Flink on Mesos expects a Mesos cluster to be around. It also requires the Flink binaries being 
deployed. Additionally, Hadoop needs to be installed on the very same machine.

Flink provides `bin/mesos-appmaster.sh` to initiate a Flink on Mesos cluster. A Mesos application master 
will be created (i.e. a JobManager process with Mesos support) which will utilize the Mesos workers to 
run Flink's TaskManager processes.

For `bin/mesos-appmaster.sh` to work, you have to set the two variables `HADOOP_CLASSPATH` and 
`MESOS_NATIVE_JAVA_LIBRARY`:

```bash
$ export HADOOP_CLASSPATH=$(hadoop classpath)
$ export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so
```

`MESOS_NATIVE_JAVA_LIBRARY` needs to point to Mesos' native Java library. The library name `libmesos.so` 
used above refers to Mesos' Linux library. Running Mesos on MacOS would require you to use 
`libmesos.dylib` instead.

### Starting a Flink Session on Mesos

Connect to the machine which matches all the requirements listed in the [Preparation section](#preparation).
Change into Flink's home directory and call `bin/mesos-appmaster.sh`:

```bash
# (0) set required environment variables
$ export HADOOP_CLASSPATH=$(hadoop classpath)
$ export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so

# (1) create Flink on Mesos cluster
$ ./bin/mesos-appmaster.sh \
      -Dmesos.master=<mesos-master>:5050 \
      -Djobmanager.rpc.address=<jobmanager-host> \
      -Dmesos.resourcemanager.framework.user=<flink-user> \
      -Dmesos.resourcemanager.tasks.cpus=6

# (2) execute Flink job passing the relevant configuration parameters
$ ./bin/flink run \
      --detached \
      --target remote \
      -Djobmanager.rpc.address=<jobmanager-host> \
      -Dmesos.resourcemanager.framework.user=<flink-user> \
      -Dmesos.master=<mesos-master>:5050 \
      examples/streaming/WindowJoin.jar
```

The commands above use a few placeholders that need to be substituted by settings of the actual 
underlying cluster:
* `<mesos-master>` refers to the Mesos master's IP address or hostname. 
* `<jobmanager-host>` refers to the host that executes `bin/mesos-appmaster.sh` which is starting 
  Flink's JobManager process. It's important to not use `localhost` or `127.0.0.1` as this parameter 
  is being shared with the Mesos cluster and the TaskManagers.
* `<flink-user>` refers to the user that owns the Mesos master's Flink installation directory (see Mesos' 
  documentation on [specifying a user](http://mesos.apache.org/documentation/latest/fetcher/#specifying-a-user-name)
  for further details).

The `run` action requires `--target` to be set to `remote`. Refer to the [CLI documentation]({{< ref "docs/deployment/cli" >}}) 
for further details on that parameter.

The Flink on Mesos cluster is now deployed in [Session Mode]({{< ref "docs/deployment/overview" >}}#session-mode).
Note that you can run multiple Flink jobs on a Session cluster. Each job needs to be submitted to the 
cluster. TaskManagers are deployed on the Mesos workers as needed. Keep in mind that you can only run as 
many jobs as the Mesos cluster allows in terms of resources provided by the Mesos workers. Play around 
with Flink's parameters to find the right resource utilization for your needs.

Check out [Flink's Mesos configuration]({{< ref "docs/deployment/config" >}}#mesos) to further influence 
the resources Flink on Mesos is going to allocate.

## Deployment Modes

For production use, we recommend deploying Flink Applications in the 
[Per-Job Mode]({{< ref "docs/deployment/overview" >}}#per-job-mode), as it provides a better isolation 
for each job.

### Application Mode

Flink on Mesos does not support [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode).

### Per-Job Cluster Mode

A job which is executed in [Per-Job Cluster Mode]({{< ref "docs/deployment/overview" >}}#per-job-mode) spins 
up a dedicated Flink cluster that is only used for that specific job. No extra job submission is 
needed. `bin/mesos-appmaster-job.sh` is used as the startup script. It will start a Flink cluster 
for a dedicated job which is passed as a JobGraph file. This file can be created by applying the 
following code to your Job source code:
```java
final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
final String jobGraphFilename = "job.graph";
File jobGraphFile = new File(jobGraphFilename);
try (FileOutputStream output = new FileOutputStream(jobGraphFile);
    ObjectOutputStream obOutput = new ObjectOutputStream(output)){
    obOutput.writeObject(jobGraph);
}
```

Flink on Mesos Per-Job cluster can be started in the following way:
```bash
# (0) set required environment variables
$ export HADOOP_CLASSPATH=$(hadoop classpath)
$ export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so

# (1) create Per-Job Flink on Mesos cluster
$ ./bin/mesos-appmaster-job.sh \
      -Dmesos.master=<mesos-master>:5050 \
      -Djobmanager.rpc.address=<jobmanager-host> \
      -Dmesos.resourcemanager.framework.user=<flink-user> \
      -Dinternal.jobgraph-path=<job-graph-file>
``` 

`<job-graph-file>` refers to the path of the uploaded JobGraph file defining the job that shall be 
executed on the Per-Job Flink cluster in the command above. The meaning of `<mesos-master>`, 
`<jobmanager-host>` and `<flink-user>` are described in the 
[Getting Started](#starting-a-flink-session-on-mesos) guide of this page.

### Session Mode

The [Getting Started](#starting-a-flink-session-on-mesos) guide at the top of this page describes 
deploying Flink in Session Mode.

## Flink on Mesos Reference

### Deploying User Libraries

User libraries can be passed to the Mesos workers by placing them in Flink's `lib/` folder. This way, 
they will be picked by Mesos' Fetcher and copied over into the worker's sandbox folders. Alternatively, 
Docker containerization can be used as described in [Installing Flink on the Workers](#installing-flink-on-the-workers).

### Installing Flink on the Workers

Flink on Mesos offers two ways to distribute the Flink and user binaries within the Mesos cluster:
1. **Using Mesos' Artifact Server**: The Artifact Server provides the resources which are moved by 
   [Mesos' Fetcher](http://mesos.apache.org/documentation/latest/fetcher/) into the Mesos worker's 
   [sandbox folders](http://mesos.apache.org/documentation/latest/sandbox/). It can be explicitly 
   specified by setting [mesos.resourcemanager.tasks.container.type]({{< ref "docs/deployment/config" >}}#mesos-resourcemanager-tasks-container-type) 
   to `mesos`. This is the default option and is used in the example commands of this page.
2. **Using Docker containerization**: This enables the user to provide user libraries and other 
   customizations as part of a Docker image. Docker utilization can be enabled by setting 
   [mesos.resourcemanager.tasks.container.type]({{< ref "docs/deployment/config" >}}#mesos-resourcemanager-tasks-container-type) 
   to `docker` and by providing the image name through [mesos.resourcemanager.tasks.container.image.name]({{< ref "docs/deployment/config" >}}#mesos-resourcemanager-tasks-container-image-name).

### High Availability on Mesos

You will need to run a service like Marathon or Apache Aurora which takes care of restarting the 
JobManager process in case of node or process failures. In addition, Zookeeper needs to be configured 
as described in the [High Availability section of the Flink docs]({{< ref "docs/deployment/ha/overview" >}}).

#### Marathon

Marathon needs to be set up to launch the `bin/mesos-appmaster.sh` script. In particular, it should 
also adjust any configuration parameters for the Flink cluster.

Here is an example configuration for Marathon:
```javascript
{
  "id": "flink",
  "cmd": "/opt/flink-{{ site.version }}/bin/mesos-appmaster.sh -Djobmanager.rpc.address=$HOST -Dmesos.resourcemanager.framework.user=<flink-user> -Dmesos.master=<mesos-master>:5050 -Dparallelism.default=2",
  "user": "<flink-user>",
  "cpus": 2,
  "mem": 2048,
  "instances": 1,
  "env": {
    "MESOS_NATIVE_JAVA_LIBRARY": "/usr/lib/libmesos.so"
  },
  "healthChecks": [
    {
      "protocol": "HTTP",
      "path": "/",
      "port": 8081,
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3
    }
  ]
}
```

Flink is installed into `/opt/flink-{{ site.version }}` having `<flink-user>` as the owner of the Flink 
directory (notice that the user is used twice: once as a Marathon and another time as a Mesos 
parameter) for the example configuration above to work. Additionally, we have the bundled Hadoop jar 
saved in Flink's `lib/` folder for the sake of simplicity here. This way, we don't have to set 
`HADOOP_CLASSPATH` as a environment variable next to `MESOS_NATIVE_JAVA_LIBRARY`.

`<mesos-master>` needs to be set to the hostname or IP of Mesos' master node. `$HOST` is a Marathon 
environment variable referring to the hostname of the machine the script is executed on. `$HOST` should 
not be replaced in the config above!

The whole Flink cluster including the JobManager will be run as Mesos tasks in the Mesos cluster when 
deploying Flink using Marathon. Flink's binaries have to be installed on all Mesos workers for the 
above Marathon config to work.

### Supported Hadoop versions

Flink on Mesos is compiled against Hadoop 2.4.1, and all Hadoop versions >= 2.4.1 are supported, 
including Hadoop 3.x.

For providing Flink with the required Hadoop dependencies, we recommend setting the `HADOOP_CLASSPATH` 
environment variable already introduced in the [Getting Started / Preparation](#preparation) section.

If that is not possible, the dependencies can also be put into the `lib/` folder of Flink. 

Flink also offers pre-bundled Hadoop fat jars for placing them in the `lib/` folder, on the 
[Downloads / Additional Components]({{site.download_url}}#additional-components) section of the website. 
These pre-bundled fat jars are shaded to avoid dependency conflicts with common libraries. The Flink 
community is not testing the Mesos integration against these pre-bundled jars.

### Flink on Mesos Architecture

The Flink on Mesos implementation consists of two components: The application master and the workers. 
The workers are simple TaskManagers parameterized by the environment which is set up through the 
application master. The most sophisticated component of the Flink on Mesos implementation is the 
application master. The application master currently hosts the following components:
- **Mesos Scheduler**: The Scheduler is responsible for registering a framework with Mesos, requesting 
  resources, and launching worker nodes. The Scheduler continuously needs to report back to Mesos to 
  ensure the framework is in a healthy state. To verify the health of the cluster, the Scheduler 
  monitors the spawned workers, marks them as failed and restarts them if necessary.

  Flink's Mesos Scheduler itself is currently not highly available. However, it persists all necessary 
  information about its state (e.g. configuration, list of workers) in [ZooKeeper](#high-availability-on-mesos). 
  In the presence of a failure, it relies on an external system to bring up a new Scheduler (see the 
  [Marathon subsection](#marathon) for further details). The Scheduler will then register with Mesos 
  again and go through the reconciliation phase. In the reconciliation phase, the Scheduler receives 
  a list of running workers nodes. It matches these against the recovered information from ZooKeeper 
  and makes sure to bring back the cluster in the state before the failure.
- **Artifact Server**: The Artifact Server is responsible for providing resources to the worker nodes. 
  The resources can be anything from the Flink binaries to shared secrets or configuration files. 
  For instance, in non-containerized environments, the Artifact Server will provide the Flink binaries. 
  What files will be served depends on the configuration overlay used.

Flink's Mesos startup scripts `bin/mesos-appmaster.sh` and `bin/mesos-appmaster-job.sh` provide a way 
to configure and start the application master. The worker nodes inherit all further configuration. 
They are deployed through `bin/mesos-taskmanager.sh`. The configuration inheritance is achieved using 
configuration overlays. Configuration overlays provide a way to infer a configuration from environment 
variables and config files which are shipped to the worker nodes.

See [Mesos Architecture](http://mesos.apache.org/documentation/latest/architecture/) for a more details 
on how frameworks are handled by Mesos.

{{< top >}}

## Appendix
The following resource files can be used to set up a local Mesos cluster running the Marathon framework 
and having Flink 1.11.2 installed.
 
### Dockerfile

```yaml
FROM mesosphere/mesos:1.7.1

# install Java 11 and wget
RUN apt update && \
    apt -y install wget && \
    wget -nv https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz && \
    tar xzf openjdk-11.0.2_linux-x64_bin.tar.gz && \
    mv jdk-11* /usr/local/jdk-11.0.2 && \
    update-alternatives --install /usr/bin/java java /usr/local/jdk-11.0.2/bin/java 2048 && \
    update-alternatives --auto java
ENV JAVA_HOME=/usr/local/jdk-11.0.2

WORKDIR /opt

# install Hadoop
RUN wget -nv https://apache.mirror.digionline.de/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz && \
    tar -xf hadoop-2.10.1.tar.gz
ENV HADOOP_CLASSPATH=/opt/hadoop-2.10.1/etc/hadoop:/opt/hadoop-2.10.1/share/hadoop/common/lib/*:/opt/hadoop-2.10.1/share/hadoop/common/*:/opt/hadoop-2.10.1/share/hadoop/hdfs:/opt/hadoop-2.10.1/share/hadoop/hdfs/lib/*:/opt/hadoop-2.10.1/share/hadoop/hdfs/*:/opt/hadoop-2.10.1/share/hadoop/yarn:/opt/hadoop-2.10.1/share/hadoop/yarn/lib/*:/opt/hadoop-2.10.1/share/hadoop/yarn/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar

# install Flink on Mesos
RUN wget -nv https://apache.mirror.digionline.de/flink/flink-1.11.2/flink-1.11.2-bin-scala_2.11.tgz && \
    tar -xf flink-1.11.2-bin-scala_2.11.tgz
ENV MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
```

### Docker Compose

The `docker-compose.yml` provided below is based on the work done by 
[Sean Bennet](https://github.com/sean-bennett112/mesos-docker/blob/master/fig.yml).

Keep in mind that it requires the `Dockerfile` of the previous section to be found in the same 
directory and the file being named `Dockerfile`. It might make sense to scale the worker nodes up to 
have enough workers to run Flink on Mesos next 
to the Marathon framework:
```bash
docker-compose up -d --scale worker=2
```

```yaml
version: "3.8"
services:
  zookeeper:
    build:
      context: .
      dockerfile: Dockerfile
    command: /usr/share/zookeeper/bin/zkServer.sh start-foreground
    container_name: zookeeper
  master:
    build:
      context: .
      dockerfile: Dockerfile
    command: mesos-master --registry=in_memory
    container_name: master
    environment:
      - MESOS_ZK=zk://zookeeper:2181/mesos
      - MESOS_LOG_DIR=/var/log/mesos
      - MESOS_QUORUM=1
      - MESOS_WORK_DIR=/var/lib/mesos
    depends_on:
      - zookeeper
    ports:
      - "5050:5050"
      - "8081:8081"
  worker:
    build:
      context: .
      dockerfile: Dockerfile
    command: mesos-slave --launcher=posix
    environment:
      - MESOS_MASTER=zk://zookeeper:2181/mesos
      - MESOS_WORK_DIR=/var/lib/mesos
      - MESOS_LOG_DIR=/var/log/mesos
      - MESOS_LOGGING_LEVEL=INFO
      - MESOS_SYSTEMD_ENABLE_SUPPORT=false
    depends_on:
      - zookeeper
      - master
    ports:
      - "8081"
  marathon:
    image: mesosphere/marathon:v1.11.24
    container_name: marathon
    environment:
      - MARATHON_MASTER=zk://zookeeper:2181/mesos
      - MARATHON_ZK=zk://zookeeper:2181/marathon
      - MARATHON_ZK_CONNECTION_TIMEOUT=60000
    ports:
      - "8080:8080"
    depends_on:
      - zookeeper
      - master
```

### Marathon configuration

The following Marathon configuration can be applied through the Marathon UI: http://localhost:8080/
It will start a Flink on Mesos cluster on any of the worker machines. Flink's default port `8081` is 
forwarded to random ports due to the scaling of the worker nodes. Use `docker ps` to figure out the 
host system's ports that to be able to access Flink's web interface. 

```javascript
{
  "id": "flink",
  "cmd": "/opt/flink-1.11.2/bin/mesos-appmaster.sh -Dmesos.resourcemanager.framework.user=root -Dmesos.master=master:5050 -Djobmanager.rpc.address=$HOST -Dparallelism.default=2",
  "cpus": 2,
  "mem": 4096,
  "disk": 0,
  "instances": 1,
  "env": {
    "HADOOP_CLASSPATH": "/opt/hadoop-2.10.1/etc/hadoop:/opt/hadoop-2.10.1/share/hadoop/common/lib/*:/opt/hadoop-2.10.1/share/hadoop/common/*:/opt/hadoop-2.10.1/share/hadoop/hdfs:/opt/hadoop-2.10.1/share/hadoop/hdfs/lib/*:/opt/hadoop-2.10.1/share/hadoop/hdfs/*:/opt/hadoop-2.10.1/share/hadoop/yarn:/opt/hadoop-2.10.1/share/hadoop/yarn/lib/*:/opt/hadoop-2.10.1/share/hadoop/yarn/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/*:/opt/hadoop-2.10.1/etc/hadoop:/opt/hadoop-2.10.1/share/hadoop/common/lib/*:/opt/hadoop-2.10.1/share/hadoop/common/*:/opt/hadoop-2.10.1/share/hadoop/hdfs:/opt/hadoop-2.10.1/share/hadoop/hdfs/lib/*:/opt/hadoop-2.10.1/share/hadoop/hdfs/*:/opt/hadoop-2.10.1/share/hadoop/yarn:/opt/hadoop-2.10.1/share/hadoop/yarn/lib/*:/opt/hadoop-2.10.1/share/hadoop/yarn/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/contrib/capacity-scheduler/*.jar"
  },
  "healthChecks": [
    {
      "protocol": "HTTP",
      "path": "/",
      "port": 8081,
      "gracePeriodSeconds": 300,
      "intervalSeconds": 60,
      "timeoutSeconds": 20,
      "maxConsecutiveFailures": 3
    }
  ],
  "user": "root"
}
```

{{< top >}}
