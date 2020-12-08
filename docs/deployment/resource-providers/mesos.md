---
title:  "Flink on Mesos"
nav-title: Mesos
nav-parent_id: resource_providers
nav-pos: 5
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

* This will be replaced by the TOC
{:toc}

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

{% highlight bash %}
export HADOOP_CLASSPATH=$(hadoop classpath)
export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so
{% endhighlight %}

`MESOS_NATIVE_JAVA_LIBRARY` needs to point to Mesos' native Java library. The library name `libmesos.so` 
used above refers to Mesos' Linux library. Running Mesos on MacOS would require you to use 
`libmesos.dylib` instead.

### Starting a Flink Session on Mesos

Connect to the machine which matches all the requirements listed in the [Preparation section](#preparation).
Change into Flink's home directory and call `bin/mesos-appmaster.sh`:

{% highlight bash %}
# (0) set required environment variables
export HADOOP_CLASSPATH=$(hadoop classpath)
export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so

# (1) create Flink on Mesos cluster
./bin/mesos-appmaster.sh \
    -Dmesos.master=$MESOS_MASTER:5050 \
    -Djobmanager.rpc.address=$JOBMANAGER_HOST \
    -Dmesos.resourcemanager.framework.user=$FLINK_USER \
    -Dmesos.resourcemanager.tasks.cpus=6
{% endhighlight %}

The call above uses two variables not introduced, yet, as they depend on the cluster:
* `MESOS_MASTER` refers to the Mesos master's IP address or hostname. 
* `JOBMANAGER_HOST` refers to the host that executes `bin/mesos-appmaster.sh` which is starting 
  Flink's JobManager process. It's important to not use `localhost` or `127.0.0.1` as this parameter 
  is being shared with the Mesos cluster and the TaskManagers.
* `FLINK_USER` refers to the user that owns the Mesos master's Flink installation directory (see Mesos' 
  documentation on [specifying a user](http://mesos.apache.org/documentation/latest/fetcher/#specifying-a-user-name)
  for further details).

The Flink on Mesos cluster is now deployed in [Session Mode]({% link deployment/index.md %}#session-mode).
Note that you can run multiple Flink jobs on a Session cluster. Each job needs to be submitted to the 
cluster. TaskManagers are deployed on the Mesos workers as needed. Keep in mind that you can only run as 
many jobs as the Mesos cluster allows in terms of resources provided by the Mesos workers. Play around 
with Flink's parameters to find the right resource utilization for your needs.

Check out [Flink's Mesos configuration]({% link deployment/config.md %}#mesos) to further influence 
the resources Flink on Mesos is going to allocate.

## Deployment Modes Supported by Flink on Mesos

For production use, we recommend deploying Flink Applications in the 
[Per-Job Mode]({% link deployment/index.md %}#per-job-mode), as it provides a better isolation 
for each job.

### Application Mode

Flink on Mesos does not support [Application Mode]({% link deployment/index.md %}#application-mode).

### Per-Job Cluster Mode

A job which is executed in [Per-Job Cluster Mode]({% link deployment/index.md %}#per-job-mode) spins 
up a dedicated Flink cluster that is only used for that specific job. No extra job submission is 
needed. `bin/mesos-appmaster-job.sh` is used as the startup script. It will start a Flink cluster 
for a dedicated job which is passed as a JobGraph file. This file can be created by applying the 
following code to your Job source code:
{% highlight java %}
final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
final String jobGraphFilename = "job.graph";
File jobGraphFile = new File(jobGraphFilename);
try (FileOutputStream output = new FileOutputStream(jobGraphFile);
    ObjectOutputStream obOutput = new ObjectOutputStream(output)){
    obOutput.writeObject(jobGraph);
}
{% endhighlight %}

Flink on Mesos Per-Job cluster can be started in the following way:
{% highlight bash %}
# (0) set required environment variables
export HADOOP_CLASSPATH=$(hadoop classpath)
export MESOS_NATIVE_JAVA_LIBRARY=/path/to/lib/libmesos.so

# (1) create Per-Job Flink on Mesos cluster
./bin/mesos-appmaster-job.sh \
    -Dmesos.master=$MESOS_MASTER:5050 \
    -Djobmanager.rpc.address=$MESOS_MASTER \
    -Dmesos.resourcemanager.framework.user=$FLINK_USER \
    -Dinternal.jobgraph-path=$JOB_GRAPH_FILE
{% endhighlight %} 

`JOB_GRAPH_FILE` in the command above refers to the path of the uploaded JobGraph file defining the 
job that shall be executed on the Per-Job Flink cluster. The meaning of `MESOS_MASTER` and `FLINK_USER` 
are described in the [Getting Started](#starting-a-flink-session-on-mesos) guide of this page.

### Session Mode

The [Getting Started](#starting-a-flink-session-on-mesos) guide at the top of this page describes 
deploying Flink in Session Mode.

## Flink on Mesos Reference

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

### Deploying User Libraries

User libraries can be passed to the Mesos workers by placing them in Flink's `lib/` folder. This way, 
they will be picked by Mesos' Fetcher and copied over into the worker's sandbox folders. Alternatively, 
Docker containerization can be used as described in [Installing Flink on the Workers](#installing-flink-on-the-workers).

### Installing Flink on the Workers

Flink on Mesos offers two ways to distribute the Flink and user binaries within the Mesos cluster:
1. **Using Mesos' Artifact Server**: The Artifact Server provides the resources which are moved by 
   [Mesos' Fetcher](http://mesos.apache.org/documentation/latest/fetcher/) into the Mesos worker's 
   [sandbox folders](http://mesos.apache.org/documentation/latest/sandbox/). It can be explicitly 
   specified by setting [mesos.resourcemanager.tasks.container.type]({% link deployment/config.md %}#mesos-resourcemanager-tasks-container-type) 
   to `mesos`. This is the default option and is used in the example commands of this page.
2. **Using Docker containerization**: This enables the user to provide user libraries and other 
   customizations as part of a Docker image. Docker utilization can be enabled by setting 
   [mesos.resourcemanager.tasks.container.type]({% link deployment/config.md %}#mesos-resourcemanager-tasks-container-type) 
   to `docker` and by providing the image name through [mesos.resourcemanager.tasks.container.image.name]({% link deployment/config.md %}#mesos-resourcemanager-tasks-container-image-name).

### High Availability on Mesos

You will need to run a service like Marathon or Apache Aurora which takes care of restarting the 
JobManager process in case of node or process failures. In addition, Zookeeper needs to be configured 
as described in the [High Availability section of the Flink docs]({% link deployment/ha/index.md %}).

#### Marathon

Marathon needs to be set up to launch the `bin/mesos-appmaster.sh` script. In particular, it should 
also adjust any configuration parameters for the Flink cluster.

Here is an example configuration for Marathon:
{% highlight javascript %}
{
    "id": "flink",
    "cmd": "/opt/flink-{{ site.version }}/bin/mesos-appmaster.sh -Dmesos.resourcemanager.framework.user=root -Dmesos.resourcemanager.tasks.taskmanager-cmd=/opt/flink-{{ site.version }}/bin/mesos-taskmanager.sh -Dmesos.master=master:5050 -Djobmanager.memory.process.size=1472m -Dtaskmanager.memory.process.size=3500m -Dtaskmanager.numberOfTaskSlots=2 -Dparallelism.default=2",
    "cpus": 2,
    "mem": 1024,
    "disk": 0,
    "instances": 1,
    "env": {
        "MESOS_NATIVE_JAVA_LIBRARY": "/usr/lib/libmesos.so",
        "HADOOP_CLASSPATH": "/opt/hadoop-2.10.1/etc/hadoop:/opt/hadoop-2.10.1/share/hadoop/common/lib/*:/opt/hadoop-2.10.1/share/hadoop/common/*:/opt/hadoop-2.10.1/share/hadoop/hdfs:/opt/hadoop-2.10.1/share/hadoop/hdfs/lib/*:/opt/hadoop-2.10.1/share/hadoop/hdfs/*:/opt/hadoop-2.10.1/share/hadoop/yarn:/opt/hadoop-2.10.1/share/hadoop/yarn/lib/*:/opt/hadoop-2.10.1/share/hadoop/yarn/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.10.1/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar"
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
{% endhighlight %}

Flink is installed into `/opt/flink-{{ site.version }}` for this example having `root` as the owner of the Flink 
directory.

When running Flink with Marathon, the whole Flink cluster including the JobManager will be run as 
Mesos tasks in the Mesos cluster. Flink's binaries have to be installed on all Mesos workers for the 
above Marathon config to work.

{% top %}
