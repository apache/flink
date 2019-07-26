---
title:  "Mesos 安装"
nav-title: Mesos
nav-parent_id: deployment
nav-pos: 3
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

## Background

The Mesos implementation consists of two components: The Application Master and
the Worker. The workers are simple TaskManagers which are parameterized by the environment
set up by the application master. The most sophisticated component of the Mesos
implementation is the application master. The application master currently hosts
the following components:

### Mesos Scheduler

The scheduler is responsible for registering the framework with Mesos,
requesting resources, and launching worker nodes. The scheduler continuously
needs to report back to Mesos to ensure the framework is in a healthy state. To
verify the health of the cluster, the scheduler monitors the spawned workers and
marks them as failed and restarts them if necessary.

Flink's Mesos scheduler itself is currently not highly available. However, it
persists all necessary information about its state (e.g. configuration, list of
workers) in Zookeeper. In the presence of a failure, it relies on an external
system to bring up a new scheduler. The scheduler will then register with Mesos
again and go through the reconciliation phase. In the reconciliation phase, the
scheduler receives a list of running workers nodes. It matches these against the
recovered information from Zookeeper and makes sure to bring back the cluster in
the state before the failure.

### Artifact Server

The artifact server is responsible for providing resources to the worker
nodes. The resources can be anything from the Flink binaries to shared secrets
or configuration files. For instance, in non-containerized environments, the
artifact server will provide the Flink binaries. What files will be served
depends on the configuration overlay used.

### Flink's Dispatcher and Web Interface

The Dispatcher and the web interface provide a central point for monitoring,
job submission, and other client interaction with the cluster
(see [FLIP-6](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077)).

### Startup script and configuration overlays

The startup script provide a way to configure and start the application
master. All further configuration is then inherited by the workers nodes. This
is achieved using configuration overlays. Configuration overlays provide a way
to infer configuration from environment variables and config files which are
shipped to the worker nodes.


## DC/OS

This section refers to [DC/OS](https://dcos.io) which is a Mesos distribution
with a sophisticated application management layer. It comes pre-installed with
Marathon, a service to supervise applications and maintain their state in case
of failures.

If you don't have a running DC/OS cluster, please follow the
[instructions on how to install DC/OS on the official website](https://dcos.io/install/).

Once you have a DC/OS cluster, you may install Flink through the DC/OS
Universe. In the search prompt, just search for Flink. Alternatively, you can use the DC/OS CLI:

    dcos package install flink

Further information can be found in the
[DC/OS examples documentation](https://github.com/dcos/examples/tree/master/1.8/flink).


## Mesos without DC/OS

You can also run Mesos without DC/OS.

### Installing Mesos

Please follow the [instructions on how to setup Mesos on the official website](http://mesos.apache.org/getting-started/).

After installation you have to configure the set of master and agent nodes by creating the files `MESOS_HOME/etc/mesos/masters` and `MESOS_HOME/etc/mesos/slaves`.
These files contain in each row a single hostname on which the respective component will be started (assuming SSH access to these nodes).

Next you have to create `MESOS_HOME/etc/mesos/mesos-master-env.sh` or use the template found in the same directory.
In this file, you have to define

    export MESOS_work_dir=WORK_DIRECTORY

and it is recommended to uncommment

    export MESOS_log_dir=LOGGING_DIRECTORY


In order to configure the Mesos agents, you have to create `MESOS_HOME/etc/mesos/mesos-agent-env.sh` or use the template found in the same directory.
You have to configure

    export MESOS_master=MASTER_HOSTNAME:MASTER_PORT

and uncomment

    export MESOS_log_dir=LOGGING_DIRECTORY
    export MESOS_work_dir=WORK_DIRECTORY

#### Mesos Library

In order to run Java applications with Mesos you have to export `MESOS_NATIVE_JAVA_LIBRARY=MESOS_HOME/lib/libmesos.so` on Linux.
Under Mac OS X you have to export `MESOS_NATIVE_JAVA_LIBRARY=MESOS_HOME/lib/libmesos.dylib`.

#### Deploying Mesos

In order to start your mesos cluster, use the deployment script `MESOS_HOME/sbin/mesos-start-cluster.sh`.
In order to stop your mesos cluster, use the deployment script `MESOS_HOME/sbin/mesos-stop-cluster.sh`.
More information about the deployment scripts can be found [here](http://mesos.apache.org/documentation/latest/deploy-scripts/).

### Installing Marathon

Optionally, you may also [install Marathon](https://mesosphere.github.io/marathon/docs/) which enables you to run Flink in [high availability (HA) mode](#high-availability).

### Pre-installing Flink vs Docker/Mesos containers

You may install Flink on all of your Mesos Master and Agent nodes.
You can also pull the binaries from the Flink web site during deployment and apply your custom configuration before launching the application master.
A more convenient and easier to maintain approach is to use Docker containers to manage the Flink binaries and configuration.

This is controlled via the following configuration entries:

    mesos.resourcemanager.tasks.container.type: mesos _or_ docker

If set to 'docker', specify the image name:

    mesos.resourcemanager.tasks.container.image.name: image_name


### Flink session cluster on Mesos

A Flink session cluster is executed as a long-running Mesos Deployment. Note that you can run multiple Flink jobs on a session cluster. Each job needs to be submitted to the cluster after the cluster has been deployed.

In the `/bin` directory of the Flink distribution, you find two startup scripts
which manage the Flink processes in a Mesos cluster:

1. `mesos-appmaster.sh`
   This starts the Mesos application master which will register the Mesos scheduler.
   It is also responsible for starting up the worker nodes.

2. `mesos-taskmanager.sh`
   The entry point for the Mesos worker processes.
   You don't need to explicitly execute this script.
   It is automatically launched by the Mesos worker node to bring up a new TaskManager.

In order to run the `mesos-appmaster.sh` script you have to define `mesos.master` in the `flink-conf.yaml` or pass it via `-Dmesos.master=...` to the Java process.

When executing `mesos-appmaster.sh`, it will create a job manager on the machine where you executed the script.
In contrast to that, the task managers will be run as Mesos tasks in the Mesos cluster.

### Flink job cluster on Mesos

A Flink job cluster is a dedicated cluster which runs a single job.
There is no extra job submission needed.

In the `/bin` directory of the Flink distribution, you find one startup script
which manage the Flink processes in a Mesos cluster:

1. `mesos-appmaster-job.sh`
   This starts the Mesos application master which will register the Mesos scheduler, retrieve the job graph and then launch the task managers accordingly.

In order to run the `mesos-appmaster-job.sh` script you have to define `mesos.master` and `internal.jobgraph-path` in the `flink-conf.yaml`
or pass it via `-Dmesos.master=... -Dinterval.jobgraph-path=...` to the Java process.

The job graph file may be generated like this way:

{% highlight java %}
final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
jobGraph.setAllowQueuedScheduling(true);
final String jobGraphFilename = "job.graph";
File jobGraphFile = new File(jobGraphFilename);
try (FileOutputStream output = new FileOutputStream(jobGraphFile);
	ObjectOutputStream obOutput = new ObjectOutputStream(output)){
	obOutput.writeObject(jobGraph);
}
{% endhighlight %}

Note:
1. Before serializing the job graph, please make sure to enable queued scheduling because slots need to be allocated lazily
2. Make sure that all Mesos processes have the user code jar on the classpath (e.g. putting them in the lib directory)

#### General configuration

It is possible to completely parameterize a Mesos application through Java properties passed to the Mesos application master.
This also allows to specify general Flink configuration parameters.
For example:

    bin/mesos-appmaster.sh \
        -Dmesos.master=master.foobar.org:5050 \
        -Djobmanager.heap.size=1024m \
        -Djobmanager.rpc.port=6123 \
        -Drest.port=8081 \
        -Dmesos.resourcemanager.tasks.mem=4096 \
        -Dtaskmanager.heap.size=3500m \
        -Dtaskmanager.numberOfTaskSlots=2 \
        -Dparallelism.default=10

### High Availability

You will need to run a service like Marathon or Apache Aurora which takes care of restarting the Flink master process in case of node or process failures.
In addition, Zookeeper needs to be configured like described in the [High Availability section of the Flink docs]({{ site.baseurl }}/ops/jobmanager_high_availability.html).

#### Marathon

Marathon needs to be set up to launch the `bin/mesos-appmaster.sh` script.
In particular, it should also adjust any configuration parameters for the Flink cluster.

Here is an example configuration for Marathon:

    {
        "id": "flink",
        "cmd": "$FLINK_HOME/bin/mesos-appmaster.sh -Djobmanager.heap.size=1024m -Djobmanager.rpc.port=6123 -Drest.port=8081 -Dmesos.resourcemanager.tasks.mem=1024 -Dtaskmanager.heap.mb=1024 -Dtaskmanager.numberOfTaskSlots=2 -Dparallelism.default=2 -Dmesos.resourcemanager.tasks.cpus=1",
        "cpus": 1.0,
        "mem": 1024
    }

When running Flink with Marathon, the whole Flink cluster including the job manager will be run as Mesos tasks in the Mesos cluster.

### Configuration parameters

For a list of Mesos specific configuration, refer to the [Mesos section]({{ site.baseurl }}/ops/config.html#mesos)
of the configuration documentation.

{% top %}
