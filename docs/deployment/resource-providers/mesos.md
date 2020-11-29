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

The Mesos implementation consists of two components: The Application Master and
the Worker. The workers are simple task managers parameterized by the environment
which is set up through the application master. The most sophisticated component of the Mesos
implementation is the application master. The application master currently hosts
the following components:
- **Mesos Scheduler**: The scheduler is responsible for registering a framework with Mesos,
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
- **Artifact Server**: The artifact server is responsible for providing resources to the worker
nodes. The resources can be anything from the Flink binaries to shared secrets
or configuration files. For instance, in non-containerized environments, the
artifact server will provide the Flink binaries. What files will be served
depends on the configuration overlay used.

Flink's Mesos startup script provides a way to configure and start the application
master. The worker nodes inherit all further configuration. This is achieved using 
configuration overlays. Configuration overlays provide a way to infer a configuration 
from environment variables and config files which are shipped to the worker nodes.

### Preparation

There are two ways to install Flink on Mesos both being described in the following 
subsections.

#### Installing Flink via DC/OS

This section refers to [DC/OS](https://dcos.io) which is a Mesos distribution
with a sophisticated application management layer. It comes pre-installed with
Marathon, a service to supervise applications and maintain their state in case
of failures.

If you don't have a running DC/OS cluster, please follow the
[instructions on how to install DC/OS on the official website](https://dcos.io/install/).

Once you have a DC/OS cluster, you may install Flink through the DC/OS Universe. Just 
search for Flink in the search prompt. Alternatively, you can use the DC/OS CLI:

{% highlight bash %}
dcos package install flink
{% endhighlight %}

Further information can be found in the
[DC/OS examples documentation](https://github.com/dcos/examples/tree/master/flink).

#### Mesos without DC/OS

You can also run Mesos without DC/OS. Please follow the 
[instructions on how to setup Mesos on the official website](http://mesos.apache.org/getting-started/).

After installing Mesos into `${MESOS_HOME}`, you have to configure the set of master and 
agent nodes by creating the files `${MESOS_HOME}/etc/mesos/masters` and 
`${MESOS_HOME}/etc/mesos/slaves`. These files contain a single hostname per row pointing to the 
hosts executing the respective components (assuming SSH access being available for these nodes).  

Next, you have to create `${MESOS_HOME}/etc/mesos/mesos-master-env.sh` (a template file is 
provided in the same directory). In `mesos-master-env.sh`, you have to set `${MESOS_work_dir}` 
pointing to a directory used by Mesos to store data. Additionally, it is recommended to specify 
`${MESOS_log_dir}`:

{% highlight bash %}
export MESOS_work_dir=/path/to/mesos/working/directory
export MESOS_log_dir=/path/to/mesos/logging/directory
{% endhighlight %}

The Mesos agents are configured by creating `${MESOS_HOME}/etc/mesos/mesos-agent-env.sh` (a 
template file is provided in the same directory). `${MESOS_master}` needs to be configured as part 
of `mesos-agent-env.sh`. Additionally, `${MESOS_work_dir}` and `${MESOS_log_dir}` should be set to 
the corresponding folders:

{% highlight bash %}
export MESOS_master=MASTER_HOSTNAME:MASTER_PORT
export MESOS_work_dir=/path/to/mesos/working/directory
export MESOS_log_dir=/path/to/mesos/logging/directory
{% endhighlight %}

In order to start your Mesos cluster, use the deployment script `${MESOS_HOME}/sbin/mesos-start-cluster.sh`.
Stopping the Mesos cluster can be achieved through `${MESOS_HOME}/sbin/mesos-stop-cluster.sh`.

More information about the deployment scripts can be found [here](http://mesos.apache.org/documentation/latest/deploy-scripts/).

#### Pre-installing Flink vs Docker/Mesos containers

You may install Flink on all of your Mesos Master and Agent nodes. You can also pull the binaries 
from the Flink website during deployment and apply your custom configuration before launching the 
application master. A more convenient and easier-to-maintain approach is to use Docker containers 
to manage the Flink binaries and configuration.

This is controlled via the Flink configuration entries (`mesos` being the default):

    mesos.resourcemanager.tasks.container.type: mesos _or_ docker

If the type is set to 'docker', specify the image name:

    mesos.resourcemanager.tasks.container.image.name: image_name

### Starting a Flink Session on Mesos

A Flink session cluster is a long-running Mesos deployment. Note that you can run multiple Flink 
jobs on a session cluster. Each job needs to be submitted to the cluster after the cluster has been 
deployed.

You find two startup scripts which manage the Flink processes in a Mesos cluster under 
`${FLINK_HOME}/bin/`:

1. `mesos-appmaster.sh`
   This script starts the Mesos application master which will register the Mesos scheduler.
   It is also responsible for starting up the worker nodes.

2. `mesos-taskmanager.sh`
   This script is the entrypoint for the Mesos worker processes. You don't need to explicitly 
   execute this script. It is automatically launched by the Mesos worker node to bring up a 
   new TaskManager.

For Flink on Mesos to work properly, you have to export `${MESOS_NATIVE_JAVA_LIBRARY}` pointing to 
Mesos' native Java library. The location of this library differs depending on the underlying system:
- Linux: `export MESOS_NATIVE_JAVA_LIBRARY=${MESOS_HOME}/lib/libmesos.so`
- MacOS: `export MESOS_NATIVE_JAVA_LIBRARY=${MESOS_HOME}/lib/libmesos.dylib`

In order to run the `mesos-appmaster.sh` script you have to define `mesos.master` in the 
`flink-conf.yaml` or pass it via `-Dmesos.master=...` to the Java process. It is possible to 
completely parameterize a Mesos application through Java properties passed to the Mesos application 
master:
{% highlight bash %}
${FLINK_HOME}/bin/mesos-appmaster.sh \
    -Dmesos.master=localhost:5050 \
    -Dparallelism.default=10 \
    -Dtaskmanager.numberOfTaskSlots=2
{% endhighlight %}
This will create a JobManager on the machine that is used for executing the script.
In contrast to that, the TaskManagers will run as Mesos tasks in the Mesos cluster.

## Deployment Modes Supported by Flink on Mesos

For production use, we recommend deploying Flink Applications in the 
[Per-Job]({% link deployment/index.md %}#per-job-mode), as this mode provides a better isolation 
for each job.

### Application Mode

Flink on Mesos does not support Application Mode.

### Per-Job Cluster Mode

A job which is executed in Per-Job Cluster Mode spins up a dedicated Flink cluster that only being 
used for that specific job. No extra job submission is needed.

`${FLINK_HOME}/bin/mesos-appmaster-job.sh` is used as the startup script. It starts the Mesos 
application master which will register the Mesos scheduler, retrieve the JobGraph and launch the 
TaskManagers accordingly.

In order to run the `mesos-appmaster-job.sh` script you have to define `mesos.master` and 
`internal.jobgraph-path` in the `flink-conf.yaml` or pass it via `-Dmesos.master=... -Dinterval.jobgraph-path=...` 
to the Java process.

The JobGraph file can be generated in the following way:
{% highlight java %}
final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
final String jobGraphFilename = "job.graph";
File jobGraphFile = new File(jobGraphFilename);
try (FileOutputStream output = new FileOutputStream(jobGraphFile);
	ObjectOutputStream obOutput = new ObjectOutputStream(output)){
	obOutput.writeObject(jobGraph);
}
{% endhighlight %}

<span class="label label-info">Note</span> Make sure that all the classpath's of all Mesos 
processes contain the reference to the user code jar. There are two ways to achieve this:
1. One way is to put them in the `${FLINK_HOME}/lib/` directory, which will result in the user 
   code jar being loaded by the system classloader.
1. Another option is to create a `${FLINK_HOME}/usrlib/` directory and to put the user code jar 
   in that directory. The user code jar will be loaded by the user code classloader after launching 
   a Per-Job Cluster via `${FLINK_HOME}/bin/mesos-appmaster-job.sh ...`.

### Session Mode

The [Getting Started](#starting-a-flink-session-on-mesos) guide at the top of this page describes 
deploying Flink in Session Mode.

## Flink on Mesos Reference

### Configuring Flink on Mesos

For a list of Mesos-specific configuration parameters, refer to the 
[Mesos section]({% link deployment/config.md %}#mesos) of the configuration documentation.

### High Availability on Mesos

You will need to run a service like Marathon or Apache Aurora which takes care of restarting the 
JobManager process in case of node or process failures. In addition, Zookeeper needs to be 
configured as described in the 
[High Availability section of the Flink docs]({% link deployment/ha/index.md %}).

#### Marathon

Marathon needs to be set up to launch the `bin/mesos-appmaster.sh` script. In particular, it should 
also adjust any configuration parameters for the Flink cluster.

Here is an example configuration for Marathon:

{% highlight javascript %}
    {
        "id": "flink",
        "cmd": "$FLINK_HOME/bin/mesos-appmaster.sh -Djobmanager.memory.process.size=1472m -Djobmanager.rpc.port=6123 -Drest.port=8081 -Dtaskmanager.memory.process.size=1024m -Dtaskmanager.numberOfTaskSlots=2 -Dparallelism.default=2 -Dmesos.resourcemanager.tasks.cpus=1",
        "cpus": 1.0,
        "mem": 1024
    }
{% endhighlight %}

When running Flink with Marathon, the whole Flink cluster including the job manager will be run as 
Mesos tasks in the Mesos cluster.

{% top %}
