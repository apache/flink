---
title:  "Mesos Setup"
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
or configuration files. For instance, in non-containered environments, the
artifact server will provide the Flink binaries. What files will be served
depends on the configuration overlay used. 

### Flink's JobManager and Web Interface

The Mesos scheduler currently resides with the JobManager but will be started
independently of the JobManager in future versions (see
[FLIP-6](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077)). The
proposed changes will also add a Dipsatcher component which will be the central
point for job submission and monitoring.

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
Universe. In the search prompt, just search for Flink. 

**Note**: At the time of this writing, Flink was not yet available in the
Unvierse. Please use the following workaround in the meantime:

1. Add the Development Universe

    `dcos marathon app add https://raw.githubusercontent.com/mesosphere/dcos-flink-service/Makman2/quickstart/universe-server.json`
    
2. Add the local Universe repository:

   `dcos package repo add --index=0 dev-universe http://universe.marathon.mesos:8085/repo`

3. Install Flink through the Universe page or using the `dcos` command:
   
   `dcos package install flink`


## Mesos without DC/OS

Let's take a look at how to setup Flink on Mesos without DC/OS. 

### Prerequisites

Please follow the
[instructions on how to setup Mesos on the official website](http://mesos.apache.org/documentation/latest/). 

### Optional dependencies

Optionally,
you may also install [Marathon](https://mesosphere.github.io/marathon/) which
will be necessary if you want your Flink cluster to be highly available in the
presence of master node failures. Additionally, you probably want to install a
distributed file system to share data across nodes and make use of Flink's
checkpointing mechanism.

### Pre-installing Flink vs Docker/Mesos containers

You may install Flink on all of your Mesos Master and Agent nodes. You can also
pull the binaries from the Flink web site during deployment and apply your
custom configuration before launching the application master. A more
convenient and easier to maintain approach is to use Docker containers to manage
the Flink binaries and configuration.

This is controlled via the following configuration entries:

    mesos.resourcemanager.tasks.container.type: mesos _or_ docker
    
If set to 'docker', specify the image name:

    mesos.resourcemanager.tasks.container.image.name: image_name


### Standalone

In the `/bin` directory of the Flink distribution, you find two startup scripts
which manage the Flink processes in a Mesos cluster:

1. mesos-appmaster.sh
   This starts the Mesos application master which will register the Mesos
   scheduler. It is also responsible for starting up the worker nodes.
   
2. mesos-taskmanager.sh
   The entry point for the Mesos worker processes. You don't need to explicitly
   execute this script. It is automatically launched by the Mesos worker node to
   bring up a new TaskManager.


### High Availability

You will need to run a service like Marathon or Apache Aurora which takes care
of restarting the Flink master process in case of node or process failures. In
addition, Zookeeper needs to be configured like described in the
[High Availability section of the Flink docs]({{ site.baseurl }}/setup/jobmanager_high_availability.html)

For the reconciliation of tasks to work correctly, please also set
`recovery.zookeeper.path.mesos-workers` to a valid Zookeeper path.

#### Marathon

Marathon needs to be set up to launch the `bin/mesos-appmaster.sh` script. In
particular, it should also adjust any configuration parameters for the Flink
cluster. 

Here is an example configuration for Marathon:

    {
    "id": "basic-0", 
    "cmd": "$FLINK_HOME/bin/mesos-appmaster.sh -DconfigEntry=configValue -DanotherEntry=anotherValue ...",
    "cpus": 1.0,
    "mem": 2048,
    }

### Configuration parameters

#### Mesos configuration entries


`mesos.initial-tasks`: The initial workers to bring up when the master
    starts (**DEFAULT**: The number of workers specified at cluster startup).

`mesos.maximum-failed-tasks`: The maximum number of failed workers before
    the cluster fails (**DEFAULT**: Number of initial workers) May be set to -1
    to disable this feature.
    
`mesos.master`: The Mesos master URL. The value should be in one of the
	 following forms: host:port, zk://host1:port1,host2:port2,.../path,
	 zk://username:password@host1:port1,host2:port2,.../path,
	 file:///path/to/file (where file contains one of the above)
     
`mesos.failover-timeout`: The failover timeout in seconds for the Mesos scheduler, after
	 which running tasks are automatically shut down (**DEFAULT:** 600).

`mesos.resourcemanager.artifactserver.port`:The config parameter defining the Mesos artifact server port to use. Setting the port to 0 will let the OS choose an available port.

`mesos.resourcemanager.framework.name`: Mesos framework name (**DEFAULT:** Flink)

`mesos.resourcemanager.framework.role`: Mesos framework role definition (**DEFAULT:** *)

`recovery.zookeeper.path.mesos-workers`: The ZooKeeper root path for persisting the Mesos worker information.

`mesos.resourcemanager.framework.principal`: Mesos framework principal (**NO DEFAULT**)

`mesos.resourcemanager.framework.secret`: Mesos framework secret (**NO DEFAULT**)

`mesos.resourcemanager.framework.user`: Mesos framework user (**DEFAULT:**"")

`mesos.resourcemanager.artifactserver.ssl.enabled`: Enables SSL for the Flink
artifact server (**DEFAULT**: true). Note that `security.ssl.enabled` also needs
to be set to `true` encryption to enable encryption.

`mesos.resourcemanager.tasks.mem`: Memory to assign to the Mesos workers in MB (**DEFAULT**: 1024)

`mesos.resourcemanager.tasks.cpus`: CPUs to assign to the Mesos workers (**DEFAULT**: 0.0)

`mesos.resourcemanager.tasks.container.type`: Type of the containerization used: "mesos" or "docker" (DEFAULT: mesos);

`mesos.resourcemanager.tasks.container.image.name`: Image name to use for the container (**NO DEFAULT**)


#### General configuration

It is possible to completely parameterize a Mesos application through Java
properties passed to the Mesos application master. This also allows to specify
general Flink configuration parameters. For example:

    bin/mesos-appmaster.sh \
        -Djobmanager.heap.mb=1024 \
        -Djobmanager.rpc.port=6123 \
        -Djobmanager.web.port=8081 \
        -Dmesos.initial-tasks=10 \
        -Dmesos.resourcemanager.tasks.mem=4096 \
        -Dtaskmanager.heap.mb=3500 \
        -Dtaskmanager.numberOfTaskSlots=2 \
        -Dparallelism.default=10
