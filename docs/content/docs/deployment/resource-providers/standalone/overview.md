---
title: Overview
weight: 2
type: docs
aliases:
  - /deployment/resource-providers/standalone/
  - /ops/deployment/cluster_setup.html
  - /apis/local_execution.html
  - /getting-started/tutorials/local_setup.html
  - /quickstart/setup_quickstart.html
  - /tutorials/flink_on_windows.html
  - /tutorials/local_setup.html
  - /getting-started/tutorials/flink_on_windows.html
  - /start/flink_on_windows.html
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

# Standalone

## Getting Started

This *Getting Started* section guides you through the local setup (on one machine, but in separate processes) of a Flink cluster. This can easily be expanded to set up a distributed standalone cluster, which we describe in the [reference section](#example-2-start-a-distributed-cluster-jobmanagers).

### Introduction

The standalone mode is the most barebone way of deploying Flink: The Flink services described in the [deployment overview]({{< ref "docs/deployment/overview" >}}) are just launched as processes on the operating system. Unlike deploying Flink with a resource provider such as [Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}) or [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}}), you have to take care of restarting failed processes, or allocation and de-allocation of resources during operation.

In the additional subpages of the standalone mode resource provider, we describe additional deployment methods which are based on the standalone mode: [Deployment in Docker containers]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}), and on [Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}).

### Preparation

Flink runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**, and **Cygwin** (for Windows). Before you start to setup the system, make sure your system fulfils the following requirements.

- **Java 1.8.x** or higher installed,
- Downloaded a recent Flink distribution from the [download page]({{< downloads >}}) and unpacked it.

### Starting a Standalone Cluster (Session Mode)

These steps show how to launch a Flink standalone cluster, and submit an example job:

```bash
# we assume to be in the root directory of the unzipped Flink distribution

# (1) Start Cluster
$ ./bin/start-cluster.sh

# (2) You can now access the Flink Web Interface on http://localhost:8081

# (3) Submit example job
$ ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar

# (4) Stop the cluster again
$ ./bin/stop-cluster.sh
```

In step `(1)`, we've started 2 processes: A JVM for the JobManager, and a JVM for the TaskManager. The JobManager is serving the web interface accessible at [localhost:8081](http://localhost:8081).
In step `(3)`, we are starting a Flink Client (a short-lived JVM process) that submits an application to the JobManager.

## Deployment Modes

### Application Mode

{{< hint info >}}
For high-level intuition behind the application mode, please refer to the [deployment mode overview]({{< ref "docs/deployment/overview#application-mode" >}}).
{{< /hint >}}

To start a Flink JobManager with an embedded application, we use the `bin/standalone-job.sh` script. 
We demonstrate this mode by locally starting the `TopSpeedWindowing.jar` example, running on a single TaskManager.

The application jar file needs to be available in the classpath. The easiest approach to achieve that is putting the jar into the `lib/` folder:

```bash
$ cp ./examples/streaming/TopSpeedWindowing.jar lib/
```

Then, we can launch the JobManager:

```bash
$ ./bin/standalone-job.sh start --job-classname org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
```

The web interface is now available at [localhost:8081](http://localhost:8081). However, the application won't be able to start, because there are no TaskManagers running yet:

```bash
$ ./bin/taskmanager.sh start
```

Note: You can start multiple TaskManagers, if your application needs more resources.

Stopping the services is also supported via the scripts. Call them multiple times if you want to stop multiple instances, or use `stop-all`:

```bash
$ ./bin/taskmanager.sh stop
$ ./bin/standalone-job.sh stop
```


### Per-Job Mode

{{< hint info >}}
For high-level intuition behind the per-job mode, please refer to the [deployment mode overview]({{< ref "docs/deployment/overview#per-job-mode" >}}).
{{< /hint >}}

Per-Job Mode is not supported by the Standalone Cluster.

### Session Mode

{{< hint info >}}
For high-level intuition behind the session mode, please refer to the [deployment mode overview]({{< ref "docs/deployment/overview#session-mode" >}}).
{{< /hint >}}

Local deployment in Session Mode has already been described in the [introduction](#starting-a-standalone-cluster-session-mode) above.

## Standalone Cluster Reference

### Configuration

All available configuration options are listed on the [configuration page]({{< ref "docs/deployment/config" >}}), in particular the [Basic Setup]({{< ref "docs/deployment/config" >}}#basic-setup) section contains good advise on configuring the ports, memory, parallelism etc.

### Debugging

If Flink is behaving unexpectedly, we recommend looking at Flink's log files as a starting point for further investigations.

The log files are located in the `logs/` directory. There's a `.log` file for each Flink service running on this machine. In the default configuration, log files are rotated on each start of a Flink service -- older runs of a service will have a number suffixed to the log file.

Alternatively, logs are available from the Flink web frontend (both for the JobManager and each TaskManager).

By default, Flink is logging on the "INFO" log level, which provides basic information for all obvious issues. For cases where Flink seems to behave wrongly, reducing the log level to "DEBUG" is advised. The logging level is controlled via the `conf/log4.properties` file.
Setting `rootLogger.level = DEBUG` will bootstrap Flink on the DEBUG log level.

There's a dedicated page on the [logging]({{< ref "docs/deployment/advanced/logging" >}}) in Flink.

### Component Management Scripts

#### Starting and Stopping a cluster

`bin/start-cluster.sh` and `bin/stop-cluster.sh` rely on `conf/masters` and `conf/workers` to determine the number of cluster component instances.

If password-less SSH access to the listed machines is configured, and they share the same directory structure, the scripts also support starting and stopping instances remotely.

##### Example 1: Start a cluster with 2 TaskManagers locally

`conf/masters` contents:
```bash
localhost
```

`conf/workers` contents:
```bash
localhost
localhost
```

##### Example 2: Start a distributed cluster JobManagers

This assumes a cluster with 4 machines (`master1, worker1, worker2, worker3`), which all can reach each other over the network.

`conf/masters` contents:
```bash
master1
```

`conf/workers` contents:
```bash
worker1
worker2
worker3
```

Note that the configuration key [jobmanager.rpc.address]({{< ref "docs/deployment/config" >}}#jobmanager-rpc-address) needs to be set to `master1` for this to work.

We show a third example with a standby JobManager in the [high-availability section](#setting-up-high-availability).

#### Starting and Stopping Flink Components

The `bin/jobmanager.sh` and `bin/taskmanager.sh` scripts support starting the respective daemon in the background (using the `start` argument), or in the foreground (using `start-foreground`). In the foreground mode, the logs are printed to standard out. This mode is useful for deployment scenarios where another process is controlling the Flink daemon (e.g. Docker).

The scripts can be called multiple times, for example if multiple TaskManagers are needed. The instances are tracked by the scripts, and can be stopped one-by-one (using `stop`) or all together (using `stop-all`).

#### Windows Cygwin Users

If you are installing Flink from the git repository and you are using the Windows git shell, Cygwin can produce a failure similar to this one:

```bash
c:/flink/bin/start-cluster.sh: line 30: $'\r': command not found
```

This error occurs because git is automatically transforming UNIX line endings to Windows style line endings when running on Windows. The problem is that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

    ```bash
    cd; pwd
    ```

    This will return a path under the Cygwin root path.

3. Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following (if the file does not exist you will have to create it):

    ```bash
    $ export SHELLOPTS
    $ set -o igncr
    ```

4. Save the file and open a new bash shell.

### Setting up High-Availability

In order to enable HA for a standalone cluster, you have to use the [ZooKeeper HA services]({{< ref "docs/deployment/ha/zookeeper_ha" >}}).

Additionally, you have to configure your cluster to start multiple JobManagers.

In order to start an HA-cluster configure the *masters* file in `conf/masters`:

- **masters file**: The *masters file* contains all hosts, on which JobManagers are started, and the ports to which the web user interface binds.

```bash
master1:webUIPort1
[...]
masterX:webUIPortX
```

By default, the JobManager will pick a *random port* for inter process communication. You can change this via the [high-availability.jobmanager.port]({{< ref "docs/deployment/config" >}}#high-availability-jobmanager-port) key. This key accepts single ports (e.g. `50010`), ranges (`50000-50025`), or a combination of both (`50010,50011,50020-50025,50050-50075`).

#### Example: Standalone HA Cluster with 2 JobManagers

1. Configure high availability mode and ZooKeeper quorum in `conf/flink-conf.yaml`:

```bash
high-availability: zookeeper
high-availability.zookeeper.quorum: localhost:2181
high-availability.zookeeper.path.root: /flink
high-availability.cluster-id: /cluster_one # important: customize per cluster
high-availability.storageDir: hdfs:///flink/recovery
```

2. Configure masters in `conf/masters`:

```bash
localhost:8081
localhost:8082
```

3. Configure ZooKeeper server in `conf/zoo.cfg` (currently it's only possible to run a single ZooKeeper server per machine):

```bash
server.0=localhost:2888:3888
```

4. Start ZooKeeper quorum:

```bash
$ ./bin/start-zookeeper-quorum.sh
Starting zookeeper daemon on host localhost.
```

5. Start an HA-cluster:

```bash
$ ./bin/start-cluster.sh
Starting HA cluster with 2 masters and 1 peers in ZooKeeper quorum.
Starting standalonesession daemon on host localhost.
Starting standalonesession daemon on host localhost.
Starting taskexecutor daemon on host localhost.
```

6. Stop ZooKeeper quorum and cluster:

```bash
$ ./bin/stop-cluster.sh
Stopping taskexecutor daemon (pid: 7647) on localhost.
Stopping standalonesession daemon (pid: 7495) on host localhost.
Stopping standalonesession daemon (pid: 7349) on host localhost.
$ ./bin/stop-zookeeper-quorum.sh
Stopping zookeeper daemon (pid: 7101) on host localhost.
```


{{< top >}}
