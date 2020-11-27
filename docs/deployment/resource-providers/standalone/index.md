---
title: "Standalone"
nav-id: standalone
nav-parent_id: resource_providers
nav-pos: 1
nav-show_overview: true
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

This page provides instructions on how to run Flink in a *fully distributed fashion* on a *static* (but possibly heterogeneous) cluster.

* This will be replaced by the TOC
{:toc}

## Requirements

### Software Requirements

Flink runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**, and **Cygwin** (for Windows) and expects the cluster to consist of **one master node** and **one or more worker nodes**. Before you start to setup the system, make sure you have the following software installed **on each node**:

- **Java 1.8.x** or higher,
- **ssh** (sshd must be running to use the Flink scripts that manage
  remote components)

If your cluster does not fulfill these software requirements you will need to install/upgrade it.

Having __passwordless SSH__ and
__the same directory structure__ on all your cluster nodes will allow you to use our scripts to control
everything.

{% top %}

### `JAVA_HOME` Configuration

Flink requires the `JAVA_HOME` environment variable to be set on the master and all worker nodes and point to the directory of your Java installation.

You can set this variable in `conf/flink-conf.yaml` via the `env.java.home` key.

{% top %}

## Flink Setup

Go to the [downloads page]({{ site.download_url }}) and get the ready-to-run package.

After downloading the latest release, copy the archive to your master node and extract it:

{% highlight bash %}
tar xzf flink-*.tgz
cd flink-*
{% endhighlight %}

### Configuring Flink

After having extracted the system files, you need to configure Flink for the cluster by editing *conf/flink-conf.yaml*.

Set the `jobmanager.rpc.address` key to point to your master node. You should also define the maximum amount of main memory Flink is allowed to allocate on each node by setting the `jobmanager.memory.process.size` and `taskmanager.memory.process.size` keys.

These values are given in MB. If some worker nodes have more main memory which you want to allocate to the Flink system you can overwrite the default value by setting `taskmanager.memory.process.size` or `taskmanager.memory.flink.size` in *conf/flink-conf.yaml* on those specific nodes.

Finally, you must provide a list of all nodes in your cluster that shall be used as worker nodes, i.e., nodes running a TaskManager. Edit the file *conf/workers* and enter the IP/host name of each worker node.

The following example illustrates the setup with three nodes (with IP addresses from _10.0.0.1_
to _10.0.0.3_ and hostnames _master_, _worker1_, _worker2_) and shows the contents of the
configuration files (which need to be accessible at the same path on all machines):

<div class="row">
  <div class="col-md-6 text-center">
    <img src="{% link /page/img/quickstart_cluster.png %}" style="width: 60%">
  </div>
<div class="col-md-6">
  <div class="row">
    <p class="lead text-center">
      /path/to/<strong>flink/conf/<br>flink-conf.yaml</strong>
    <pre>jobmanager.rpc.address: 10.0.0.1</pre>
    </p>
  </div>
<div class="row" style="margin-top: 1em;">
  <p class="lead text-center">
    /path/to/<strong>flink/<br>conf/workers</strong>
  <pre>
10.0.0.2
10.0.0.3</pre>
  </p>
</div>
</div>
</div>

The Flink directory must be available on every worker under the same path. You can use a shared NFS directory, or copy the entire Flink directory to every worker node.

Please see the [configuration page]({% link deployment/config.md %}) for details and additional configuration options.

In particular,

 * the amount of available memory per JobManager (`jobmanager.memory.process.size`),
 * the amount of available memory per TaskManager (`taskmanager.memory.process.size` and check [memory setup guide]({% link deployment/memory/mem_tuning.md %}#configure-memory-for-standalone-deployment)),
 * the number of available CPUs per machine (`taskmanager.numberOfTaskSlots`),
 * the total number of CPUs in the cluster (`parallelism.default`) and
 * the temporary directories (`io.tmp.dirs`)

are very important configuration values.

{% top %}

### Starting Flink

The following script starts a JobManager on the local node and connects via SSH to all worker nodes listed in the *workers* file to start the TaskManager on each node. Now your Flink system is up and running. The JobManager running on the local node will now accept jobs at the configured RPC port.

Assuming that you are on the master node and inside the Flink directory:

{% highlight bash %}
bin/start-cluster.sh
{% endhighlight %}

To stop Flink, there is also a `stop-cluster.sh` script.

{% top %}

### Adding JobManager/TaskManager Instances to a Cluster

You can add both JobManager and TaskManager instances to your running cluster with the `bin/jobmanager.sh` and `bin/taskmanager.sh` scripts.

#### Adding a JobManager

{% highlight bash %}
bin/jobmanager.sh ((start|start-foreground) [host] [webui-port])|stop|stop-all
{% endhighlight %}

#### Adding a TaskManager

{% highlight bash %}
bin/taskmanager.sh start|start-foreground|stop|stop-all
{% endhighlight %}

Make sure to call these scripts on the hosts on which you want to start/stop the respective instance.

## High-Availability with Standalone

In order to enable HA for a standalone cluster, you have to use the [ZooKeeper HA services]({% link deployment/ha/zookeeper_ha.md %}).

Additionally, you have to configure your cluster to start multiple JobManagers.

### Masters File (masters)

In order to start an HA-cluster configure the *masters* file in `conf/masters`:

- **masters file**: The *masters file* contains all hosts, on which JobManagers are started, and the ports to which the web user interface binds.

  <pre>
jobManagerAddress1:webUIPort1
[...]
jobManagerAddressX:webUIPortX
  </pre>

By default, the job manager will pick a *random port* for inter process communication. You can change this via the [high-availability.jobmanager.port]({% link deployment/config.md %}#high-availability-jobmanager-port) key. This key accepts single ports (e.g. `50010`), ranges (`50000-50025`), or a combination of both (`50010,50011,50020-50025,50050-50075`).

### Example: Standalone Cluster with 2 JobManagers

1. **Configure high availability mode and ZooKeeper quorum** in `conf/flink-conf.yaml`:

   <pre>
high-availability: zookeeper
high-availability.zookeeper.quorum: localhost:2181
high-availability.zookeeper.path.root: /flink
high-availability.cluster-id: /cluster_one # important: customize per cluster
high-availability.storageDir: hdfs:///flink/recovery</pre>

2. **Configure masters** in `conf/masters`:

   <pre>
localhost:8081
localhost:8082</pre>

3. **Configure ZooKeeper server** in `conf/zoo.cfg` (currently it's only possible to run a single ZooKeeper server per machine):

   <pre>server.0=localhost:2888:3888</pre>

4. **Start ZooKeeper quorum**:

   <pre>
$ bin/start-zookeeper-quorum.sh
Starting zookeeper daemon on host localhost.</pre>

5. **Start an HA-cluster**:

   <pre>
$ bin/start-cluster.sh
Starting HA cluster with 2 masters and 1 peers in ZooKeeper quorum.
Starting standalonesession daemon on host localhost.
Starting standalonesession daemon on host localhost.
Starting taskexecutor daemon on host localhost.</pre>

6. **Stop ZooKeeper quorum and cluster**:

   <pre>
$ bin/stop-cluster.sh
Stopping taskexecutor daemon (pid: 7647) on localhost.
Stopping standalonesession daemon (pid: 7495) on host localhost.
Stopping standalonesession daemon (pid: 7349) on host localhost.
$ bin/stop-zookeeper-quorum.sh
Stopping zookeeper daemon (pid: 7101) on host localhost.</pre>


{% top %}
