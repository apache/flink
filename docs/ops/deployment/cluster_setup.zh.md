---
title: "独立集群"
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

Go to the [downloads page]({{ site.download_url }}) and get the ready-to-run package. Make sure to pick the Flink package **matching your Hadoop version**. If you don't plan to use Hadoop, pick any version.

After downloading the latest release, copy the archive to your master node and extract it:

{% highlight bash %}
tar xzf flink-*.tgz
cd flink-*
{% endhighlight %}

### Configuring Flink

After having extracted the system files, you need to configure Flink for the cluster by editing *conf/flink-conf.yaml*.

Set the `jobmanager.rpc.address` key to point to your master node. You should also define the maximum amount of main memory Flink is allowed to allocate on each node by setting the `jobmanager.memory.process.size` and `taskmanager.memory.process.size` keys.

These values are given in MB. If some worker nodes have more main memory which you want to allocate to the Flink system you can overwrite the default value by setting setting `taskmanager.memory.process.size` or `taskmanager.memory.flink.size` in *conf/flink-conf.yaml* on those specific nodes.

Finally, you must provide a list of all nodes in your cluster that shall be used as worker nodes, i.e., nodes running a TaskManager. Edit the file *conf/workers* and enter the IP/host name of each worker node.

The following example illustrates the setup with three nodes (with IP addresses from _10.0.0.1_
to _10.0.0.3_ and hostnames _master_, _worker1_, _worker2_) and shows the contents of the
configuration files (which need to be accessible at the same path on all machines):

<div class="row">
  <div class="col-md-6 text-center">
    <img src="{{ site.baseurl }}/page/img/quickstart_cluster.png" style="width: 60%">
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

Please see the [configuration page](../config.html) for details and additional configuration options.

In particular,

 * the amount of available memory per JobManager (`jobmanager.memory.process.size`),
 * the amount of available memory per TaskManager (`taskmanager.memory.process.size` and check [memory setup guide](../memory/mem_tuning.html#configure-memory-for-standalone-deployment)),
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

{% top %}
