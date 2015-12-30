---
title: "JobManager High Availability (HA)"
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

The JobManager is coordinates every Flink deployment. It is responsible for both *scheduling* and *resource management*.

By default, there is a single JobManager instance per Flink cluster. This creates a *single point of failure* (SPOF): if the JobManager crashes, no new programs can be submitted and running programs fail.

With JobManager High Availability, you can run recover from JobManager failures and thereby eliminate the *SPOF*. You can configure high availability for both **standalone** and **YARN clusters**.

* Toc
{:toc}

## Standalone Cluster High Availability

The general idea of JobManager high availability for standalone clusters is that there is a **single leading JobManager** at any time and **multiple standby JobManagers** to take over leadership in case the leader fails. This guarantees that there is **no single point of failure** and programs can make progress as soon as a standby JobManager has taken leadership. There is no explicit distinction between standby and master JobManager instances. Each JobManager can take the role of master or standby.

As an example, consider the following setup with three JobManager instances:

<img src="fig/jobmanager_ha_overview.png" class="center" />

### Configuration

To enable JobManager High Availability you have to set the **recovery mode** to *zookeeper*, configure a **ZooKeeper quorum** and set up a **masters file** with all JobManagers hosts and their web UI ports.

Flink leverages **[ZooKeeper](http://zookeeper.apache.org)** for  *distributed coordination* between all running JobManager instances. ZooKeeper is a separate service from Flink, which provides highly reliable distributed coordination via leader election and light-weight consistent state storage. Check out [ZooKeeper's Getting Started Guide](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html) for more information about ZooKeeper. Flink includes scripts to [bootstrap a simple ZooKeeper](#bootstrap-zookeeper) installation.

#### Masters File (masters)

In order to start an HA-cluster configure the *masters* file in `conf/masters`:

- **masters file**: The *masters file* contains all hosts, on which JobManagers are started, and the ports to which the web user interface binds.

  <pre>
jobManagerAddress1:webUIPort1
[...]
jobManagerAddressX:webUIPortX
  </pre>

#### Config File (flink-conf.yaml)

In order to start an HA-cluster add the following configuration keys to `conf/flink-conf.yaml`:

- **Recovery mode** (required): The *recovery mode* has to be set in `conf/flink-conf.yaml` to *zookeeper* in order to enable high availability mode.

  <pre>recovery.mode: zookeeper</pre>

- **ZooKeeper quorum** (required): A *ZooKeeper quorum* is a replicated group of ZooKeeper servers, which provide the distributed coordination service.

  <pre>recovery.zookeeper.quorum: address1:2181[,...],addressX:2181</pre>

  Each *addressX:port* refers to a ZooKeeper server, which is reachable by Flink at the given address and port.

- **ZooKeeper root** (recommended): The *root ZooKeeper node*, under which all required coordination data is placed.

  <pre>recovery.zookeeper.path.root: /flink # important: customize per cluster</pre>

  **Important**: if you are running multiple Flink HA clusters, you have to manually configure seperate root nodes for each cluster.

- **State backend and storage directory** (required): JobManager meta data is persisted in the *state backend* and only a pointer to this state is stored in ZooKeeper. Currently, only the file system state backend is supported in HA mode.

    <pre>
state.backend: filesystem
state.backend.fs.checkpointdir: hdfs:///flink/checkpoints
recovery.zookeeper.storageDir: hdfs:///flink/recovery/</pre>

    The `storageDir` stores all meta data needed to recover a JobManager failure.

After configuring the masters and the ZooKeeper quorum, you can use the provided cluster startup scripts as usual. They will start an HA-cluster. Keep in mind that the **ZooKeeper quorum has to be running** when you call the scripts and make sure to **configure a seperate ZooKeeper root path** for each HA cluster you are starting.

#### Example: Standalone Cluster with 2 JobManagers

1. **Configure recovery mode and ZooKeeper quorum** in `conf/flink-conf.yaml`:

   <pre>
recovery.mode: zookeeper
recovery.zookeeper.quorum: localhost:2181
recovery.zookeeper.path.root: /flink # important: customize per cluster
state.backend: filesystem
state.backend.fs.checkpointdir: hdfs:///flink/checkpoints
recovery.zookeeper.storageDir: hdfs:///flink/recovery/</pre>

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
Starting jobmanager daemon on host localhost.
Starting jobmanager daemon on host localhost.
Starting taskmanager daemon on host localhost.</pre>

6. **Stop ZooKeeper quorum and cluster**:

   <pre>
$ bin/stop-cluster.sh
Stopping taskmanager daemon (pid: 7647) on localhost.
Stopping jobmanager daemon (pid: 7495) on host localhost.
Stopping jobmanager daemon (pid: 7349) on host localhost.
$ bin/stop-zookeeper-quorum.sh
Stopping zookeeper daemon (pid: 7101) on host localhost.</pre>

## YARN Cluster High Availability

When running a highly available YARN cluster, **we don't run multiple JobManager (ApplicationMaster) instances**, but only one, which is restarted by YARN on failures. The exact behaviour depends on on the specific YARN version you are using.

### Configuration

#### Maximum Application Master Attempts (yarn-site.xml)

You have to configure the maximum number of attempts for the application masters for **your** YARN setup in `yarn-site.xml`:

{% highlight xml %}
<property>
  <name>yarn.resourcemanager.am.max-attempts</name>
  <value>4</value>
  <description>
    The maximum number of application master execution attempts.
  </description>
</property>
{% endhighlight %}

The default for current YARN versions is 2 (meaning a single JobManager failure is tolerated).

#### Application Attempts (flink-conf.yaml)

In addition to the HA configuration ([see above](#configuration)), you have to configure the maximum attempts in `conf/flink-conf.yaml`:

<pre>yarn.application-attempts: 10</pre>

This means that the application can be restarted 10 times before YARN fails the application. It's important to note that `yarn.resourcemanager.am.max-attempts` is an upper bound for the application restarts. Therfore, the number of application attempts set within Flink cannot exceed the YARN cluster setting with which YARN was started.

#### Container Shutdown Behaviour

- **YARN 2.3.0 < version < 2.4.0**. All containers are restarted if the application master fails.
- **YARN 2.4.0 < version < 2.6.0**. TaskManager containers are kept alive across application master failures. This has the advantage that the startup time is faster and that the user does not have to wait for obtaining the container resources again.
- **YARN 2.6.0 <= version**: Sets the attempt failure validity interval to the Flinks' Akka timeout value. The attempt failure validity interval says that an application is only killed after the system has seen the maximum number of application attempts during one interval. This avoids that a long lasting job will deplete it's application attempts.

#### Example: Highly Available YARN Session

1. **Configure recovery mode and ZooKeeper quorum** in `conf/flink-conf.yaml`:

   <pre>
recovery.mode: zookeeper
recovery.zookeeper.quorum: localhost:2181
recovery.zookeeper.path.root: /flink # important: customize per cluster
state.backend: filesystem
state.backend.fs.checkpointdir: hdfs:///flink/checkpoints
recovery.zookeeper.storageDir: hdfs:///flink/recovery/
yarn.application-attempts: 10</pre>

3. **Configure ZooKeeper server** in `conf/zoo.cfg` (currently it's only possible to run a single ZooKeeper server per machine):

   <pre>server.0=localhost:2888:3888</pre>

4. **Start ZooKeeper quorum**:

   <pre>
$ bin/start-zookeeper-quorum.sh
Starting zookeeper daemon on host localhost.</pre>

5. **Start an HA-cluster**:

   <pre>
$ bin/yarn-session.sh -n 2</pre>

## Bootstrap ZooKeeper

If you don't have a running ZooKeeper installation, you can use the helper scripts, which ship with Flink.

There is a ZooKeeper configuration template in `conf/zoo.cfg`. You can configure the hosts to run ZooKeeper on with the `server.X` entries, where X is a unique ID of each server:

<pre>
server.X=addressX:peerPort:leaderPort
[...]
server.Y=addressY:peerPort:leaderPort
</pre>

The script `bin/start-zookeeper-quorum.sh` will start a ZooKeeper server on each of the configured hosts. The started processes start ZooKeeper servers via a Flink wrapper, which reads the configuration from `conf/zoo.cfg` and makes sure to set some required configuration values for convenience. In production setups, it is recommended to manage your own ZooKeeper installation.
