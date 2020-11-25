---
title: "ZooKeeper HA Services"
nav-title: ZooKeeper HA Services
nav-parent_id: ha
nav-pos: 1
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

## ZooKeeper HA Services

One high availability services implementation uses ZooKeeper.

### Configuration

To enable JobManager High Availability you have to set the **high-availability mode** to *zookeeper*, configure a **ZooKeeper quorum** and set up a **masters file** with all JobManagers hosts and their web UI ports.

Flink leverages **[ZooKeeper](http://zookeeper.apache.org)** for *distributed coordination* between all running JobManager instances. ZooKeeper is a separate service from Flink, which provides highly reliable distributed coordination via leader election and light-weight consistent state storage. Check out [ZooKeeper's Getting Started Guide](http://zookeeper.apache.org/doc/current/zookeeperStarted.html) for more information about ZooKeeper. Flink includes scripts to [bootstrap a simple ZooKeeper](#bootstrap-zookeeper) installation.

#### Masters File (masters)

In order to start an HA-cluster configure the *masters* file in `conf/masters`:

- **masters file**: The *masters file* contains all hosts, on which JobManagers are started, and the ports to which the web user interface binds.

  <pre>
jobManagerAddress1:webUIPort1
[...]
jobManagerAddressX:webUIPortX
  </pre>

By default, the job manager will pick a *random port* for inter process communication. You can change this via the **`high-availability.jobmanager.port`** key. This key accepts single ports (e.g. `50010`), ranges (`50000-50025`), or a combination of both (`50010,50011,50020-50025,50050-50075`).

#### Config File (flink-conf.yaml)

In order to start an HA-cluster add the following configuration keys to `conf/flink-conf.yaml`:

- **high-availability mode** (required): The *high-availability mode* has to be set in `conf/flink-conf.yaml` to *zookeeper* in order to enable high availability mode.
Alternatively this option can be set to FQN of factory class Flink should use to create HighAvailabilityServices instance. 

  <pre>high-availability: zookeeper</pre>

- **ZooKeeper quorum** (required): A *ZooKeeper quorum* is a replicated group of ZooKeeper servers, which provide the distributed coordination service.

  <pre>high-availability.zookeeper.quorum: address1:2181[,...],addressX:2181</pre>

  Each *addressX:port* refers to a ZooKeeper server, which is reachable by Flink at the given address and port.

- **ZooKeeper root** (recommended): The *root ZooKeeper node*, under which all cluster nodes are placed.

  <pre>high-availability.zookeeper.path.root: /flink

- **ZooKeeper cluster-id** (recommended): The *cluster-id ZooKeeper node*, under which all required coordination data for a cluster is placed.

  <pre>high-availability.cluster-id: /default_ns # important: customize per cluster</pre>

  **Important**: You should not set this value manually when running a YARN
  cluster, a per-job YARN session, or on another cluster manager. In those
  cases a cluster-id is automatically being generated based on the application
  id. Manually setting a cluster-id overrides this behaviour in YARN.
  Specifying a cluster-id with the -z CLI option, in turn, overrides manual
  configuration. If you are running multiple Flink HA clusters on bare metal,
  you have to manually configure separate cluster-ids for each cluster.

- **Storage directory** (required): JobManager metadata is persisted in the file system *storageDir* and only a pointer to this state is stored in ZooKeeper.

    <pre>
high-availability.storageDir: hdfs:///flink/recovery
    </pre>

    The `storageDir` stores all metadata needed to recover a JobManager failure.

After configuring the masters and the ZooKeeper quorum, you can use the provided cluster startup scripts as usual. They will start an HA-cluster. Keep in mind that the **ZooKeeper quorum has to be running** when you call the scripts and make sure to **configure a separate ZooKeeper root path** for each HA cluster you are starting.

#### Example: Standalone Cluster with 2 JobManagers

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
