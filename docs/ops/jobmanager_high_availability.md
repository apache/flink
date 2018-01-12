---
title: "JobManager High Availability (HA)"
nav-title: High Availability (HA)
nav-parent_id: ops
nav-pos: 2
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

The JobManager coordinates every Flink deployment. It is responsible for both *scheduling* and *resource management*.

By default, there is a single JobManager instance per Flink cluster. This creates a *single point of failure* (SPOF): if the JobManager crashes, no new programs can be submitted and running programs fail.

With JobManager High Availability, you can recover from JobManager failures and thereby eliminate the *SPOF*. You can configure high availability for both **standalone** and **YARN clusters**.

* Toc
{:toc}

## Standalone Cluster High Availability

The general idea of JobManager high availability for standalone clusters is that there is a **single leading JobManager** at any time and **multiple standby JobManagers** to take over leadership in case the leader fails. This guarantees that there is **no single point of failure** and programs can make progress as soon as a standby JobManager has taken leadership. There is no explicit distinction between standby and master JobManager instances. Each JobManager can take the role of master or standby.

As an example, consider the following setup with three JobManager instances:

<img src="{{ site.baseurl }}/fig/jobmanager_ha_overview.png" class="center" />

### Configuration

To enable JobManager High Availability you have to set the **high-availability mode** to *zookeeper*, configure a **ZooKeeper quorum** and set up a **masters file** with all JobManagers hosts and their web UI ports.

Flink leverages **[ZooKeeper](http://zookeeper.apache.org)** for *distributed coordination* between all running JobManager instances. ZooKeeper is a separate service from Flink, which provides highly reliable distributed coordination via leader election and light-weight consistent state storage. Check out [ZooKeeper's Getting Started Guide](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html) for more information about ZooKeeper. Flink includes scripts to [bootstrap a simple ZooKeeper](#bootstrap-zookeeper) installation.

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
high-availability.zookeeper.storageDir: hdfs:///flink/recovery
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
high-availability.zookeeper.storageDir: hdfs:///flink/recovery</pre>

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

This means that the application can be restarted 9 times for failed attempts before YARN fails the application (9 retries + 1 initial attempt). Additional restarts can be performed by YARN if required by YARN operations: Preemption, node hardware failures or reboots, or NodeManager resyncs. These restarts are not counted against `yarn.application-attempts`, see <a href="http://johnjianfang.blogspot.de/2015/04/the-number-of-maximum-attempts-of-yarn.html">Jian Fang's blog post</a>. It's important to note that `yarn.resourcemanager.am.max-attempts` is an upper bound for the application restarts. Therefore, the number of application attempts set within Flink cannot exceed the YARN cluster setting with which YARN was started.

#### Container Shutdown Behaviour

- **YARN 2.3.0 < version < 2.4.0**. All containers are restarted if the application master fails.
- **YARN 2.4.0 < version < 2.6.0**. TaskManager containers are kept alive across application master failures. This has the advantage that the startup time is faster and that the user does not have to wait for obtaining the container resources again.
- **YARN 2.6.0 <= version**: Sets the attempt failure validity interval to the Flinks' Akka timeout value. The attempt failure validity interval says that an application is only killed after the system has seen the maximum number of application attempts during one interval. This avoids that a long lasting job will deplete it's application attempts.

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>Note</b>: Hadoop YARN 2.4.0 has a major bug (fixed in 2.5.0) preventing container restarts from a restarted Application Master/Job Manager container. See <a href="https://issues.apache.org/jira/browse/FLINK-4142">FLINK-4142</a> for details. We recommend using at least Hadoop 2.5.0 for high availability setups on YARN.</p>

#### Example: Highly Available YARN Session

1. **Configure HA mode and ZooKeeper quorum** in `conf/flink-conf.yaml`:

   <pre>
high-availability: zookeeper
high-availability.zookeeper.quorum: localhost:2181
high-availability.zookeeper.storageDir: hdfs:///flink/recovery
high-availability.zookeeper.path.root: /flink
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

## Configuring for Zookeeper Security

If ZooKeeper is running in secure mode with Kerberos, you can override the following configurations in `flink-conf.yaml` as necessary:

<pre>
zookeeper.sasl.service-name: zookeeper     # default is "zookeeper". If the ZooKeeper quorum is configured
                                           # with a different service name then it can be supplied here.
zookeeper.sasl.login-context-name: Client  # default is "Client". The value needs to match one of the values
                                           # configured in "security.kerberos.login.contexts".
</pre>

For more information on Flink configuration for Kerberos security, please see [here]({{ site.baseurl}}/ops/config.html).
You can also find [here]({{ site.baseurl}}/ops/security-kerberos.html) further details on how Flink internally setups Kerberos-based security.

## Bootstrap ZooKeeper

If you don't have a running ZooKeeper installation, you can use the helper scripts, which ship with Flink.

There is a ZooKeeper configuration template in `conf/zoo.cfg`. You can configure the hosts to run ZooKeeper on with the `server.X` entries, where X is a unique ID of each server:

<pre>
server.X=addressX:peerPort:leaderPort
[...]
server.Y=addressY:peerPort:leaderPort
</pre>

The script `bin/start-zookeeper-quorum.sh` will start a ZooKeeper server on each of the configured hosts. The started processes start ZooKeeper servers via a Flink wrapper, which reads the configuration from `conf/zoo.cfg` and makes sure to set some required configuration values for convenience. In production setups, it is recommended to manage your own ZooKeeper installation.

{% top %}
