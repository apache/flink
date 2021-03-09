---
title: "Configuration"
weight: 3
type: docs
bookToc: false
aliases:
  - /deployment/config.html
  - /ops/config.html
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

# Configuration

All configuration is done in `conf/flink-conf.yaml`, which is expected to be a flat collection of [YAML key value pairs](http://www.yaml.org/spec/1.2/spec.html) with format `key: value`.

The configuration is parsed and evaluated when the Flink processes are started. Changes to the configuration file require restarting the relevant processes.

The out of the box configuration will use your default Java installation. You can manually set the environment variable `JAVA_HOME` or the configuration key `env.java.home` in `conf/flink-conf.yaml` if you want to manually override the Java runtime to use.

# Basic Setup

The default configuration supports starting a single-node Flink session cluster without any changes.
The options in this section are the ones most commonly needed for a basic distributed Flink setup.

**Hostnames / Ports**

These options are only necessary for *standalone* application- or session deployments ([simple standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) or [Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}})).

If you use Flink with [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}), [Mesos]({{< ref "docs/deployment/resource-providers/mesos" >}}), or the [*active* Kubernetes integration]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}), the hostnames and ports are automatically discovered.

  - `rest.address`, `rest.port`: These are used by the client to connect to Flink. Set this to the hostname where the JobManager runs, or to the hostname of the (Kubernetes) service in front of the JobManager's REST interface.

  - The `jobmanager.rpc.address` (defaults to *"localhost"*) and `jobmanager.rpc.port` (defaults to *6123*) config entries are used by the TaskManager to connect to the JobManager/ResourceManager. Set this to the hostname where the JobManager runs, or to the hostname of the (Kubernetes internal) service for the JobManager. This option is ignored on [setups with high-availability]({{< ref "docs/deployment/ha/overview" >}}) where the leader election mechanism is used to discover this automatically.

**Memory Sizes** 

The default memory sizes support simple streaming/batch applications, but are too low to yield good performance for more complex applications.

  - `jobmanager.memory.process.size`: Total size of the *JobManager* (JobMaster / ResourceManager / Dispatcher) process.
  - `taskmanager.memory.process.size`: Total size of the TaskManager process.

The total sizes include everything. Flink will subtract some memory for the JVM's own memory requirements (metaspace and others), and divide and configure the rest automatically between its components (JVM Heap, Off-Heap, for Task Managers also network, managed memory etc.).

These value are configured as memory sizes, for example *1536m* or *2g*.

**Parallelism**

  - `taskmanager.numberOfTaskSlots`: The number of slots that a TaskManager offers *(default: 1)*. Each slot can take one task or pipeline.
    Having multiple slots in a TaskManager can help amortize certain constant overheads (of the JVM, application libraries, or network connections) across parallel tasks or pipelines. See the [Task Slots and Resources]({{< ref "docs/concepts/flink-architecture" >}}#task-slots-and-resources) concepts section for details.

     Running more smaller TaskManagers with one slot each is a good starting point and leads to the best isolation between tasks. Dedicating the same resources to fewer larger TaskManagers with more slots can help to increase resource utilization, at the cost of weaker isolation between the tasks (more tasks share the same JVM).

  - `parallelism.default`: The default parallelism used when no parallelism is specified anywhere *(default: 1)*.

**Checkpointing**

You can configure checkpointing directly in code within your Flink job or application. Putting these values here in the configuration defines them as defaults in case the application does not configure anything.

  - `state.backend`: The state backend to use. This defines the data structure mechanism for taking snapshots. Common values are `filesystem` or `rocksdb`.
  - `state.checkpoints.dir`: The directory to write checkpoints to. This takes a path URI like *s3://mybucket/flink-app/checkpoints* or *hdfs://namenode:port/flink/checkpoints*.
  - `state.savepoints.dir`: The default directory for savepoints. Takes a path URI, similar to `state.checkpoints.dir`.

**Web UI**

  - `web.submit.enable`: Enables uploading and starting jobs through the Flink UI *(true by default)*. Please note that even when this is disabled, session clusters still accept jobs through REST requests (HTTP calls). This flag only guards the feature to upload jobs in the UI.
  - `web.upload.dir`: The directory where to store uploaded jobs. Only used when `web.submit.enable` is true.

**Other**

  - `io.tmp.dirs`: The directories where Flink puts local data, defaults to the system temp directory (`java.io.tmpdir` property). If a list of directories is configured, Flink will rotate files across the directories.
    
    The data put in these directories include by default the files created by RocksDB, spilled intermediate results (batch algorithms), and cached jar files.
    
    This data is NOT relied upon for persistence/recovery, but if this data gets deleted, it typically causes a heavyweight recovery operation. It is hence recommended to set this to a directory that is not automatically periodically purged.
    
    Yarn, Mesos, and Kubernetes setups automatically configure this value to the local working directories by default.

----
----

# Common Setup Options

*Common options to configure your Flink application or cluster.*

### Hosts and Ports

Options to configure hostnames and ports for the different Flink components.

The JobManager hostname and port are only relevant for standalone setups without high-availability.
In that setup, the config values are used by the TaskManagers to find (and connect to) the JobManager.
In all highly-available setups, the TaskManagers discover the JobManager via the High-Availability-Service (for example ZooKeeper).

Setups using resource orchestration frameworks (K8s, Yarn, Mesos) typically use the framework's service discovery facilities.

You do not need to configure any TaskManager hosts and ports, unless the setup requires the use of specific port ranges or specific network interfaces to bind to.

{{< generated/common_host_port_section >}}

### Fault Tolerance

These configuration options control Flink's restart behaviour in case of failures during the execution. 
By configuring these options in your `flink-conf.yaml`, you define the cluster's default restart strategy. 

The default restart strategy will only take effect if no job specific restart strategy has been configured via the `ExecutionConfig`.

{{< generated/restart_strategy_configuration >}}

**Fixed Delay Restart Strategy**

{{< generated/fixed_delay_restart_strategy_configuration >}}

**Failure Rate Restart Strategy**

{{< generated/failure_rate_restart_strategy_configuration >}}

### Checkpoints and State Backends

These options control the basic setup of state backends and checkpointing behavior.

The options are only relevant for jobs/applications executing in a continuous streaming fashion.
Jobs/applications executing in a batch fashion do not use state backends and checkpoints, but different internal data structures that are optimized for batch processing.

{{< generated/common_state_backends_section >}}

### High Availability

High-availability here refers to the ability of the JobManager process to recover from failures.

The JobManager ensures consistency during recovery across TaskManagers. For the JobManager itself to recover consistently, an external service must store a minimal amount of recovery metadata (like "ID of last committed checkpoint"), as well as help to elect and lock which JobManager is the leader (to avoid split-brain situations).

{{< generated/common_high_availability_section >}}

**Options for high-availability setups with ZooKeeper**

{{< generated/common_high_availability_zk_section >}}

### Memory Configuration

These configuration values control the way that TaskManagers and JobManagers use memory.

Flink tries to shield users as much as possible from the complexity of configuring the JVM for data-intensive processing.
In most cases, users should only need to set the values `taskmanager.memory.process.size` or `taskmanager.memory.flink.size` (depending on how the setup), and possibly adjusting the ratio of JVM heap and Managed Memory via `taskmanager.memory.managed.fraction`. The other options below can be used for performance tuning and fixing memory related errors.

For a detailed explanation of how these options interact,
see the documentation on [TaskManager]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) and
[JobManager]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}} ) memory configurations.

{{< generated/common_memory_section >}}

### Miscellaneous Options

{{< generated/common_miscellaneous_section >}}

----
----

# Security

Options for configuring Flink's security and secure interaction with external systems.

### SSL

Flink's network connections can be secured via SSL. Please refer to the [SSL Setup Docs]({{< ref "docs/deployment/security/security-ssl" >}}) for detailed setup guide and background.

{{< generated/security_ssl_section >}}


### Auth with External Systems

**ZooKeeper Authentication / Authorization**

These options are necessary when connecting to a secured ZooKeeper quorum.

{{< generated/security_auth_zk_section >}}

**Kerberos-based Authentication / Authorization**

Please refer to the [Flink and Kerberos Docs]({{< ref "docs/deployment/security/security-kerberos" >}}) for a setup guide and a list of external system to which Flink can authenticate itself via Kerberos.

{{< generated/security_auth_kerberos_section >}}

----
----

# Resource Orchestration Frameworks

This section contains options related to integrating Flink with resource orchestration frameworks, like Kubernetes, Yarn, Mesos, etc.

Note that is not always necessary to integrate Flink with the resource orchestration framework.
For example, you can easily deploy Flink applications on Kubernetes without Flink knowing that it runs on Kubernetes (and without specifying any of the Kubernetes config options here.) See [this setup guide]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) for an example.

The options in this section are necessary for setups where Flink itself actively requests and releases resources from the orchestrators.

### YARN

{{< generated/yarn_config_configuration >}}

### Kubernetes

{{< generated/kubernetes_config_configuration >}}

### Mesos

{{< generated/mesos_configuration >}}

**Mesos TaskManager**

{{< generated/mesos_task_manager_configuration >}}

----
----

# State Backends

Please refer to the [State Backend Documentation]({{< ref "docs/ops/state/state_backends" >}}) for background on State Backends.

### RocksDB State Backend

These are the options commonly needed to configure the RocksDB state backend. See the [Advanced RocksDB Backend Section](#advanced-rocksdb-state-backends-options) for options necessary for advanced low level configurations and trouble-shooting.

{{< generated/state_backend_rocksdb_section >}}

----
----

# Metrics

Please refer to the [metrics system documentation]({{< ref "docs/ops/metrics" >}}) for background on Flink's metrics infrastructure.

{{< generated/metric_configuration >}}

### RocksDB Native Metrics

Flink can report metrics from RocksDB's native code, for applications using the RocksDB state backend.
The metrics here are scoped to the operators and then further broken down by column family; values are reported as unsigned longs. 

{{< hint warning >}}
Enabling RocksDB's native metrics may cause degraded performance and should be set carefully. 
{{< /hint >}}

{{< generated/rocksdb_native_metric_configuration >}}

----
----

# History Server

The history server keeps the information of completed jobs (graphs, runtimes, statistics). To enable it, you have to enable "job archiving" in the JobManager (`jobmanager.archive.fs.dir`).

See the [History Server Docs]({{< ref "docs/deployment/advanced/historyserver" >}}) for details.

{{< generated/history_server_configuration >}}

----
----

# Experimental

*Options for experimental features in Flink.*

### Queryable State

*Queryable State* is an experimental features that gives lets you access Flink's internal state like a key/value store.
See the [Queryable State Docs]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) for details.

{{< generated/queryable_state_configuration >}}

----
----

# Debugging & Expert Tuning

<div class="alert alert-warning">
  The options below here are meant for expert users and for fixing/debugging problems. Most setups should not need to configure these options.
</div>

### Class Loading

Flink dynamically loads the code for jobs submitted to a session cluster. In addition, Flink tries to hide many dependencies in the classpath from the application. This helps to reduce dependency conflicts between the application code and the dependencies in the classpath.

Please refer to the [Debugging Classloading Docs]({{< ref "docs/ops/debugging/debugging_classloading" >}}) for details.

{{< generated/expert_class_loading_section >}}

### Advanced Options for the debugging

{{< generated/expert_debugging_and_tuning_section >}}

### Advanced State Backends Options

{{< generated/expert_state_backends_section >}}

### Advanced RocksDB State Backends Options

Advanced options to tune RocksDB and RocksDB checkpoints.

{{< generated/expert_rocksdb_section >}}

**RocksDB Configurable Options**

These options give fine-grained control over the behavior and resoures of ColumnFamilies.
With the introduction of `state.backend.rocksdb.memory.managed` and `state.backend.rocksdb.memory.fixed-per-slot` (Apache Flink 1.10), it should be only necessary to use the options here for advanced performance tuning. These options here can also be specified in the application program via `RocksDBStateBackend.setRocksDBOptions(RocksDBOptionsFactory)`.

{{< generated/rocksdb_configurable_configuration >}}

### Advanced Fault Tolerance Options

*These parameters can help with problems related to failover and to components erroneously considering each other as failed.*

{{< generated/expert_fault_tolerance_section >}}

### Advanced Cluster Options

{{< generated/expert_cluster_section >}}

### Advanced Scheduling Options

*These parameters can help with fine-tuning scheduling for specific situations.*

{{< generated/expert_scheduling_section >}}

### Advanced High-availability Options

{{< generated/expert_high_availability_section >}}

### Advanced High-availability ZooKeeper Options

{{< generated/expert_high_availability_zk_section >}}

### Advanced High-availability Kubernetes Options

{{< generated/expert_high_availability_k8s_section >}}

### Advanced SSL Security Options

{{< generated/expert_security_ssl_section >}}

### Advanced Options for the REST endpoint and Client

{{< generated/expert_rest_section >}}

### Advanced Options for Flink Web UI

{{< generated/web_configuration >}}

### Full JobManager Options

**JobManager**

{{< generated/all_jobmanager_section >}}

**Blob Server**

The Blob Server is a component in the JobManager. It is used for distribution of objects that are too large to be attached to a RPC message and that benefit from caching (like Jar files or large serialized code objects).

{{< generated/blob_server_configuration >}}

**ResourceManager**

These configuration keys control basic Resource Manager behavior, independent of the used resource orchestration management framework (YARN, Mesos, etc.)

{{< generated/resource_manager_configuration >}}

### Full TaskManagerOptions

{{< generated/all_taskmanager_section >}}

**Data Transport Network Stack**

These options are for the network stack that handles the streaming and batch data exchanges between TaskManagers.

{{< generated/all_taskmanager_network_section >}}

### RPC / Akka

Flink uses Akka for RPC between components (JobManager/TaskManager/ResourceManager).
Flink does not use Akka for data transport.

{{< generated/akka_configuration >}}

----
----

# JVM and Logging Options

{{< generated/environment_configuration >}}

# Forwarding Environment Variables

You can configure environment variables to be set on the JobManager and TaskManager processes started on Yarn/Mesos.

  - `containerized.master.env.`: Prefix for passing custom environment variables to Flink's JobManager process. 
   For example for passing LD_LIBRARY_PATH as an env variable to the JobManager, set containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
    in the flink-conf.yaml.

  - `containerized.taskmanager.env.`: Similar to the above, this configuration prefix allows setting custom environment variables for the workers (TaskManagers).

----
----

# Deprecated Options

These options relate to parts of Flink that are not actively developed any more.
These options may be removed in a future release.

**DataSet API Optimizer**

{{< generated/optimizer_configuration >}}

**DataSet API Runtime Algorithms**

{{< generated/algorithm_configuration >}}

**DataSet File Sinks**

{{< generated/deprecated_file_sinks_section >}}

----
----

# Backup

#### Client

{{< generated/client_configuration >}}

#### Execution

{{< generated/deployment_configuration >}}
{{< generated/savepoint_config_configuration >}}
{{< generated/execution_configuration >}}

#### Pipeline

{{< generated/pipeline_configuration >}}
{{< generated/stream_pipeline_configuration >}}

#### Checkpointing

{{< generated/execution_checkpointing_configuration >}}

{{< top >}}
