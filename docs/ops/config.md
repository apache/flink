---
title: "Configuration"
nav-id: "config"
nav-parent_id: ops
nav-pos: 4
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

**For single-node setups Flink is ready to go out of the box and you don't need to change the default configuration to get started.**

The out of the box configuration will use your default Java installation. You can manually set the environment variable `JAVA_HOME` or the configuration key `env.java.home` in `conf/flink-conf.yaml` if you want to manually override the Java runtime to use.

This page lists the most common options that are typically needed to set up a well performing (distributed) installation. In addition a full list of all available configuration parameters is listed here.

All configuration is done in `conf/flink-conf.yaml`, which is expected to be a flat collection of [YAML key value pairs](http://www.yaml.org/spec/1.2/spec.html) with format `key: value`.

The system and run scripts parse the config at startup time. Changes to the configuration file require restarting the Flink JobManager and TaskManagers.

The configuration files for the TaskManagers can be different, Flink does not assume uniform machines in the cluster.

* This will be replaced by the TOC
{:toc}

## Common Options

- `env.java.home`: The path to the Java installation to use (DEFAULT: system's default Java installation, if found). Needs to be specified if the startup scripts fail to automatically resolve the java home directory. Can be specified to point to a specific java installation or version. If this option is not specified, the startup scripts also evaluate the `$JAVA_HOME` environment variable.

- `env.java.opts`: Set custom JVM options. This value is respected by Flink's start scripts, both JobManager and
TaskManager, and Flink's YARN client. This can be used to set different garbage collectors or to include remote
debuggers into the JVMs running Flink's services. Enclosing options in double quotes delays parameter substitution
allowing access to variables from Flink's startup scripts. Use `env.java.opts.jobmanager` and `env.java.opts.taskmanager`
for JobManager or TaskManager-specific options, respectively.

- `env.java.opts.jobmanager`: JobManager-specific JVM options. These are used in addition to the regular `env.java.opts`.

- `env.java.opts.taskmanager`: TaskManager-specific JVM options. These are used in addition to the regular `env.java.opts`.

- `jobmanager.rpc.address`: The external address of the JobManager, which is the master/coordinator of the distributed system (DEFAULT: localhost). **Note:** The address (host name or IP) should be accessible by all nodes including the client.

- `jobmanager.rpc.port`: The port number of the JobManager (DEFAULT: 6123).

- `jobmanager.heap.mb`: JVM heap size (in megabytes) for the JobManager. You may have to increase the heap size for the JobManager if you are running very large applications (with many operators), or if you are keeping a long history of them.

- `taskmanager.heap.mb`: JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of the system. In contrast to Hadoop, Flink runs operators (e.g., join, aggregate) and user-defined functions (e.g., Map, Reduce, CoGroup) inside the TaskManager (including sorting/hashing/caching), so this value should be as large as possible. If the cluster is exclusively running Flink, the total amount of available memory per machine minus some memory for the operating system (maybe 1-2 GB) is a good value. On YARN setups, this value is automatically configured to the size of the TaskManager's YARN container, minus a certain tolerance value.

- `taskmanager.numberOfTaskSlots`: The number of parallel operator or user function instances that a single TaskManager can run (DEFAULT: 1). If this value is larger than 1, a single TaskManager takes multiple instances of a function or operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the available memory is divided between the different operator or function instances. This value is typically proportional to the number of physical CPU cores that the TaskManager's machine has (e.g., equal to the number of cores, or half the number of cores). [More about task slots](config.html#configuring-taskmanager-processing-slots).

- `parallelism.default`: The default parallelism to use for programs that have no parallelism specified. (DEFAULT: 1). For setups that have no concurrent jobs running, setting this value to NumTaskManagers * NumSlotsPerTaskManager will cause the system to use all available execution resources for the program's execution. **Note**: The default parallelism can be overwritten for an entire job by calling `setParallelism(int parallelism)` on the `ExecutionEnvironment` or by passing `-p <parallelism>` to the Flink Command-line frontend. It can be overwritten for single transformations by calling `setParallelism(int
parallelism)` on an operator. See [Parallel Execution]({{site.baseurl}}/dev/parallel.html) for more information about parallelism.

- `fs.default-scheme`: The default filesystem scheme to be used, with the necessary authority to contact, e.g. the host:port of the NameNode in the case of HDFS (if needed).
By default, this is set to `file:///` which points to the local filesystem. This means that the local
filesystem is going to be used to search for user-specified files **without** an explicit scheme
definition. As another example, if this is set to `hdfs://localhost:9000/`, then a user-specified file path
without explicit scheme definition, such as `/user/USERNAME/in.txt`, is going to be transformed into
`hdfs://localhost:9000/user/USERNAME/in.txt`. This scheme is used **ONLY** if no other scheme is specified (explicitly) in the user-provided `URI`.

- `classloader.resolve-order`: Whether Flink should use a child-first `ClassLoader` when loading
user-code classes or a parent-first `ClassLoader`. Can be one of `parent-first` or `child-first`. (default: `child-first`)

- `classloader.parent-first-patterns.default`: A (semicolon-separated) list of patterns that specifies which
classes should always be resolved through the parent `ClassLoader` first. A pattern is a simple
prefix that is checked against the fully qualified class name. By default, this is set to
`"java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache.logging.log4j;ch.qos.logback"`.
To extend this list beyond the default it is recommended to configure `classloader.parent-first-patterns.additional` instead of modifying this setting directly.

- `classloader.parent-first-patterns.additional`: A (semicolon-separated) list of patterns that specifies which
classes should always be resolved through the parent `ClassLoader` first. A pattern is a simple
prefix that is checked against the fully qualified class name.
This list is appended to `classloader.parent-first-patterns.default`.

## Advanced Options

### Compute

- `taskmanager.compute.numa`: When enabled a TaskManager is started on each NUMA node for each worker listed in *conf/slaves* (DEFAULT: false). Note: only supported when deploying Flink as a standalone cluster.

### Managed Memory

By default, Flink allocates a fraction of `0.7` of the free memory (total memory configured via `taskmanager.heap.mb` minus memory used for network buffers) for its managed memory. Managed memory helps Flink to run the batch operators efficiently. It prevents `OutOfMemoryException`s because Flink knows how much memory it can use to execute operations. If Flink runs out of managed memory, it utilizes disk space. Using managed memory, some operations can be performed directly on the raw data without having to deserialize the data to convert it into Java objects. All in all, managed memory improves the robustness and speed of the system.

The default fraction for managed memory can be adjusted using the `taskmanager.memory.fraction` parameter. An absolute value may be set using `taskmanager.memory.size` (overrides the fraction parameter). If desired, the managed memory may be allocated outside the JVM heap. This may improve performance in setups with large memory sizes.

- `taskmanager.memory.size`: The amount of memory (in megabytes) that the task manager reserves on-heap or off-heap (depending on `taskmanager.memory.off-heap`) for sorting, hash tables, and caching of intermediate results. If unspecified (-1), the memory manager will take a fixed ratio with respect to the size of the task manager JVM as specified by `taskmanager.memory.fraction`. (DEFAULT: -1)

- `taskmanager.memory.fraction`: The relative amount of memory (with respect to `taskmanager.heap.mb`, after subtracting the amount of memory used by network buffers) that the task manager reserves for sorting, hash tables, and caching of intermediate results. For example, a value of `0.8` means that a task manager reserves 80% of its memory (on-heap or off-heap depending on `taskmanager.memory.off-heap`) for internal data buffers, leaving 20% of free memory for the task manager's heap for objects created by user-defined functions. (DEFAULT: 0.7) This parameter is only evaluated, if `taskmanager.memory.size` is not set.

- `taskmanager.memory.off-heap`: If set to `true`, the task manager allocates memory which is used for sorting, hash tables, and caching of intermediate results outside of the JVM heap. For setups with larger quantities of memory, this can improve the efficiency of the operations performed on the memory (DEFAULT: false).

- `taskmanager.memory.segment-size`: The size of memory buffers used by the memory manager and the network stack in bytes (DEFAULT: 32768 (= 32 KiBytes)).

- `taskmanager.memory.preallocate`: Can be either of `true` or `false`. Specifies whether task managers should allocate all managed memory when starting up. (DEFAULT: false). When `taskmanager.memory.off-heap` is set to `true`, then it is advised that this configuration is also set to `true`.  If this configuration is set to `false` cleaning up of the allocated offheap memory happens only when the configured JVM parameter MaxDirectMemorySize is reached by triggering a full GC. **Note:** For streaming setups, we highly recommend to set this value to `false` as the core state backends currently do not use the managed memory.

### Memory and Performance Debugging

These options are useful for debugging a Flink application for memory and garbage collection related issues, such as performance and out-of-memory process kills or exceptions.

- `taskmanager.debug.memory.startLogThread`: Causes the TaskManagers to periodically log memory and Garbage collection statistics. The statistics include current heap-, off-heap, and other memory pool utilization, as well as the time spent on garbage collection, by heap memory pool.

- `taskmanager.debug.memory.logIntervalMs`: The interval (in milliseconds) in which the TaskManagers log the memory and garbage collection statistics. Only has an effect, if `taskmanager.debug.memory.startLogThread` is set to true.

### Kerberos-based Security

Flink supports Kerberos authentication for the following services:

+ Hadoop Components, such as HDFS, YARN, or HBase *(version 2.6.1 and above; all other versions have critical bugs which might fail the Flink job unexpectedly)*.
+ Kafka Connectors *(version 0.9+ and above)*.
+ Zookeeper

Configuring Flink for Kerberos security involves three aspects, explained separately in the following sub-sections.

##### 1. Providing the cluster with a Kerberos credential (i.e. a keytab or a ticket via `kinit`)

To provide the cluster with a Kerberos credential, Flink supports using a Kerberos keytab file or ticket caches managed by `kinit`.

- `security.kerberos.login.use-ticket-cache`: Indicates whether to read from your Kerberos ticket cache (default: `true`).

- `security.kerberos.login.keytab`: Absolute path to a Kerberos keytab file that contains the user credentials.

- `security.kerberos.login.principal`: Kerberos principal name associated with the keytab.

If both `security.kerberos.login.keytab` and `security.kerberos.login.principal` have values provided, keytabs will be used for authentication.
It is preferable to use keytabs for long-running jobs, to avoid ticket expiration issues.   If you prefer to use the ticket cache,
talk to your administrator about increasing the Hadoop delegation token lifetime.

Note that authentication using ticket caches is only supported when deploying Flink as a standalone cluster or on YARN.

##### 2. Making the Kerberos credential available to components and connectors as needed

For Hadoop components, Flink will automatically detect if the configured Kerberos credentials should be used when connecting to HDFS, HBase, and other Hadoop components depending on whether Hadoop security is enabled (in `core-site.xml`).

For any connector or component that uses a JAAS configuration file, make the Kerberos credentials available to them by configuring JAAS login contexts for each one respectively, using the following configuration:

- `security.kerberos.login.contexts`: A comma-separated list of login contexts to provide the Kerberos credentials to (for example, `Client,KafkaClient` to use the credentials for ZooKeeper authentication and for Kafka authentication).

This allows enabling Kerberos authentication for different connectors or components independently. For example, you can enable Hadoop security without necessitating the use of Kerberos for ZooKeeper, or vice versa.

You may also provide a static JAAS configuration file using the mechanisms described in the [Java SE Documentation](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html), whose entries will override those produced by the above configuration option.

##### 3. Configuring the component and/or connector to use Kerberos authentication

Finally, be sure to configure the connector within your Flink program or component as necessary to use Kerberos authentication.

Below is a list of currently first-class supported connectors or components by Flink for Kerberos authentication:

- Kafka: see [here]({{site.baseurl}}/dev/connectors/kafka.html#enabling-kerberos-authentication-for-versions-above-09-only) for details on configuring the Kafka connector to use Kerberos authentication.

- Zookeeper (for HA): see [here]({{site.baseurl}}/ops/jobmanager_high_availability.html#configuring-for-zookeeper-security) for details on Zookeeper security configuration to work with the Kerberos-based security configurations mentioned here.

For more information on how Flink security internally setups Kerberos authentication, please see [here]({{site.baseurl}}/ops/security-kerberos.html).

### Other

- `taskmanager.tmp.dirs`: The directory for temporary files, or a list of directories separated by the system's directory delimiter (for example ':' (colon) on Linux/Unix). If multiple directories are specified, then the temporary files will be distributed across the directories in a round-robin fashion. The I/O manager component will spawn one reading and one writing thread per directory. A directory may be listed multiple times to have the I/O manager use multiple threads for it (for example if it is physically stored on a very fast disc or RAID) (DEFAULT: The system's tmp dir).

- `taskmanager.log.path`: The config parameter defining the taskmanager log file location

- `jobmanager.web.address`: Address of the JobManager's web interface (DEFAULT: anyLocalAddress()).

- `jobmanager.web.port`: Port of the JobManager's web interface (DEFAULT: 8081).

- `jobmanager.web.tmpdir`: This configuration parameter allows defining the Flink web directory to be used by the web interface. The web interface
will copy its static files into the directory. Also uploaded job jars are stored in the directory if not overridden. By default, the temporary directory is used.

- `jobmanager.web.upload.dir`: The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
will be used under the directory specified by jobmanager.web.tmpdir.

- `fs.overwrite-files`: Specifies whether file output writers should overwrite existing files by default. Set to *true* to overwrite by default, *false* otherwise. (DEFAULT: false)

- `fs.output.always-create-directory`: File writers running with a parallelism larger than one create a directory for the output file path and put the different result files (one per parallel writer task) into that directory. If this option is set to *true*, writers with a parallelism of 1 will also create a directory and place a single result file into it. If the option is set to *false*, the writer will directly create the file directly at the output path, without creating a containing directory. (DEFAULT: false)

- `taskmanager.network.memory.fraction`: Fraction of JVM memory to use for network buffers. This determines how many streaming data exchange channels a TaskManager can have at the same time and how well buffered the channels are. If a job is rejected or you get a warning that the system has not enough buffers available, increase this value or the min/max values below. (DEFAULT: 0.1)

- `taskmanager.network.memory.min`: Minimum memory size for network buffers in bytes (DEFAULT: 64 MB)

- `taskmanager.network.memory.max`: Maximum memory size for network buffers in bytes (DEFAULT: 1 GB)

- `state.backend`: The backend that will be used to store operator state checkpoints if checkpointing is enabled. Supported backends:
   -  `jobmanager`: In-memory state, backup to JobManager's/ZooKeeper's memory. Should be used only for minimal state (Kafka offsets) or testing and local debugging.
   -  `filesystem`: State is in-memory on the TaskManagers, and state snapshots are stored in a file system. Supported are all filesystems supported by Flink, for example HDFS, S3, ...

- `state.backend.fs.checkpointdir`: Directory for storing checkpoints in a Flink supported filesystem. Note: State backend must be accessible from the JobManager, use `file://` only for local setups.

- `state.backend.rocksdb.checkpointdir`:  The local directory for storing RocksDB files, or a list of directories separated by the systems directory delimiter (for example ':' (colon) on Linux/Unix). (DEFAULT value is `taskmanager.tmp.dirs`)

- `state.checkpoints.dir`: The target directory for meta data of [externalized checkpoints]({{ site.baseurl }}/ops/state/checkpoints.html#externalized-checkpoints).

- `state.checkpoints.num-retained`: The number of completed checkpoint instances to retain. Having more than one allows recovery fallback to an earlier checkpoints if the latest checkpoint is corrupt. (Default: 1)

- `high-availability.zookeeper.storageDir`: Required for HA. Directory for storing JobManager metadata; this is persisted in the state backend and only a pointer to this state is stored in ZooKeeper. Exactly like the checkpoint directory it must be accessible from the JobManager and a local filesystem should only be used for local deployments. Previously this key was named `recovery.zookeeper.storageDir`.

- `blob.storage.directory`: Directory for storing blobs (such as user JARs) on the TaskManagers.
If not set or empty, Flink will fall back to `taskmanager.tmp.dirs` and select one temp directory
at random.

- `blob.service.cleanup.interval`: Cleanup interval (in seconds) of transient blobs at server and caches as well as permanent blobs at the caches (DEFAULT: 1 hour).
Whenever a job is not referenced at the cache anymore, we set a TTL for its permanent blob files and
let the periodic cleanup task (executed every `blob.service.cleanup.interval` seconds) remove them
after this TTL has passed. We do the same for transient blob files at both server and caches but
immediately after accessing them, i.e. an put or get operation.
This means that a blob will be retained at most <tt>2 * `blob.service.cleanup.interval`</tt> seconds after
not being referenced anymore (permanent blobs) or their last access (transient blobs). For permanent blobs,
this means that a recovery still has the chance to use existing files rather downloading them again.

- `blob.server.port`: Port definition for the blob server (serving user JARs) on the TaskManagers. By default the port is set to 0, which means that the operating system is picking an ephemeral port. Flink also accepts a list of ports ("50100,50101"), ranges ("50100-50200") or a combination of both. It is recommended to set a range of ports to avoid collisions when multiple JobManagers are running on the same machine.

- `blob.service.ssl.enabled`: Flag to enable ssl for the blob client/server communication. This is applicable only when the global ssl flag security.ssl.enabled is set to true (DEFAULT: true).

- `restart-strategy`: Default [restart strategy]({{site.baseurl}}/dev/restart_strategies.html) to use in case no
restart strategy has been specified for the job.
The options are:
    - fixed delay strategy: `fixed-delay`.
    - failure rate strategy: `failure-rate`.
    - no restarts: `none`

    Default value is `none` unless checkpointing is enabled for the job in which case the default is `fixed-delay` with `Integer.MAX_VALUE` restart attempts and `10s` delay.

- `restart-strategy.fixed-delay.attempts`: Number of restart attempts, used if the default restart strategy is set to "fixed-delay".
Default value is 1, unless "fixed-delay" was activated by enabling checkpoints, in which case the default is `Integer.MAX_VALUE`.

- `restart-strategy.fixed-delay.delay`: Delay between restart attempts, used if the default restart strategy is set to "fixed-delay". (default: `1 s`)

- `restart-strategy.failure-rate.max-failures-per-interval`: Maximum number of restarts in given time interval before failing a job in "failure-rate" strategy.
Default value is 1.

- `restart-strategy.failure-rate.failure-rate-interval`: Time interval for measuring failure rate in "failure-rate" strategy.
Default value is `1 minute`.

- `restart-strategy.failure-rate.delay`: Delay between restart attempts, used if the default restart strategy is set to "failure-rate".
Default value is the `akka.ask.timeout`.

- `jobstore.cache-size`: The job store cache size in bytes which is used to keep completed jobs in memory (DEFAULT: `52428800` (`50` MB)).

- `jobstore.expiration-time`: The time in seconds after which a completed job expires and is purged from the job store (DEFAULT: `3600`).

## Full Reference

### HDFS

<div class="alert alert-warning">
  <strong>Note:</strong> These keys are deprecated and it is recommended to configure the Hadoop path with the environment variable <code>HADOOP_CONF_DIR</code> instead.
</div>

These parameters configure the default HDFS used by Flink. Setups that do not specify a HDFS configuration have to specify the full path to HDFS files (`hdfs://address:port/path/to/files`) Files will also be written with default HDFS parameters (block size, replication factor).

- `fs.hdfs.hadoopconf`: The absolute path to the Hadoop File System's (HDFS) configuration **directory** (OPTIONAL VALUE). Specifying this value allows programs to reference HDFS files using short URIs (`hdfs:///path/to/files`, without including the address and port of the NameNode in the file URI). Without this option, HDFS files can be accessed, but require fully qualified URIs like `hdfs://address:port/path/to/files`. This option also causes file writers to pick up the HDFS's default values for block sizes and replication factors. Flink will look for the "core-site.xml" and "hdfs-site.xml" files in the specified directory.

- `fs.hdfs.hdfsdefault`: The absolute path of Hadoop's own configuration file "hdfs-default.xml" (DEFAULT: null).

- `fs.hdfs.hdfssite`: The absolute path of Hadoop's own configuration file "hdfs-site.xml" (DEFAULT: null).

### Core

{% include generated/core_configuration.html %}

### JobManager

{% include generated/job_manager_configuration.html %}

### TaskManager

{% include generated/task_manager_configuration.html %}

### Distributed Coordination (via Akka)

{% include generated/akka_configuration.html %}

### REST

{% include generated/rest_configuration.html %}

### Blob Server

{% include generated/blob_server_configuration.html %}

### Heartbeat Manager

{% include generated/heartbeat_manager_configuration.html %}

### SSL Settings

{% include generated/security_configuration.html %}

### Network communication (via Netty)

These parameters allow for advanced tuning. The default values are sufficient when running concurrent high-throughput jobs on a large cluster.

{% include generated/netty_configuration.html %}

### Web Frontend

{% include generated/web_configuration.html %}

### File Systems

{% include generated/file_system_configuration.html %}

### Compiler/Optimizer

{% include generated/optimizer_configuration.html %}

### Runtime Algorithms

{% include generated/algorithm_configuration.html %}

### Resource Manager

The configuration keys in this section are independent of the used resource management framework (YARN, Mesos, Standalone, ...)

{% include generated/resource_manager_configuration.html %}

### YARN

{% include generated/yarn_config_configuration.html %}

### Mesos

{% include generated/mesos_configuration.html %}

#### Mesos TaskManager

{% include generated/mesos_task_manager_configuration.html %}

### High Availability (HA)

{% include generated/high_availability_configuration.html %}

#### ZooKeeper-based HA Mode

{% include generated/high_availability_zookeeper_configuration.html %}

### ZooKeeper Security

{% include generated/zoo_keeper_configuration.html %}

### Kerberos-based Security

{% include generated/kerberos_configuration.html %}

### Environment

{% include generated/environment_configuration.html %}

### Checkpointing

{% include generated/checkpointing_configuration.html %}

### Queryable State

{% include generated/queryable_state_configuration.html %}

### Metrics

{% include generated/metric_configuration.html %}

### History Server

You have to configure `jobmanager.archive.fs.dir` in order to archive terminated jobs and add it to the list of monitored directories via `historyserver.archive.fs.dir` if you want to display them via the HistoryServer's web frontend.

- `jobmanager.archive.fs.dir`: Directory to upload information about terminated jobs to. You have to add this directory to the list of monitored directories of the history server via `historyserver.archive.fs.dir`.

{% include generated/history_server_configuration.html %}

### Slot Manager

The configuration keys in this section are relevant for the SlotManager running in the ResourceManager

{% include generated/slot_manager_configuration.html %}

## Legacy

- `mode`: Execution mode of Flink. Possible values are `legacy` and `new`. In order to start the legacy components, you have to specify `legacy` (DEFAULT: `new`).

## Background


### Configuring the Network Buffers

If you ever see the Exception `java.io.IOException: Insufficient number of network buffers`, you
need to adapt the amount of memory used for network buffers in order for your program to run on your
task managers.

Network buffers are a critical resource for the communication layers. They are used to buffer
records before transmission over a network, and to buffer incoming data before dissecting it into
records and handing them to the application. A sufficient number of network buffers is critical to
achieve a good throughput.

<div class="alert alert-info">
Since Flink 1.3, you may follow the idiom "more is better" without any penalty on the latency (we
prevent excessive buffering in each outgoing and incoming channel, i.e. *buffer bloat*, by limiting
the actual number of buffers used by each channel).
</div>

In general, configure the task manager to have enough buffers that each logical network connection
you expect to be open at the same time has a dedicated buffer. A logical network connection exists
for each point-to-point exchange of data over the network, which typically happens at
repartitioning or broadcasting steps (shuffle phase). In those, each parallel task inside the
TaskManager has to be able to talk to all other parallel tasks.

<div class="alert alert-warning">
  <strong>Note:</strong> Since Flink 1.5, network buffers will always be allocated off-heap, i.e. outside of the JVM heap, irrespective of the value of <code>taskmanager.memory.off-heap</code>. This way, we can pass these buffers directly to the underlying network stack layers.
</div>

#### Setting Memory Fractions

Previously, the number of network buffers was set manually which became a quite error-prone task
(see below). Since Flink 1.3, it is possible to define a fraction of memory that is being used for
network buffers with the following configuration parameters:

- `taskmanager.network.memory.fraction`: Fraction of JVM memory to use for network buffers (DEFAULT: 0.1),
- `taskmanager.network.memory.min`: Minimum memory size for network buffers in bytes (DEFAULT: 64 MB),
- `taskmanager.network.memory.max`: Maximum memory size for network buffers in bytes (DEFAULT: 1 GB), and
- `taskmanager.memory.segment-size`: Size of memory buffers used by the memory manager and the
network stack in bytes (DEFAULT: 32768 (= 32 KiBytes)).

#### Setting the Number of Network Buffers directly

<div class="alert alert-warning">
  <strong>Note:</strong> This way of configuring the amount of memory used for network buffers is deprecated. Please consider using the method above by defining a fraction of memory to use.
</div>

The required number of buffers on a task manager is
*total-degree-of-parallelism* (number of targets) \* *intra-node-parallelism* (number of sources in one task manager) \* *n*
with *n* being a constant that defines how many repartitioning-/broadcasting steps you expect to be
active at the same time. Since the *intra-node-parallelism* is typically the number of cores, and
more than 4 repartitioning or broadcasting channels are rarely active in parallel, it frequently
boils down to

{% highlight plain %}
#slots-per-TM^2 * #TMs * 4
{% endhighlight %}

Where `#slots per TM` are the [number of slots per TaskManager](#configuring-taskmanager-processing-slots) and `#TMs` are the total number of task managers.

To support, for example, a cluster of 20 8-slot machines, you should use roughly 5000 network
buffers for optimal throughput.

Each network buffer has by default a size of 32 KiBytes. In the example above, the system would thus
allocate roughly 300 MiBytes for network buffers.

The number and size of network buffers can be configured with the following parameters:

- `taskmanager.network.numberOfBuffers`, and
- `taskmanager.memory.segment-size`.

### Configuring Temporary I/O Directories

Although Flink aims to process as much data in main memory as possible, it is not uncommon that more data needs to be processed than memory is available. Flink's runtime is designed to write temporary data to disk to handle these situations.

The `taskmanager.tmp.dirs` parameter specifies a list of directories into which Flink writes temporary files. The paths of the directories need to be separated by ':' (colon character). Flink will concurrently write (or read) one temporary file to (from) each configured directory. This way, temporary I/O can be evenly distributed over multiple independent I/O devices such as hard disks to improve performance. To leverage fast I/O devices (e.g., SSD, RAID, NAS), it is possible to specify a directory multiple times.

If the `taskmanager.tmp.dirs` parameter is not explicitly specified, Flink writes temporary data to the temporary directory of the operating system, such as */tmp* in Linux systems.

### Configuring TaskManager processing slots

Flink executes a program in parallel by splitting it into subtasks and scheduling these subtasks to processing slots.

Each Flink TaskManager provides processing slots in the cluster. The number of slots is typically proportional to the number of available CPU cores __of each__ TaskManager. As a general recommendation, the number of available CPU cores is a good default for `taskmanager.numberOfTaskSlots`.

When starting a Flink application, users can supply the default number of slots to use for that job. The command line value therefore is called `-p` (for parallelism). In addition, it is possible to [set the number of slots in the programming APIs]({{site.baseurl}}/dev/parallel.html) for the whole application and for individual operators.

<img src="{{ site.baseurl }}/fig/slots_parallelism.svg" class="img-responsive" />

{% top %}
