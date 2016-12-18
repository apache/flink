---
title: "Configuration"
nav-id: "config"
nav-parent_id: setup
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

- `env.java.opts`: Set custom JVM options. This value is respected by Flink's start scripts, both JobManager and TaskManager, and Flink's YARN client. This can be used to set different garbage collectors or to include remote debuggers into the JVMs running Flink's services. Use `env.java.opts.jobmanager` and `env.java.opts.taskmanager` for JobManager or TaskManager-specific options, respectively.

- `env.java.opts.jobmanager`: JobManager-specific JVM options. These are used in addition to the regular `env.java.opts`. This configuration option is ignored by the YARN client.

- `env.java.opts.taskmanager`: TaskManager-specific JVM options. These are used in addition to the regular `env.java.opts`. This configuration option is ignored by the YARN client.

- `jobmanager.rpc.address`: The external address of the JobManager, which is the master/coordinator of the distributed system (DEFAULT: localhost). **Note:** The address (host name or IP) should be accessible by all nodes including the client.

- `jobmanager.rpc.port`: The port number of the JobManager (DEFAULT: 6123).

- `jobmanager.heap.mb`: JVM heap size (in megabytes) for the JobManager. You may have to increase the heap size for the JobManager if you are running very large applications (with many operators), or if you are keeping a long history of them.

- `taskmanager.heap.mb`: JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of the system. In contrast to Hadoop, Flink runs operators (e.g., join, aggregate) and user-defined functions (e.g., Map, Reduce, CoGroup) inside the TaskManager (including sorting/hashing/caching), so this value should be as large as possible. If the cluster is exclusively running Flink, the total amount of available memory per machine minus some memory for the operating system (maybe 1-2 GB) is a good value. On YARN setups, this value is automatically configured to the size of the TaskManager's YARN container, minus a certain tolerance value.

- `taskmanager.numberOfTaskSlots`: The number of parallel operator or user function instances that a single TaskManager can run (DEFAULT: 1). If this value is larger than 1, a single TaskManager takes multiple instances of a function or operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the available memory is divided between the different operator or function instances. This value is typically proportional to the number of physical CPU cores that the TaskManager's machine has (e.g., equal to the number of cores, or half the number of cores). [More about task slots](config.html#configuring-taskmanager-processing-slots).

- `parallelism.default`: The default parallelism to use for programs that have no parallelism specified. (DEFAULT: 1). For setups that have no concurrent jobs running, setting this value to NumTaskManagers * NumSlotsPerTaskManager will cause the system to use all available execution resources for the program's execution. **Note**: The default parallelism can be overwriten for an entire job by calling `setParallelism(int parallelism)` on the `ExecutionEnvironment` or by passing `-p <parallelism>` to the Flink Command-line frontend. It can be overwritten for single transformations by calling `setParallelism(int
parallelism)` on an operator. See [Parallel Execution]({{site.baseurl}}/dev/parallel) for more information about parallelism.

- `fs.default-scheme`: The default filesystem scheme to be used, with the necessary authority to contact, e.g. the host:port of the NameNode in the case of HDFS (if needed).
By default, this is set to `file:///` which points to the local filesystem. This means that the local
filesystem is going to be used to search for user-specified files **without** an explicit scheme
definition. As another example, if this is set to `hdfs://localhost:9000/`, then a user-specified file path
without explicit scheme definition, such as `/user/USERNAME/in.txt`, is going to be transformed into
`hdfs://localhost:9000/user/USERNAME/in.txt`. This scheme is used **ONLY** if no other scheme is specified (explicitly) in the user-provided `URI`.

- `fs.hdfs.hadoopconf`: The absolute path to the Hadoop File System's (HDFS) configuration **directory** (OPTIONAL VALUE). Specifying this value allows programs to reference HDFS files using short URIs (`hdfs:///path/to/files`, without including the address and port of the NameNode in the file URI). Without this option, HDFS files can be accessed, but require fully qualified URIs like `hdfs://address:port/path/to/files`. This option also causes file writers to pick up the HDFS's default values for block sizes and replication factors. Flink will look for the "core-site.xml" and "hdfs-site.xml" files in the specified directory.

## Advanced Options

### Managed Memory

By default, Flink allocates a fraction of `0.7` of the total memory configured via `taskmanager.heap.mb` for its managed memory. Managed memory helps Flink to run the batch operators efficiently. It prevents `OutOfMemoryException`s because Flink knows how much memory it can use to execute operations. If Flink runs out of managed memory, it utilizes disk space. Using managed memory, some operations can be performed directly on the raw data without having to deserialize the data to convert it into Java objects. All in all, managed memory improves the robustness and speed of the system.

The default fraction for managed memory can be adjusted using the `taskmanager.memory.fraction` parameter. An absolute value may be set using `taskmanager.memory.size` (overrides the fraction parameter). If desired, the managed memory may be allocated outside the JVM heap. This may improve performance in setups with large memory sizes.

- `taskmanager.memory.size`: The amount of memory (in megabytes) that the task manager reserves on-heap or off-heap (depending on `taskmanager.memory.off-heap`) for sorting, hash tables, and caching of intermediate results. If unspecified (-1), the memory manager will take a fixed ratio with respect to the size of the task manager JVM as specified by `taskmanager.memory.fraction`. (DEFAULT: -1)

- `taskmanager.memory.fraction`: The relative amount of memory (with respect to `taskmanager.heap.mb`) that the task manager reserves for sorting, hash tables, and caching of intermediate results. For example, a value of `0.8` means that a task manager reserves 80% of its memory (on-heap or off-heap depending on `taskmanager.memory.off-heap`) for internal data buffers, leaving 20% of free memory for the task manager's heap for objects created by user-defined functions. (DEFAULT: 0.7) This parameter is only evaluated, if `taskmanager.memory.size` is not set.

- `taskmanager.memory.off-heap`: If set to `true`, the task manager allocates memory which is used for sorting, hash tables, and caching of intermediate results outside of the JVM heap. For setups with larger quantities of memory, this can improve the efficiency of the operations performed on the memory (DEFAULT: false).

- `taskmanager.memory.segment-size`: The size of memory buffers used by the memory manager and the network stack in bytes (DEFAULT: 32768 (= 32 KiBytes)).

- `taskmanager.memory.preallocate`: Can be either of `true` or `false`. Specifies whether task managers should allocate all managed memory when starting up. (DEFAULT: false). When `taskmanager.memory.off-heap` is set to `true`, then it is advised that this configuration is also set to `true`.  If this configuration is set to `false` cleaning up of the allocated offheap memory happens only when the configured JVM parameter MaxDirectMemorySize is reached by triggering a full GC.

### Memory and Performance Debugging

These options are useful for debugging a Flink application for memory and garbage collection related issues, such as performance and out-of-memory process kills or exceptions.

- `taskmanager.debug.memory.startLogThread`: Causes the TaskManagers to periodically log memory and Garbage collection statistics. The statistics include current heap-, off-heap, and other memory pool utilization, as well as the time spent on garbage collection, by heap memory pool.

- `taskmanager.debug.memory.logIntervalMs`: The interval (in milliseconds) in which the TaskManagers log the memory and garbage collection statistics. Only has an effect, if `taskmanager.debug.memory.startLogThread` is set to true.

### Kerberos

Flink supports Kerberos authentication for the following services

+ Hadoop Components: such as HDFS, YARN, or HBase.
+ Kafka Connectors (version 0.9+)
+ Zookeeper Server/Client

Hadoop components relies on the UserGroupInformation (UGI) implementation to handle Kerberos authentication, whereas Kafka and Zookeeper services handles Kerberos authentication through SASL/JAAS implementation.

**Kerberos is only properly supported in Hadoop version 2.6.1 and above. All
  other versions have critical bugs which might fail the Flink job
  unexpectedly.**

**Ticket cache** and **Keytab** modes are supported for all above mentioned services.

> Ticket cache (Supported only to provide backward compatibility support. Keytab is the preferred approach for long running jobs)

While Hadoop uses Kerberos tickets to authenticate users with services initially, the authentication process continues differently afterwards. Instead of saving the ticket to authenticate on a later access, Hadoop creates its own security tokens (DelegationToken) that it passes around. These are authenticated to Kerberos periodically but are independent of the token renewal time. The tokens have a maximum life span identical to the Kerberos ticket maximum life span.

While using ticket cache mode, please make sure to set the maximum ticket life span high long running jobs.

If you are on YARN, then it is sufficient to authenticate the client with Kerberos. On a Flink standalone cluster you need to ensure that, initially, all nodes are authenticated with Kerberos using the `kinit` tool.

> Keytab (security principal and keytab can be configured through Flink configuration file)
- `security.keytab`: Path to Keytab file
- `security.principal`: Principal associated with the keytab

Kerberos ticket renewal is abstracted and automatically handled by the Hadoop/Kafka/ZK login modules and ensures that tickets are renewed in time and you can be sure to be authenticated until the end of the ticket life time.

For Kafka and ZK, process-wide JAAS config will be created using the provided security credentials and the Kerberos authentication will be handled by Kafka/ZK login handlers.

### Other

- `taskmanager.tmp.dirs`: The directory for temporary files, or a list of directories separated by the systems directory delimiter (for example ':' (colon) on Linux/Unix). If multiple directories are specified, then the temporary files will be distributed across the directories in a round-robin fashion. The I/O manager component will spawn one reading and one writing thread per directory. A directory may be listed multiple times to have the I/O manager use multiple threads for it (for example if it is physically stored on a very fast disc or RAID) (DEFAULT: The system's tmp dir).

- `taskmanager.log.path`: The config parameter defining the taskmanager log file location

- `jobmanager.web.address`: Address of the JobManager's web interface (DEFAULT: anyLocalAddress()).

- `jobmanager.web.port`: Port of the JobManager's web interface (DEFAULT: 8081).

- `jobmanager.web.tmpdir`: This configuration parameter allows defining the Flink web directory to be used by the web interface. The web interface
will copy its static files into the directory. Also uploaded job jars are stored in the directory if not overridden. By default, the temporary directory is used.

- `jobmanager.web.upload.dir`: The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
will be used under the directory specified by jobmanager.web.tmpdir.

- `fs.overwrite-files`: Specifies whether file output writers should overwrite existing files by default. Set to *true* to overwrite by default, *false* otherwise. (DEFAULT: false)

- `fs.output.always-create-directory`: File writers running with a parallelism larger than one create a directory for the output file path and put the different result files (one per parallel writer task) into that directory. If this option is set to *true*, writers with a parallelism of 1 will also create a directory and place a single result file into it. If the option is set to *false*, the writer will directly create the file directly at the output path, without creating a containing directory. (DEFAULT: false)

- `taskmanager.network.numberOfBuffers`: The number of buffers available to the network stack. This number determines how many streaming data exchange channels a TaskManager can have at the same time and how well buffered the channels are. If a job is rejected or you get a warning that the system has not enough buffers available, increase this value (DEFAULT: 2048).

- `state.backend`: The backend that will be used to store operator state checkpoints if checkpointing is enabled. Supported backends:
   -  `jobmanager`: In-memory state, backup to JobManager's/ZooKeeper's memory. Should be used only for minimal state (Kafka offsets) or testing and local debugging.
   -  `filesystem`: State is in-memory on the TaskManagers, and state snapshots are stored in a file system. Supported are all filesystems supported by Flink, for example HDFS, S3, ...

- `state.backend.fs.checkpointdir`: Directory for storing checkpoints in a Flink supported filesystem. Note: State backend must be accessible from the JobManager, use `file://` only for local setups.

- `state.backend.rocksdb.checkpointdir`:  The local directory for storing RocksDB files, or a list of directories separated by the systems directory delimiter (for example ‘:’ (colon) on Linux/Unix). (DEFAULT value is `taskmanager.tmp.dirs`)

- `state.checkpoints.dir`: The target directory for meta data of [externalized checkpoints]({{ site.baseurl }}/setup/fault_tolerance.md#externalized-checkpoints).

- `high-availability.zookeeper.storageDir`: Required for HA. Directory for storing JobManager metadata; this is persisted in the state backend and only a pointer to this state is stored in ZooKeeper. Exactly like the checkpoint directory it must be accessible from the JobManager and a local filesystem should only be used for local deployments. Previously this key was named `recovery.zookeeper.storageDir`.

- `blob.storage.directory`: Directory for storing blobs (such as user jar's) on the TaskManagers.

- `blob.server.port`: Port definition for the blob server (serving user jar's) on the Taskmanagers. By default the port is set to 0, which means that the operating system is picking an ephemeral port. Flink also accepts a list of ports ("50100,50101"), ranges ("50100-50200") or a combination of both. It is recommended to set a range of ports to avoid collisions when multiple JobManagers are running on the same machine.

- `blob.service.ssl.enabled`: Flag to enable ssl for the blob client/server communication. This is applicable only when the global ssl flag security.ssl.enabled is set to true (DEFAULT: true).

- `restart-strategy`: Default restart strategy to use in case that no restart strategy has been specified for the submitted job.
Currently, it can be chosen from fixed delay restart strategy, failure rate restart strategy or no restart strategy.
To use the fixed delay strategy you have to specify "fixed-delay".
To use the failure rate strategy you have to specify "failure-rate".
To turn the restart behaviour off you have to specify "none".
Default value "none".

- `restart-strategy.fixed-delay.attempts`: Number of restart attempts, used if the default restart strategy is set to "fixed-delay".
Default value is 1.

- `restart-strategy.fixed-delay.delay`: Delay between restart attempts, used if the default restart strategy is set to "fixed-delay".
Default value is the `akka.ask.timeout`.

- `restart-strategy.failure-rate.max-failures-per-interval`: Maximum number of restarts in given time interval before failing a job in "failure-rate" strategy.
Default value is 1.

- `restart-strategy.failure-rate.failure-rate-interval`: Time interval for measuring failure rate in "failure-rate" strategy.
Default value is `1 minute`.

- `restart-strategy.failure-rate.delay`: Delay between restart attempts, used if the default restart strategy is set to "failure-rate".
Default value is the `akka.ask.timeout`.

## Full Reference

### HDFS

These parameters configure the default HDFS used by Flink. Setups that do not specify a HDFS configuration have to specify the full path to HDFS files (`hdfs://address:port/path/to/files`) Files will also be written with default HDFS parameters (block size, replication factor).

- `fs.hdfs.hadoopconf`: The absolute path to the Hadoop configuration directory. The system will look for the "core-site.xml" and "hdfs-site.xml" files in that directory (DEFAULT: null).

- `fs.hdfs.hdfsdefault`: The absolute path of Hadoop's own configuration file "hdfs-default.xml" (DEFAULT: null).

- `fs.hdfs.hdfssite`: The absolute path of Hadoop's own configuration file "hdfs-site.xml" (DEFAULT: null).

### JobManager &amp; TaskManager

The following parameters configure Flink's JobManager and TaskManagers.

- `jobmanager.rpc.address`: The external address of the JobManager, which is the master/coordinator of the distributed system (DEFAULT: **localhost**). **Note:** The address (host name or IP) should be accessible by all nodes including the client.

- `jobmanager.rpc.port`: The port number of the JobManager (DEFAULT: **6123**).

- `taskmanager.hostname`: The hostname of the network interface that the TaskManager binds to. By default, the TaskManager searches for network interfaces that can connect to the JobManager and other TaskManagers. This option can be used to define a hostname if that strategy fails for some reason. Because different TaskManagers need different values for this option, it usually is specified in an additional non-shared TaskManager-specific config file.

- `taskmanager.rpc.port`: The task manager's IPC port (DEFAULT: **0**, which lets the OS choose a free port).

- `taskmanager.data.port`: The task manager's port used for data exchange operations (DEFAULT: **0**, which lets the OS choose a free port).

- `taskmanager.data.ssl.enabled`: Enable SSL support for the taskmanager data transport. This is applicable only when the global ssl flag security.ssl.enabled is set to true (DEFAULT: **true**)

- `jobmanager.heap.mb`: JVM heap size (in megabytes) for the JobManager (DEFAULT: **256**).

- `taskmanager.heap.mb`: JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of the system. In contrast to Hadoop, Flink runs operators (e.g., join, aggregate) and user-defined functions (e.g., Map, Reduce, CoGroup) inside the TaskManager (including sorting/hashing/caching), so this value should be as large as possible (DEFAULT: **512**). On YARN setups, this value is automatically configured to the size of the TaskManager's YARN container, minus a certain tolerance value.

- `taskmanager.numberOfTaskSlots`: The number of parallel operator or user function instances that a single TaskManager can run (DEFAULT: **1**). If this value is larger than 1, a single TaskManager takes multiple instances of a function or operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the available memory is divided between the different operator or function instances. This value is typically proportional to the number of physical CPU cores that the TaskManager's machine has (e.g., equal to the number of cores, or half the number of cores).

- `taskmanager.tmp.dirs`: The directory for temporary files, or a list of directories separated by the systems directory delimiter (for example ':' (colon) on Linux/Unix). If multiple directories are specified, then the temporary files will be distributed across the directories in a round robin fashion. The I/O manager component will spawn one reading and one writing thread per directory. A directory may be listed multiple times to have the I/O manager use multiple threads for it (for example if it is physically stored on a very fast disc or RAID) (DEFAULT: **The system's tmp dir**).

- `taskmanager.network.numberOfBuffers`: The number of buffers available to the network stack. This number determines how many streaming data exchange channels a TaskManager can have at the same time and how well buffered the channels are. If a job is rejected or you get a warning that the system has not enough buffers available, increase this value (DEFAULT: **2048**).

- `taskmanager.memory.size`: The amount of memory (in megabytes) that the task manager reserves on the JVM's heap space for sorting, hash tables, and caching of intermediate results. If unspecified (-1), the memory manager will take a fixed ratio of the heap memory available to the JVM, as specified by `taskmanager.memory.fraction`. (DEFAULT: **-1**)

- `taskmanager.memory.fraction`: The relative amount of memory that the task manager reserves for sorting, hash tables, and caching of intermediate results. For example, a value of 0.8 means that TaskManagers reserve 80% of the JVM's heap space for internal data buffers, leaving 20% of the JVM's heap space free for objects created by user-defined functions. (DEFAULT: **0.7**) This parameter is only evaluated, if `taskmanager.memory.size` is not set.

- `taskmanager.debug.memory.startLogThread`: Causes the TaskManagers to periodically log memory and Garbage collection statistics. The statistics include current heap-, off-heap, and other memory pool utilization, as well as the time spent on garbage collection, by heap memory pool.

- `taskmanager.debug.memory.logIntervalMs`: The interval (in milliseconds) in which the TaskManagers log the memory and garbage collection statistics. Only has an effect, if `taskmanager.debug.memory.startLogThread` is set to true.

- `taskmanager.maxRegistrationDuration`: Defines the maximum time it can take for the TaskManager registration. If the duration is exceeded without a successful registration, then the TaskManager terminates. The max registration duration requires a time unit specifier (ms/s/min/h/d) (e.g. "10 min"). (DEFAULT: **Inf**)

- `taskmanager.initial-registration-pause`: The initial registration pause between two consecutive registration attempts. The pause is doubled for each new registration attempt until it reaches the maximum registration pause. The initial registration pause requires a time unit specifier (ms/s/min/h/d) (e.g. "5 s"). (DEFAULT: **500 ms**)

- `taskmanager.max-registration-pause`: The maximum registration pause between two consecutive registration attempts. The max registration pause requires a time unit specifier (ms/s/min/h/d) (e.g. "5 s"). (DEFAULT: **30 s**)

- `taskmanager.refused-registration-pause`: The pause after a registration has been refused by the job manager before retrying to connect. The refused registration pause requires a time unit specifier (ms/s/min/h/d) (e.g. "5 s"). (DEFAULT: **10 s**)

- `blob.fetch.retries`: The number of retries for the TaskManager to download BLOBs (such as JAR files) from the JobManager (DEFAULT: **50**).

- `blob.fetch.num-concurrent`: The number concurrent BLOB fetches (such as JAR file downloads) that the JobManager serves (DEFAULT: **50**).

- `blob.fetch.backlog`: The maximum number of queued BLOB fetches (such as JAR file downloads) that the JobManager allows (DEFAULT: **1000**).

- `task.cancellation-interval`: Time interval between two successive task cancellation attempts in milliseconds (DEFAULT: **30000**).

### Distributed Coordination (via Akka)

- `akka.ask.timeout`: Timeout used for all futures and blocking Akka calls. If Flink fails due to timeouts then you should try to increase this value. Timeouts can be caused by slow machines or a congested network. The timeout value requires a time-unit specifier (ms/s/min/h/d) (DEFAULT: **10 s**).

- `akka.lookup.timeout`: Timeout used for the lookup of the JobManager. The timeout value has to contain a time-unit specifier (ms/s/min/h/d) (DEFAULT: **10 s**).

- `akka.framesize`: Maximum size of messages which are sent between the JobManager and the TaskManagers. If Flink fails because messages exceed this limit, then you should increase it. The message size requires a size-unit specifier (DEFAULT: **10485760b**).

- `akka.watch.heartbeat.interval`: Heartbeat interval for Akka's DeathWatch mechanism to detect dead TaskManagers. If TaskManagers are wrongly marked dead because of lost or delayed heartbeat messages, then you should increase this value. A thorough description of Akka's DeathWatch can be found [here](http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector) (DEFAULT: **10 s**).

- `akka.watch.heartbeat.pause`: Acceptable heartbeat pause for Akka's DeathWatch mechanism. A low value does not allow a irregular heartbeat. A thorough description of Akka's DeathWatch can be found [here](http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector) (DEFAULT: **60 s**).

- `akka.watch.threshold`: Threshold for the DeathWatch failure detector. A low value is prone to false positives whereas a high value increases the time to detect a dead TaskManager. A thorough description of Akka's DeathWatch can be found [here](http://doc.akka.io/docs/akka/snapshot/scala/remoting.html#failure-detector) (DEFAULT: **12**).

- `akka.transport.heartbeat.interval`: Heartbeat interval for Akka's transport failure detector. Since Flink uses TCP, the detector is not necessary. Therefore, the detector is disabled by setting the interval to a very high value. In case you should need the transport failure detector, set the interval to some reasonable value. The interval value requires a time-unit specifier (ms/s/min/h/d) (DEFAULT: **1000 s**).

- `akka.transport.heartbeat.pause`: Acceptable heartbeat pause for Akka's transport failure detector. Since Flink uses TCP, the detector is not necessary. Therefore, the detector is disabled by setting the pause to a very high value. In case you should need the transport failure detector, set the pause to some reasonable value. The pause value requires a time-unit specifier (ms/s/min/h/d) (DEFAULT: **6000 s**).

- `akka.transport.threshold`: Threshold for the transport failure detector. Since Flink uses TCP, the detector is not necessary and, thus, the threshold is set to a high value (DEFAULT: **300**).

- `akka.tcp.timeout`: Timeout for all outbound connections. If you should experience problems with connecting to a TaskManager due to a slow network, you should increase this value (DEFAULT: **20 s**).

- `akka.throughput`: Number of messages that are processed in a batch before returning the thread to the pool. Low values denote a fair scheduling whereas high values can increase the performance at the cost of unfairness (DEFAULT: **15**).

- `akka.log.lifecycle.events`: Turns on the Akka's remote logging of events. Set this value to 'true' in case of debugging (DEFAULT: **false**).

- `akka.startup-timeout`: Timeout after which the startup of a remote component is considered being failed (DEFAULT: **akka.ask.timeout**).

- `akka.ssl.enabled`: Turns on SSL for Akka's remote communication. This is applicable only when the global ssl flag security.ssl.enabled is set to true (DEFAULT: **true**).

### SSL Settings

- `security.ssl.enabled`: Turns on SSL for internal network communication. This can be optionally overridden by flags defined in different transport modules (DEFAULT: **false**).

- `security.ssl.keystore`: The Java keystore file to be used by the flink endpoint for its SSL Key and Certificate.

- `security.ssl.keystore-password`: The secret to decrypt the keystore file.

- `security.ssl.key-password`: The secret to decrypt the server key in the keystore.

- `security.ssl.truststore`: The truststore file containing the public CA certificates to be used by flink endpoints to verify the peer's certificate.

- `security.ssl.truststore-password`: The secret to decrypt the truststore.

- `security.ssl.protocol`: The SSL protocol version to be supported for the ssl transport (DEFAULT: **TLSv1.2**).

- `security.ssl.algorithms`: The comma separated list of standard SSL algorithms to be supported. Read more [here](http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites) (DEFAULT: **TLS_RSA_WITH_AES_128_CBC_SHA**).

- `security.ssl.verify-hostname`: Flag to enable peer's hostname verification during ssl handshake (DEFAULT: **true**).

### Network communication (via Netty)

These parameters allow for advanced tuning. The default values are sufficient when running concurrent high-throughput jobs on a large cluster.

- `taskmanager.net.num-arenas`: The number of Netty arenas (DEFAULT: **taskmanager.numberOfTaskSlots**).

- `taskmanager.net.server.numThreads`: The number of Netty server threads (DEFAULT: **taskmanager.numberOfTaskSlots**).

- `taskmanager.net.client.numThreads`: The number of Netty client threads (DEFAULT: **taskmanager.numberOfTaskSlots**).

- `taskmanager.net.server.backlog`: The netty server connection backlog.

- `taskmanager.net.client.connectTimeoutSec`: The Netty client connection timeout (DEFAULT: **120 seconds**).

- `taskmanager.net.sendReceiveBufferSize`: The Netty send and receive buffer size. This defaults to the system buffer size (`cat /proc/sys/net/ipv4/tcp_[rw]mem`) and is 4 MiB in modern Linux.

- `taskmanager.net.transport`: The Netty transport type, either "nio" or "epoll" (DEFAULT: **nio**).

### JobManager Web Frontend

- `jobmanager.web.port`: Port of the JobManager's web interface that displays status of running jobs and execution time breakdowns of finished jobs (DEFAULT: 8081). Setting this value to `-1` disables the web frontend.

- `jobmanager.web.history`: The number of latest jobs that the JobManager's web front-end in its history (DEFAULT: 5).

- `jobmanager.web.checkpoints.disable`: Disables checkpoint statistics (DEFAULT: `false`).

- `jobmanager.web.checkpoints.history`: Number of checkpoint statistics to remember (DEFAULT: `10`).

- `jobmanager.web.backpressure.cleanup-interval`: Time after which cached stats are cleaned up if not accessed (DEFAULT: `600000`, 10 mins).

- `jobmanager.web.backpressure.refresh-interval`: Time after which available stats are deprecated and need to be refreshed (DEFAULT: `60000`, 1 min).

- `jobmanager.web.backpressure.num-samples`: Number of stack trace samples to take to determine back pressure (DEFAULT: `100`).

- `jobmanager.web.backpressure.delay-between-samples`: Delay between stack trace samples to determine back pressure (DEFAULT: `50`, 50 ms).

- `jobmanager.web.ssl.enabled`: Enable https access to the web frontend. This is applicable only when the global ssl flag security.ssl.enabled is set to true (DEFAULT: `true`).

### File Systems

The parameters define the behavior of tasks that create result files.

- `fs.default-scheme`: The default filesystem scheme to be used, with the necessary authority to contact, e.g. the host:port of the NameNode in the case of HDFS (if needed).
By default, this is set to `file:///` which points to the local filesystem. This means that the local
filesystem is going to be used to search for user-specified files **without** an explicit scheme
definition. This scheme is used **ONLY** if no other scheme is specified (explicitly) in the user-provided `URI`.

- `fs.overwrite-files`: Specifies whether file output writers should overwrite existing files by default. Set to *true* to overwrite by default, *false* otherwise. (DEFAULT: false)

- `fs.output.always-create-directory`: File writers running with a parallelism larger than one create a directory for the output file path and put the different result files (one per parallel writer task) into that directory. If this option is set to *true*, writers with a parallelism of 1 will also create a directory and place a single result file into it. If the option is set to *false*, the writer will directly create the file directly at the output path, without creating a containing directory. (DEFAULT: false)

### Compiler/Optimizer

- `compiler.delimited-informat.max-line-samples`: The maximum number of line samples taken by the compiler for delimited inputs. The samples are used to estimate the number of records. This value can be overridden for a specific input with the input format's parameters (DEFAULT: 10).

- `compiler.delimited-informat.min-line-samples`: The minimum number of line samples taken by the compiler for delimited inputs. The samples are used to estimate the number of records. This value can be overridden for a specific input with the input format's parameters (DEFAULT: 2).

- `compiler.delimited-informat.max-sample-len`: The maximal length of a line sample that the compiler takes for delimited inputs. If the length of a single sample exceeds this value (possible because of misconfiguration of the parser), the sampling aborts. This value can be overridden for a specific input with the input format's parameters (DEFAULT: 2097152 (= 2 MiBytes)).

### Runtime Algorithms

- `taskmanager.runtime.hashjoin-bloom-filters`: Flag to activate/deactivate bloom filters in the hybrid hash join implementation. In cases where the hash join needs to spill to disk (datasets larger than the reserved fraction of memory), these bloom filters can greatly reduce the number of spilled records, at the cost some CPU cycles. (DEFAULT: false)

- `taskmanager.runtime.max-fan`: The maximal fan-in for external merge joins and fan-out for spilling hash tables. Limits the number of file handles per operator, but may cause intermediate merging/partitioning, if set too small (DEFAULT: 128).

- `taskmanager.runtime.sort-spilling-threshold`: A sort operation starts spilling when this fraction of its memory budget is full (DEFAULT: 0.8).

### Resource Manager

The configuration keys in this section are independent of the used resource management framework (YARN, Mesos, Standalone, ...)

- `resourcemanager.rpc.port`: The config parameter defining the network port to connect to for communication with the resource manager. By default, the port
of the JobManager, because the same ActorSystem is used. Its not possible to use this configuration key to define port ranges.


## YARN

- `yarn.heap-cutoff-ratio`: (Default 0.25) Percentage of heap space to remove from containers started by YARN. When a user requests a certain amount of memory for each TaskManager container (for example 4 GB), we can not pass this amount as the maximum heap space for the JVM (`-Xmx` argument) because the JVM is also allocating memory outside the heap. YARN is very strict with killing containers which are using more memory than requested. Therefore, we remove a 15% of the memory from the requested heap as a safety margin.

- `yarn.heap-cutoff-min`: (Default 384 MB) Minimum amount of memory to cut off the requested heap size.

- `yarn.maximum-failed-containers` (Default: number of requested containers). Maximum number of containers the system is going to reallocate in case of a failure.

- `yarn.application-attempts` (Default: 1). Number of ApplicationMaster restarts. Note that that the entire Flink cluster will restart and the YARN Client will loose the connection. Also, the JobManager address will change and you'll need to set the JM host:port manually. It is recommended to leave this option at 1.

- `yarn.heartbeat-delay` (Default: 5 seconds). Time between heartbeats with the ResourceManager.

- `yarn.properties-file.location` (Default: temp directory). When a Flink job is submitted to YARN, the JobManager's host and the number of available processing slots is written into a properties file, so that the Flink client is able to pick those details up. This configuration parameter allows changing the default location of that file (for example for environments sharing a Flink installation between users)

- `yarn.containers.vcores` The number of virtual cores (vcores) per YARN container. By default, the number of `vcores` is set to the number of slots per TaskManager, if set, or to 1, otherwise.

- `yarn.application-master.env.`*ENV_VAR1=value* Configuration values prefixed with `yarn.application-master.env.` will be passed as environment variables to the ApplicationMaster/JobManager process. For example for passing `LD_LIBRARY_PATH` as an env variable to the ApplicationMaster, set:

- `yarn.application-master.env.LD_LIBRARY_PATH: "/usr/lib/native"`

- `yarn.containers.vcores` The number of virtual cores (vcores) per YARN container. By default, the number of `vcores` is set to the number of slots per TaskManager, if set, or to 1, otherwise.

- `yarn.taskmanager.env.` Similar to the configuration prefix about, this prefix allows setting custom environment variables for the TaskManager processes.

- `yarn.application-master.port` (Default: 0, which lets the OS choose an ephemeral port) With this configuration option, users can specify a port, a range of ports or a list of ports for the  Application Master (and JobManager) RPC port. By default we recommend using the default value (0) to let the operating system choose an appropriate port. In particular when multiple AMs are running on the  same physical host, fixed port assignments prevent the AM from starting.

  For example when running Flink on YARN on an environment with a restrictive firewall, this option allows specifying a range of allowed ports.


## High Availability (HA)

- `high-availability`: Defines the high availability mode used for the cluster execution. Currently, Flink supports the following modes:
  - `none` (default): No high availability. A single JobManager runs and no JobManager state is checkpointed.
  - `zookeeper`: Supports the execution of multiple JobManagers and JobManager state checkpointing. Among the group of JobManagers, ZooKeeper elects one of them as the leader which is responsible for the cluster execution. In case of a JobManager failure, a standby JobManager will be elected as the new leader and is given the last checkpointed JobManager state. In order to use the 'zookeeper' mode, it is mandatory to also define the `high-availability.zookeeper.quorum` configuration value.

Previously this key was named `recovery.mode` and the default value was `standalone`.

### ZooKeeper-based HA Mode

- `high-availability.zookeeper.quorum`: Defines the ZooKeeper quorum URL which is used to connect to the ZooKeeper cluster when the 'zookeeper' HA mode is selected. Previously this key was name `recovery.zookeeper.quorum`.

- `high-availability.zookeeper.path.root`: (Default `/flink`) Defines the root dir under which the ZooKeeper HA mode will create namespace directories. Previously this ket was named `recovery.zookeeper.path.root`.

- `high-availability.zookeeper.path.namespace`: (Default `/default_ns` in standalone cluster mode, or the <yarn-application-id> under YARN) Defines the subdirectory under the root dir where the ZooKeeper HA mode will create znodes. This allows to isolate multiple applications on the same ZooKeeper. Previously this key was named `recovery.zookeeper.path.namespace`.

- `high-availability.zookeeper.path.latch`: (Default `/leaderlatch`) Defines the znode of the leader latch which is used to elect the leader. Previously this key was named `recovery.zookeeper.path.latch`.

- `high-availability.zookeeper.path.leader`: (Default `/leader`) Defines the znode of the leader which contains the URL to the leader and the current leader session ID. Previously this key was named `recovery.zookeeper.path.leader`.

- `high-availability.zookeeper.storageDir`: Defines the directory in the state backend where the JobManager metadata will be stored (ZooKeeper only keeps pointers to it). Required for HA. Previously this key was named `recovery.zookeeper.storageDir`.

- `high-availability.zookeeper.client.session-timeout`: (Default `60000`) Defines the session timeout for the ZooKeeper session in ms. Previously this key was named `recovery.zookeeper.client.session-timeout`

- `high-availability.zookeeper.client.connection-timeout`: (Default `15000`) Defines the connection timeout for ZooKeeper in ms. Previously this key was named `recovery.zookeeper.client.connection-timeout`.

- `high-availability.zookeeper.client.retry-wait`: (Default `5000`) Defines the pause between consecutive retries in ms. Previously this key was named `recovery.zookeeper.client.retry-wait`.

- `high-availability.zookeeper.client.max-retry-attempts`: (Default `3`) Defines the number of connection retries before the client gives up. Previously this key was named `recovery.zookeeper.client.max-retry-attempts`.

- `high-availability.job.delay`: (Default `akka.ask.timeout`) Defines the delay before persisted jobs are recovered in case of a master recovery situation. Previously this key was named `recovery.job.delay`.

- `high-availability.zookeeper.client.acl`: (Default `open`) Defines the ACL (open|creator) to be configured on ZK node. The configuration value can be set to "creator" if the ZooKeeper server configuration has the "authProvider" property mapped to use SASLAuthenticationProvider and the cluster is configured to run in secure mode (Kerberos). The ACL options are based on https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_BuiltinACLSchemes

### ZooKeeper-Security

- `zookeeper.sasl.disable`: (Default: `true`) Defines if SASL based authentication needs to be enabled or disabled. The configuration value can be set to "true" if ZooKeeper cluster is running in secure mode (Kerberos)

- `zookeeper.sasl.service-name`: (Default: `zookeeper`) If the ZooKeeper server is configured with a different service name (default:"zookeeper") then it can be supplied using this configuration. A mismatch in service name between client and server configuration will cause the authentication to fail.

## Environment

- `env.log.dir`: (Defaults to the `log` directory under Flink's home) Defines the directory where the Flink logs are saved. It has to be an absolute path.

## Queryable State

### Server

- `query.server.enable`: Enable queryable state (Default: `true`).

- `query.server.port`: Port to bind queryable state server to (Default: `0`, binds to random port).

- `query.server.network-threads`: Number of network (Netty's event loop) Threads for queryable state server (Default: `0`, picks number of slots).

- `query.server.query-threads`: Number of query Threads for queryable state server (Default: `0`, picks number of slots).

### Client

- `query.client.network-threads`: Number of network (Netty's event loop) Threads for queryable state client (Default: `0`, picks number of available cores as returned by `Runtime.getRuntime().availableProcessors()`).

- `query.client.lookup.num-retries`: Number of retries on KvState lookup failure due to unavailable JobManager (Default: `3`).

- `query.client.lookup.retry-delay`: Retry delay in milliseconds on KvState lookup failure due to unavailable JobManager (Default: `1000`).

## Metrics

- `metrics.reporters`: The list of named reporters, i.e. "foo,bar".

- `metrics.reporter.<name>.<config>`: Generic setting `<config>` for the reporter named `<name>`.

- `metrics.reporter.<name>.class`: The reporter class to use for the reporter named `<name>`.

- `metrics.reporter.<name>.interval`: The reporter interval to use for the reporter named `<name>`.

- `metrics.scope.jm`: (Default: &lt;host&gt;.jobmanager) Defines the scope format string that is applied to all metrics scoped to a JobManager.

- `metrics.scope.jm.job`: (Default: &lt;host&gt;.jobmanager.&lt;job_name&gt;) Defines the scope format string that is applied to all metrics scoped to a job on a JobManager.

- `metrics.scope.tm`: (Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;) Defines the scope format string that is applied to all metrics scoped to a TaskManager.

- `metrics.scope.tm.job`: (Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;) Defines the scope format string that is applied to all metrics scoped to a job on a TaskManager.

- `metrics.scope.tm.task`: (Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;task_name&gt;.&lt;subtask_index&gt;) Defines the scope format string that is applied to all metrics scoped to a task.

- `metrics.scope.tm.operator`: (Default: &lt;host&gt;.taskmanager.&lt;tm_id&gt;.&lt;job_name&gt;.&lt;operator_name&gt;.&lt;subtask_index&gt;) Defines the scope format string that is applied to all metrics scoped to an operator.

- `metrics.latency.history-size`: (Default: 128) Defines the number of measured latencies to maintain at each operator


## Background

### Configuring the Network Buffers

If you ever see the Exception `java.io.IOException: Insufficient number of network buffers`, please use the following formula to adjust the number of network buffers:

```
#slots-per-TM^2 * #TMs * 4
```

Where `#slots per TM` are the [number of slots per TaskManager](#configuring-taskmanager-processing-slots) and `#TMs` are the total number of task managers.

Network buffers are a critical resource for the communication layers. They are used to buffer records before transmission over a network, and to buffer incoming data before dissecting it into records and handing them to the
application. A sufficient number of network buffers is critical to achieve a good throughput.

In general, configure the task manager to have enough buffers that each logical network connection you expect to be open at the same time has a dedicated buffer. A logical network connection exists for each point-to-point exchange of data over the network, which typically happens at repartitioning- or broadcasting steps (shuffle phase). In those, each parallel task inside the TaskManager has to be able to talk to all other parallel tasks. Hence, the required number of buffers on a task manager is *total-degree-of-parallelism* (number of targets) \* *intra-node-parallelism* (number of sources in one task manager) \* *n*. Here, *n* is a constant that defines how many repartitioning-/broadcasting steps you expect to be active at the same time.

Since the *intra-node-parallelism* is typically the number of cores, and more than 4 repartitioning or broadcasting channels are rarely active in parallel, it frequently boils down to `#slots-per-TM^2 * #TMs * 4`.

To support for example a cluster of 20 8-slot machines, you should use roughly 5000 network buffers for optimal throughput.

Each network buffer has by default a size of 32 KiBytes. In the above example, the system would allocate roughly 300 MiBytes for network buffers.

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

When starting a Flink application, users can supply the default number of slots to use for that job. The command line value therefore is called `-p` (for parallelism). In addition, it is possible to [set the number of slots in the programming APIs]({{site.baseurl}}/dev/parallel) for the whole application and for individual operators.

<img src="{{ site.baseurl }}/fig/slots_parallelism.svg" class="img-responsive" />
