---
title: "State Backends"
weight: 12
type: docs
aliases:
  - /ops/state/state_backends.html
  - /apis/streaming/state_backends.html
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

# State Backends

Programs written in the [Data Stream API]({{< ref "docs/dev/datastream/overview" >}}) often hold state in various forms:

- Windows gather elements or aggregates until they are triggered
- Transformation functions may use the key/value state interface to store values
- Transformation functions may implement the `CheckpointedFunction` interface to make their local variables fault tolerant

See also [state section]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}) in the streaming API guide.

When checkpointing is activated, such state is persisted upon checkpoints to guard against data loss and recover consistently.
How the state is represented internally, and how and where it is persisted upon checkpoints depends on the
chosen **State Backend**.

## Available State Backends

Out of the box, Flink bundles these state backends:

 - *HashMapStateBackend*
 - *EmbeddedRocksDBStateBackend*

If nothing else is configured, the system will use the HashMapStateBackend.

### The HashMapStateBackend

The *HashMapStateBackend* holds data internally as objects on the Java heap. Key/value state and window operators hold hash tables
that store the values, triggers, etc.

The HashMapStateBackend is encouraged for:

  - Jobs with large state, long windows, large key/value states.
  - All high-availability setups.

It is also recommended to set [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) to zero.
This will ensure that the maximum amount of memory is allocated for user code on the JVM.

Unlike EmbeddedRocksDBStateBackend, the HashMapStateBackend stores data as objects on the heap so that it is unsafe to reuse objects.

### The EmbeddedRocksDBStateBackend

The EmbeddedRocksDBStateBackend holds in-flight data in a [RocksDB](http://rocksdb.org) database
that is (per default) stored in the TaskManager local data directories.
Unlike storing java objects in `HashMapStateBackend`, data is stored as serialized byte arrays, which are mainly defined by the type serializer, resulting in key comparisons being byte-wise instead of using Java's `hashCode()` and `equals()` methods.

The EmbeddedRocksDBStateBackend always performs asynchronous snapshots.

Limitations of the EmbeddedRocksDBStateBackend:

  - As RocksDB's JNI bridge API is based on byte[], the maximum supported size per key and per value is 2^31 bytes each. 
  States that use merge operations in RocksDB (e.g. ListState) can silently accumulate value sizes > 2^31 bytes and will then fail on their next retrieval. This is currently a limitation of RocksDB JNI.

The EmbeddedRocksDBStateBackend is encouraged for:

  - Jobs with very large state, long windows, large key/value states.
  - All high-availability setups.

Note that the amount of state that you can keep is only limited by the amount of disk space available.
This allows keeping very large state, compared to the HashMapStateBackend that keeps state in memory.
This also means, however, that the maximum throughput that can be achieved will be lower with
this state backend. All reads/writes from/to this backend have to go through de-/serialization to retrieve/store the state objects, which is also more expensive than always working with the
on-heap representation as the heap-based backends are doing. It's safe for EmbeddedRocksDBStateBackend to reuse objects due to the de-/serialization.

Check also recommendations about the [task executor memory configuration]({{< ref "docs/deployment/memory/mem_tuning" >}}#rocksdb-state-backend) for the EmbeddedRocksDBStateBackend.

EmbeddedRocksDBStateBackend is currently the only backend that offers incremental checkpoints (see [here]({{< ref "docs/ops/state/large_state_tuning" >}})). 

Certain RocksDB native metrics are available but disabled by default, you can find full documentation [here]({{< ref "docs/deployment/config" >}}#rocksdb-native-metrics)

The total memory amount of RocksDB instance(s) per slot can also be bounded, please refer to documentation [here]({{< ref "docs/ops/state/large_state_tuning" >}}#bounding-rocksdb-memory-usage) for details.

## Choose The Right State Backend

When deciding between `HashMapStateBackend` and `RocksDB`, it is a choice between performance and scalability.
`HashMapStateBackend` is very fast as each state access and update operates on objects on the Java heap; however, state size is limited by available memory within the cluster.
On the other hand, `RocksDB` can scale based on available disk space and is the only state backend to support incremental snapshots.
However, each state access and update requires (de-)serialization and potentially reading from disk which leads to average performance that is an order of magnitude slower than the memory state backends.

{{< hint info >}}
In Flink 1.13 we unified the binary format of Flink's savepoints. That means you can take a savepoint and then restore from it using a different state backend.
All the state backends produce a common format only starting from version 1.13. Therefore, if you want to switch the state backend you should first upgrade your Flink version then
take a savepoint with the new version, and only after that you can restore it with a different state backend.
{{< /hint >}}

## Configuring a State Backend

The default state backend, if you specify nothing, is the jobmanager. If you wish to establish a different default for all jobs on your cluster, you can do so by defining a new default state backend in [**Flink configuration file**]({{< ref "docs/deployment/config#flink-configuration-file" >}}). The default state backend can be overridden on a per-job basis, as shown below.

### Setting the Per-job State Backend

The per-job state backend is set on the `StreamExecutionEnvironment` of the job, as shown in the example below:

{{< tabs "6e6f1fd6-fcc6-4af4-929f-97dc7d639df4" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStateBackend(new HashMapStateBackend())
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('state.backend.type', 'hashmap')
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}

If you want to use the `EmbeddedRocksDBStateBackend` in your IDE or configure it programmatically in your Flink job, you will have to add the following dependency to your Flink project.

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb</artifactId>
    <version>{{< version >}}</version>
    <scope>provided</scope>
</dependency>
```

{{< hint info >}}
Since RocksDB is part of the default Flink distribution, you do not need this dependency if you are not using any RocksDB code in your job and configure the state backend via `state.backend.type` and further [checkpointing]({{< ref "docs/deployment/config" >}}#checkpointing) and [RocksDB-specific]({{< ref "docs/deployment/config" >}}#rocksdb-state-backend) parameters in your [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}).
{{< /hint >}}


### Setting Default State Backend

A default state backend can be configured in the [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}), using the configuration key `state.backend.type`.

Possible values for the config entry are *hashmap* (HashMapStateBackend), *rocksdb* (EmbeddedRocksDBStateBackend), or the fully qualified class
name of the class that implements the state backend factory {{< gh_link file="flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackendFactory.java" name="StateBackendFactory" >}},
such as `org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackendFactory` for EmbeddedRocksDBStateBackend.

The `execution.checkpointing.dir` option defines the directory to which all backends write checkpoint data and meta data files.
You can find more details about the checkpoint directory structure [here]({{< ref "docs/ops/state/checkpoints" >}}#directory-structure).

A sample section in the configuration file could look as follows:

```yaml
# The backend that will be used to store operator state checkpoints
state.backend: hashmap

# Directory for storing checkpoints
execution.checkpointing.dir: hdfs://namenode:40010/flink/checkpoints
```

## RocksDB State Backend Details

*This section describes the RocksDB state backend in more detail.*

### Incremental Checkpoints

RocksDB supports *Incremental Checkpoints*, which can dramatically reduce the checkpointing time in comparison to full checkpoints.
Instead of producing a full, self-contained backup of the state backend, incremental checkpoints only record the changes that happened since the latest completed checkpoint.

An incremental checkpoint builds upon (typically multiple) previous checkpoints. Flink leverages RocksDB's internal compaction mechanism in a way that is self-consolidating over time. As a result, the incremental checkpoint history in Flink does not grow indefinitely, and old checkpoints are eventually subsumed and pruned automatically.

Recovery time of incremental checkpoints may be longer or shorter compared to full checkpoints. If your network bandwidth is the bottleneck, it may take a bit longer to restore from an incremental checkpoint, because it implies fetching more data (more deltas). Restoring from an incremental checkpoint is faster, if the bottleneck is your CPU or IOPs, because restoring from an incremental checkpoint means not re-building the local RocksDB tables from Flink's canonical key/value snapshot format (used in savepoints and full checkpoints).

While we encourage the use of incremental checkpoints for large state, you need to enable this feature manually:
  - Setting a default in your [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}): `execution.checkpointing.incremental: true` will enable incremental checkpoints, unless the application overrides this setting in the code.
  - You can alternatively configure this directly in the code (overrides the config default): `EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);`

Notice that once incremental checkpoont is enabled, the `Checkpointed Data Size` showed in web UI only represents the 
delta checkpointed data size of that checkpoint instead of full state size.

### Memory Management

Flink aims to control the total process memory consumption to make sure that the Flink TaskManagers have a well-behaved memory footprint. That means staying within the limits enforced by the environment (Docker/Kubernetes, Yarn, etc) to not get killed for consuming too much memory, but also to not under-utilize memory (unnecessary spilling to disk, wasted caching opportunities, reduced performance).

To achieve that, Flink by default configures RocksDB's memory allocation to the amount of managed memory of the TaskManager (or, more precisely, task slot). This should give good out-of-the-box experience for most applications, meaning most applications should not need to tune any of the detailed RocksDB settings. The primary mechanism for improving memory-related performance issues would be to simply increase Flink's managed memory.

Users can choose to deactivate that feature and let RocksDB allocate memory independently per ColumnFamily (one per state per operator). This offers expert users ultimately more fine grained control over RocksDB, but means that users need to take care themselves that the overall memory consumption does not exceed the limits of the environment. See [large state tuning]({{< ref "docs/ops/state/large_state_tuning" >}}#tuning-rocksdb-memory) for some guideline about large state performance tuning.

**Managed Memory for RocksDB**

This feature is active by default and can be (de)activated via the `state.backend.rocksdb.memory.managed` configuration key.

Flink does not directly manage RocksDB's native memory allocations, but configures RocksDB in a certain way to ensure it uses exactly as much memory as Flink has for its managed memory budget. This is done on a per-slot level (managed memory is accounted per slot).

To set the total memory usage of RocksDB instance(s), Flink leverages a shared [cache](https://github.com/facebook/rocksdb/wiki/Block-Cache)
and [write buffer manager](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager) among all instances in a single slot. 
The shared cache will place an upper limit on the [three components](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB) that use the majority of memory
in RocksDB: block cache, index and bloom filters, and MemTables.

For advanced tuning, Flink also provides two parameters to control the division of memory between the *write path* (MemTable) and *read path* (index & filters, remaining cache). When you see that RocksDB performs badly due to lack of write buffer memory (frequent flushes) or cache misses, you can use these parameters to redistribute the memory.

  - `state.backend.rocksdb.memory.write-buffer-ratio`, by default `0.5`, which means 50% of the given memory would be used by write buffer manager.
  - `state.backend.rocksdb.memory.high-prio-pool-ratio`, by default `0.1`, which means 10% of the given memory would be set as high priority for index and filters in shared block cache.
  We strongly suggest not to set this to zero, to prevent index and filters from competing against data blocks for staying in cache and causing performance issues.
  Moreover, the L0 level filter and index are pinned into the cache by default to mitigate performance problems,
  more details please refer to the [RocksDB-documentation](https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-filter-and-compression-dictionary-blocks).

{{< hint info >}}
When the above described mechanism (`cache` and `write buffer manager`) is enabled, it will override any customized settings for block caches and write buffers done via [`PredefinedOptions`](#predefined-per-columnfamily-options) and [`RocksDBOptionsFactory`](#passing-options-factory-to-rocksdb).
{{< /hint >}}

{{< details "Expert Mode" >}}
To control memory manually, you can set `state.backend.rocksdb.memory.managed` to `false` and configure RocksDB via [`ColumnFamilyOptions`](#passing-options-factory-to-rocksdb). Alternatively, you can use the above mentioned cache/buffer-manager mechanism, but set the memory size to a fixed amount independent of Flink's managed memory size (`state.backend.rocksdb.memory.fixed-per-slot` or `state.backend.rocksdb.memory.fixed-per-tm` options). Note that in both cases, users need to ensure on their own that enough memory is available outside the JVM for RocksDB.
{{< /details >}}

### Timers (Heap vs. RocksDB)

Timers are used to schedule actions for later (event-time or processing-time), such as firing a window, or calling back a `ProcessFunction`.

When selecting the RocksDB State Backend, timers are by default also stored in RocksDB. That is a robust and scalable way that lets applications scale to many timers. However, maintaining timers in RocksDB can have a certain cost, which is why Flink provides the option to store timers on the JVM heap instead, even when RocksDB is used to store other states. Heap-based timers can have a better performance when there is a smaller number of timers.

Set the configuration option `state.backend.rocksdb.timer-service.factory` to `heap` (rather than the default, `rocksdb`) to store timers on heap.

{{< hint warning >}}
*The combination RocksDB state backend with heap-based timers currently does **NOT** support asynchronous snapshots for the timers state. Other state like keyed state is still snapshotted asynchronously.*
{{< /hint >}}

{{< hint warning >}}
*When using RocksDB state backend with heap-based timers, checkpointing and taking savepoints is expected to fail if there are operators in application that write to raw keyed state.* This is only relevant to advanced users who are writing custom stream operators.
{{< /hint >}}

### Enabling RocksDB Native Metrics

You can optionally access RockDB's native metrics through Flink's metrics system, by enabling certain metrics selectively.
See [configuration docs]({{< ref "docs/deployment/config" >}}#rocksdb-native-metrics) for details.

{{< hint warning >}}
Enabling RocksDB's native metrics may have a negative performance impact on your application.
{{< /hint >}}

### Advanced RocksDB Memory Turning

{{< hint info >}}
Flink offers sophisticated default [memory management for RocksDB](#memory-management) that should work for most use-cases. The below mechanisms should mainly be used for *expert tuning* or *trouble shooting*.
{{< /hint >}}

#### Predefined Per-ColumnFamily Options

With *Predefined Options*, users can apply some predefined config profiles on each RocksDB Column Family, configuring for example memory use, thread, compaction settings, etc. There is currently one Column Family per each state in each operator.

There are two ways to select predefined options to be applied:
  - Set the option's name in [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}) via `state.backend.rocksdb.predefined-options`.
  - Set the predefined options programmatically: `EmbeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)`.

The default value for this option is `DEFAULT` which translates to `PredefinedOptions.DEFAULT`.

Predefined options set programmatically would override the ones configured via [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}).

#### Reading Column Family Options from Flink configuration file

RocksDB State Backend picks up all config options [defined here]({{< ref "docs/deployment/config" >}}#advanced-rocksdb-state-backends-options). Hence, you can configure low-level Column Family options simply by turning off managed memory for RocksDB and putting the relevant entries in the configuration.

#### Passing Options Factory to RocksDB

To manually control RocksDB's options, you need to configure an `RocksDBOptionsFactory`. This mechanism gives you fine-grained control over the settings of the Column Families, for example memory use, thread, compaction settings, etc. There is currently one Column Family per each state in each operator.

There are two ways to pass a RocksDBOptionsFactory to the RocksDB State Backend:

  - Configure options factory class name in the [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}) via `state.backend.rocksdb.options-factory`.
  
  - Set the options factory programmatically, e.g. `EmbeddedRocksDBStateBackend.setRocksDBOptions(new MyOptionsFactory());`

Options factory which set programmatically would override the one configured via [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}),
and options factory has a higher priority over the predefined options if ever configured or set.

RocksDB is a native library that allocates memory directly from the process,
and not from the JVM. Any memory you assign to RocksDB will have to be accounted for, typically by decreasing the JVM heap size
of the TaskManagers by the same amount. Not doing that may result in YARN/etc terminating the JVM processes for
allocating more memory than configured.

Below is an example how to define a custom ConfigurableOptionsFactory (set class name under `state.backend.rocksdb.options-factory`).

{{< tabs "6e6f1fd6-fcc6-4af4-929f-97dc7d639eg8" >}}
{{< tab "Java" >}}
```java
public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {
    public static final ConfigOption<Integer> BLOCK_RESTART_INTERVAL = ConfigOptions
            .key("my.custom.rocksdb.block.restart-interval")
            .intType()
            .defaultValue(16)
            .withDescription(
                    " Block restart interval. RocksDB has default block restart interval as 16. ");

    private int blockRestartInterval = BLOCK_RESTART_INTERVAL.defaultValue();

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions,
                                     Collection<AutoCloseable> handlesToClose) {
        return currentOptions
                .setIncreaseParallelism(4)
                .setUseFsync(false);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions,
                                                   Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setTableFormatConfig(
                new BlockBasedTableConfig()
                        .setBlockRestartInterval(blockRestartInterval));
    }

    @Override
    public RocksDBOptionsFactory configure(ReadableConfig configuration) {
        this.blockRestartInterval = configuration.get(BLOCK_RESTART_INTERVAL);
        return this;
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Enabling Changelog

### Introduction

Changelog is a feature that aims to decrease checkpointing time and, therefore, end-to-end latency in exactly-once mode.

Most commonly, checkpoint duration is affected by:

1. Barrier travel time and alignment, addressed by
   [Unaligned checkpoints]({{< ref "docs/ops/state/checkpointing_under_backpressure#unaligned-checkpoints" >}})
   and [Buffer debloating]({{< ref "docs/ops/state/checkpointing_under_backpressure#buffer-debloating" >}})
2. Snapshot creation time (so-called synchronous phase), addressed by asynchronous snapshots (mentioned [above]({{<
   ref "#the-embeddedrocksdbstatebackend">}}))
3. Snapshot upload time (asynchronous phase)

Upload time can be decreased by [incremental checkpoints]({{< ref "#incremental-checkpoints" >}}).
However, most incremental state backends perform some form of compaction periodically, which results in re-uploading the
old state in addition to the new changes. In large deployments, the probability of at least one task uploading lots of
data tends to be very high in every checkpoint.

With Changelog enabled, Flink uploads state changes continuously and forms a changelog. On checkpoint, only the relevant
part of this changelog needs to be uploaded. The configured state backend is snapshotted in the
background periodically. Upon successful upload, the changelog is truncated.

As a result, asynchronous phase duration is reduced, as well as synchronous phase - because no data needs to be flushed
to disk. In particular, long-tail latency is improved. At the same time, some other benefits could be got:
1. More Stable and Lower End-to-end Latency.
2. Less Data Replay after Failover.
3. More Stable Utilization of Resources.

However, resource usage is higher:

- more files are created on DFS
- more IO bandwidth is used to upload state changes
- more CPU used to serialize state changes
- more memory used by Task Managers to buffer state changes

It is worth noting that changelog adds a small amount of daily CPU and network bandwidth resources, 
but reduces peak CPU and network bandwidth usage.

Recovery time is another thing to consider. Depending on the `state.changelog.periodic-materialize.interval`
setting, the changelog can become lengthy and replaying it may take more time. However, recovery time combined with
checkpoint duration will likely still be lower than in non-changelog setups, providing lower end-to-end latency even in
failover case. However, it's also possible that the effective recovery time will increase, depending on the actual ratio
of the aforementioned times.

For more details, see [FLIP-158](https://cwiki.apache.org/confluence/display/FLINK/FLIP-158%3A+Generalized+incremental+checkpoints).

### Installation

Changelog JARs are included into the standard Flink distribution.

Make sure to [add]({{< ref "docs/deployment/filesystems/overview" >}}) the necessary filesystem plugins.

### Configuration

Here is an example configuration in YAML:
```yaml
state.changelog.enabled: true
state.changelog.storage: filesystem # currently, only filesystem and memory (for tests) are supported
state.changelog.dstl.dfs.base-path: s3://<bucket-name> # similar to execution.checkpointing.dir
```

Please keep the following defaults (see [limitations](#limitations)):
```yaml
execution.checkpointing.max-concurrent-checkpoints: 1
```

Please refer to the [configuration section]({{< ref "docs/deployment/config#state-changelog-options" >}}) for other options.

Changelog can also be enabled or disabled per job programmatically:
{{< tabs  >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableChangelogStateBackend(true);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableChangelogStateBackend(true)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_changelog_statebackend(true)
```
{{< /tab >}}
{{< /tabs >}}

### Monitoring

Available metrics are listed [here]({{< ref "docs/ops/metrics#state-changelog" >}}).

If a task is backpressured by writing state changes, it will be shown as busy (red) in the UI.

### Upgrading existing jobs

**Enabling Changelog**

Resuming from both savepoints and checkpoints is supported:
- given an existing non-changelog job
- take either a [savepoint]({{< ref "docs/ops/state/savepoints#resuming-from-savepoints" >}}) or a [checkpoint]({{< ref "docs/ops/state/checkpoints#resuming-from-a-retained-checkpoint" >}})
- alter configuration (enable Changelog)
- resume from the taken snapshot

**Disabling Changelog**

Resuming from both savepoints and checkpoints is supported:
- given an existing changelog job
- take either a [savepoint]({{< ref "docs/ops/state/savepoints#resuming-from-savepoints" >}}) or a [checkpoint]({{< ref "docs/ops/state/checkpoints#resuming-from-a-retained-checkpoint" >}})
- alter configuration (disable Changelog)
- resume from the taken snapshot

### Limitations
 - At most one concurrent checkpoint
 - As of Flink 1.15, only `filesystem` changelog implementation is available
- [NO_CLAIM]({{< ref "docs/deployment/config#execution-savepoint-restore-mode" >}}) mode not supported

## Migrating from Legacy Backends

Beginning in **Flink 1.13**, the community reworked its public state backend classes to help users better understand the separation of local state storage and checkpoint storage.
This change does not affect the runtime implementation or characteristics of Flink's state backend or checkpointing process; it is simply to communicate intent better.
Users can migrate existing applications to use the new API without losing any state or consistency. 

### MemoryStateBackend

The legacy `MemoryStateBackend` is equivalent to using [`HashMapStateBackend`](#the-hashmapstatebackend) and [`JobManagerCheckpointStorage`]({{< ref "docs/ops/state/checkpoints#the-jobmanagercheckpointstorage" >}}).

#### Configuration using Flink configuration file

```yaml
state.backend: hashmap

# Optional, Flink will automatically default to JobManagerCheckpointStorage
# when no checkpoint directory is specified.
execution.checkpointing.storage: jobmanager
```

#### Code Configuration

{{< tabs "memorystatebackendmigration" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStateBackend(new HashMapStateBackend)
env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('state.backend.type', 'hashmap')
config.set_string('execution.checkpointing.storage', 'jobmanager')
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs>}}

### FsStateBackend 

The legacy `FsStateBackend` is equivalent to using [`HashMapStateBackend`](#the-hashmapstatebackend) and [`FileSystemCheckpointStorage`]({{< ref "docs/ops/state/checkpoints#the-filesystemcheckpointstorage" >}}).

#### Configuration using Flink configuration file

```yaml
state.backend: hashmap
execution.checkpointing.dir: file:///checkpoint-dir/

# Optional, Flink will automatically default to FileSystemCheckpointStorage
# when a checkpoint directory is specified.
execution.checkpointing.storage: filesystem
```

#### Code Configuration

{{< tabs "fsstatebackendbackendmigration" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///checkpoint-dir");
env.configure(config);


// Advanced FsStateBackend configurations, such as write buffer size
// can be set manually by using CheckpointingOptions.
config.set(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 4 * 1024);
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStateBackend(new HashMapStateBackend)
env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir")


// Advanced FsStateBackend configurations, such as write buffer size
// can be set by using manually instantiating a FileSystemCheckpointStorage object.
env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('state.backend.type', 'hashmap')
config.set_string('execution.checkpointing.storage', 'filesystem')
config.set_string('execution.checkpointing.dir', 'file:///checkpoint-dir')
env = StreamExecutionEnvironment.get_execution_environment(config)


# Advanced FsStateBackend configurations, such as write buffer size
# can be set manually by using CheckpointingOptions.
config.set_string('state.storage.fs.write-buffer-size', '4096');
env.configure(config);
```
{{< /tab >}}
{{< /tabs>}}

### RocksDBStateBackend 

The legacy `RocksDBStateBackend` is equivalent to using [`EmbeddedRocksDBStateBackend`](#the-embeddedrocksdbstatebackend) and [`FileSystemCheckpointStorage`]({{< ref "docs/ops/state/checkpoints#the-filesystemcheckpointstorage" >}}).

#### Configuration using Flink configuration file

```yaml
state.backend: rocksdb
execution.checkpointing.dir: file:///checkpoint-dir/

# Optional, Flink will automatically default to FileSystemCheckpointStorage
# when a checkpoint directory is specified.
execution.checkpointing.storage: filesystem
```

#### Code Configuration

{{< tabs "rocksdbstatebackendmigration" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///checkpoint-dir");
env.configure(config);


// If you manually passed FsStateBackend into the RocksDBStateBackend constructor
// to specify advanced checkpointing configurations such as write buffer size,
// you can achieve the same results by using CheckpointingOptions.
config.set(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 4 * 1024);
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStateBackend(new EmbeddedRocksDBStateBackend)
env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir")


// If you manually passed FsStateBackend into the RocksDBStateBackend constructor
// to specify advanced checkpointing configurations such as write buffer size,
// you can achieve the same results by using manually instantiating a FileSystemCheckpointStorage object.
env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('state.backend.type', 'rocksdb')
config.set_string('execution.checkpointing.storage', 'filesystem')
config.set_string('execution.checkpointing.dir', 'file:///checkpoint-dir')
env = StreamExecutionEnvironment.get_execution_environment(config)


# If you manually passed FsStateBackend into the RocksDBStateBackend constructor
# to specify advanced checkpointing configurations such as write buffer size,
# you can achieve the same results by using CheckpointingOptions.
config.set_string('state.storage.fs.write-buffer-size', '4096');
env.configure(config);
```
{{< /tab >}}
{{< /tabs>}}
