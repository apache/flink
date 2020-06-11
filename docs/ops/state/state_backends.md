---
title: "State Backends"
nav-parent_id: ops_state
nav-pos: 11
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

Programs written in the [Data Stream API]({{ site.baseurl }}/dev/datastream_api.html) often hold state in various forms:

- Windows gather elements or aggregates until they are triggered
- Transformation functions may use the key/value state interface to store values
- Transformation functions may implement the `CheckpointedFunction` interface to make their local variables fault tolerant

See also [state section]({{ site.baseurl }}/dev/stream/state/index.html) in the streaming API guide.

When checkpointing is activated, such state is persisted upon checkpoints to guard against data loss and recover consistently.
How the state is represented internally, and how and where it is persisted upon checkpoints depends on the
chosen **State Backend**.

* ToC
{:toc}

# Available State Backends

Out of the box, Flink bundles these state backends:

 - *MemoryStateBackend*
 - *FsStateBackend*
 - *RocksDBStateBackend*

If nothing else is configured, the system will use the MemoryStateBackend.


### The MemoryStateBackend

The *MemoryStateBackend* holds data internally as objects on the Java heap. Key/value state and window operators hold hash tables
that store the values, triggers, etc.

Upon checkpoints, this state backend will snapshot the state and send it as part of the checkpoint acknowledgement messages to the
JobManager (master), which stores it on its heap as well.

The MemoryStateBackend can be configured to use asynchronous snapshots. While we strongly encourage the use of asynchronous snapshots to avoid blocking pipelines, please note that this is currently enabled 
by default. To disable this feature, users can instantiate a `MemoryStateBackend` with the corresponding boolean flag in the constructor set to `false`(this should only used for debug), e.g.:

{% highlight java %}
new MemoryStateBackend(MAX_MEM_STATE_SIZE, false);
{% endhighlight %}

Limitations of the MemoryStateBackend:

  - The size of each individual state is by default limited to 5 MB. This value can be increased in the constructor of the MemoryStateBackend.
  - Irrespective of the configured maximal state size, the state cannot be larger than the akka frame size (see [Configuration]({{ site.baseurl }}/ops/config.html)).
  - The aggregate state must fit into the JobManager memory.

The MemoryStateBackend is encouraged for:

  - Local development and debugging
  - Jobs that do hold little state, such as jobs that consist only of record-at-a-time functions (Map, FlatMap, Filter, ...). The Kafka Consumer requires very little state.

It is also recommended to set [managed memory](../memory/mem_setup_tm.html#managed-memory) to zero.
This will ensure that the maximum amount of memory is allocated for user code on the JVM.

### The FsStateBackend

The *FsStateBackend* is configured with a file system URL (type, address, path), such as "hdfs://namenode:40010/flink/checkpoints" or "file:///data/flink/checkpoints".

The FsStateBackend holds in-flight data in the TaskManager's memory. Upon checkpointing, it writes state snapshots into files in the configured file system and directory. Minimal metadata is stored in the JobManager's memory (or, in high-availability mode, in the metadata checkpoint).

The FsStateBackend uses *asynchronous snapshots by default* to avoid blocking the processing pipeline while writing state checkpoints. To disable this feature, users can instantiate a `FsStateBackend` with the corresponding boolean flag in the constructor set to `false`, e.g.:

{% highlight java %}
new FsStateBackend(path, false);
{% endhighlight %}

The FsStateBackend is encouraged for:

  - Jobs with large state, long windows, large key/value states.
  - All high-availability setups.

It is also recommended to set [managed memory](../memory/mem_setup_tm.html#managed-memory) to zero.
This will ensure that the maximum amount of memory is allocated for user code on the JVM.

### The RocksDBStateBackend

The *RocksDBStateBackend* is configured with a file system URL (type, address, path), such as "hdfs://namenode:40010/flink/checkpoints" or "file:///data/flink/checkpoints".

The RocksDBStateBackend holds in-flight data in a [RocksDB](http://rocksdb.org) database
that is (per default) stored in the TaskManager data directories. Upon checkpointing, the whole
RocksDB database will be checkpointed into the configured file system and directory. Minimal
metadata is stored in the JobManager's memory (or, in high-availability mode, in the metadata checkpoint).

The RocksDBStateBackend always performs asynchronous snapshots.

Limitations of the RocksDBStateBackend:

  - As RocksDB's JNI bridge API is based on byte[], the maximum supported size per key and per value is 2^31 bytes each. 
  IMPORTANT: states that use merge operations in RocksDB (e.g. ListState) can silently accumulate value sizes > 2^31 bytes and will then fail on their next retrieval. This is currently a limitation of RocksDB JNI.

The RocksDBStateBackend is encouraged for:

  - Jobs with very large state, long windows, large key/value states.
  - All high-availability setups.

Note that the amount of state that you can keep is only limited by the amount of disk space available.
This allows keeping very large state, compared to the FsStateBackend that keeps state in memory.
This also means, however, that the maximum throughput that can be achieved will be lower with
this state backend. All reads/writes from/to this backend have to go through de-/serialization to retrieve/store the state objects, which is also more expensive than always working with the
on-heap representation as the heap-based backends are doing.

Check also recommendations about the [task executor memory configuration](../memory/mem_tuning.html#rocksdb-state-backend) for the RocksDBStateBackend.

RocksDBStateBackend is currently the only backend that offers incremental checkpoints (see [here](large_state_tuning.html)). 

Certain RocksDB native metrics are available but disabled by default, you can find full documentation [here]({{ site.baseurl }}/ops/config.html#rocksdb-native-metrics)

The total memory amount of RocksDB instance(s) per slot can also be bounded, please refer to documentation [here](large_state_tuning.html#bounding-rocksdb-memory-usage) for details.

# Configuring a State Backend

The default state backend, if you specify nothing, is the jobmanager. If you wish to establish a different default for all jobs on your cluster, you can do so by defining a new default state backend in **flink-conf.yaml**. The default state backend can be overridden on a per-job basis, as shown below.

### Setting the Per-job State Backend

The per-job state backend is set on the `StreamExecutionEnvironment` of the job, as shown in the example below:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"))
{% endhighlight %}
</div>
</div>

If you want to use the `RocksDBStateBackend` in your IDE or configure it programmatically in your Flink job, you will have to add the following dependency to your Flink project.

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb{{ site.scala_version_suffix }}</artifactId>
    <version>{{ site.version }}</version>
    <scope>provided</scope>
</dependency>
{% endhighlight %}

<div class="alert alert-info" markdown="span">
  <strong>Note:</strong> Since RocksDB is part of the default Flink distribution, you do not need this dependency if you are not using any RocksDB code in your job and configure the state backend via `state.backend` and further [checkpointing]({{ site.baseurl }}/ops/config.html#checkpointing) and [RocksDB-specific]({{ site.baseurl }}/ops/config.html#rocksdb-state-backend) parameters in your `flink-conf.yaml`.
</div>


### Setting Default State Backend

A default state backend can be configured in the `flink-conf.yaml`, using the configuration key `state.backend`.

Possible values for the config entry are *jobmanager* (MemoryStateBackend), *filesystem* (FsStateBackend), *rocksdb* (RocksDBStateBackend), or the fully qualified class
name of the class that implements the state backend factory [StateBackendFactory](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackendFactory.java),
such as `org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory` for RocksDBStateBackend.

The `state.checkpoints.dir` option defines the directory to which all backends write checkpoint data and meta data files.
You can find more details about the checkpoint directory structure [here](checkpoints.html#directory-structure).

A sample section in the configuration file could look as follows:

{% highlight yaml %}
# The backend that will be used to store operator state checkpoints

state.backend: filesystem


# Directory for storing checkpoints

state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
{% endhighlight %}


# RocksDB State Backend Details

*This section describes the RocksDB state backend in more detail.*

### Incremental Checkpoints

RocksDB supports *Incremental Checkpoints*, which can dramatically reduce the checkpointing time in comparison to full checkpoints.
Instead of producing a full, self-contained backup of the state backend, incremental checkpoints only record the changes that happened since the latest completed checkpoint.

An incremental checkpoint builds upon (typically multiple) previous checkpoints. Flink leverages RocksDB's internal compaction mechanism in a way that is self-consolidating over time. As a result, the incremental checkpoint history in Flink does not grow indefinitely, and old checkpoints are eventually subsumed and pruned automatically.

Recovery time of incremental checkpoints may be longer or shorter compared to full checkpoints. If your network bandwidth is the bottleneck, it may take a bit longer to restore from an incremental checkpoint, because it implies fetching more data (more deltas). Restoring from an incremental checkpoint is faster, if the bottleneck is your CPU or IOPs, because restoring from an incremental checkpoint means not re-building the local RocksDB tables from Flink's canonical key/value snapshot format (used in savepoints and full checkpoints).

While we encourage the use of incremental checkpoints for large state, you need to enable this feature manually:
  - Setting a default in your `flink-conf.yaml`: `state.backend.incremental: true` will enable incremental checkpoints, unless the application overrides this setting in the code.
  - You can alternatively configure this directly in the code (overrides the config default): `RocksDBStateBackend backend = new RocksDBStateBackend(checkpointDirURI, true);`

Notice that once incremental checkpoont is enabled, the `Checkpointed Data Size` showed in web UI only represents the 
delta checkpointed data size of that checkpoint instead of full state size.

### Memory Management

Flink aims to control the total process memory consumption to make sure that the Flink TaskManagers have a well-behaved memory footprint. That means staying within the limits enforced by the environment (Docker/Kubernetes, Yarn, etc) to not get killed for consuming too much memory, but also to not under-utilize memory (unnecessary spilling to disk, wasted caching opportunities, reduced performance).

To achieve that, Flink by default configures RocksDB's memory allocation to the amount of managed memory of the TaskManager (or, more precisely, task slot). This should give good out-of-the-box experience for most applications, meaning most applications should not need to tune any of the detailed RocksDB settings. The primary mechanism for improving memory-related performance issues would be to simply increase Flink's managed memory.

Users can choose to deactivate that feature and let RocksDB allocate memory independently per ColumnFamily (one per state per operator). This offers expert users ultimately more fine grained control over RocksDB, but means that users need to take care themselves that the overall memory consumption does not exceed the limits of the environment. See [large state tuning]({{ site.baseurl }}/ops/state/large_state_tuning.html#tuning-rocksdb-memory) for some guideline about large state performance tuning.

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

<span class="label label-info">Note</span> When the above described mechanism (`cache` and `write buffer manager`) is enabled, it will override any customized settings for block caches and write buffers done via [`PredefinedOptions`](#predefined-per-columnfamily-options) and [`RocksDBOptionsFactory`](#passing-options-factory-to-rocksdb).

<span class="label label-info">Note</span> *Expert Mode*: To control memory manually, you can set `state.backend.rocksdb.memory.managed` to `false` and configure RocksDB via [`ColumnFamilyOptions`](#passing-options-factory-to-rocksdb). Alternatively, you can use the above mentioned cache/buffer-manager mechanism, but set the memory size to a fixed amount independent of Flink's managed memory size (`state.backend.rocksdb.memory.fixed-per-slot` option). Note that in both cases, users need to ensure on their own that enough memory is available outside the JVM for RocksDB.

### Timers (Heap vs. RocksDB)

Timers are used to schedule actions for later (event-time or processing-time), such as firing a window, or calling back a `ProcessFunction`.

When selecting the RocksDB State Backend, timers are by default also stored in RocksDB. That is a robust and scalable way that lets applications scale to many timers. However, maintaining timers in RocksDB can have a certain cost, which is why Flink provides the option to store timers on the JVM heap instead, even when RocksDB is used to store other states. Heap-based timers can have a better performance when there is a smaller number of timers.

Set the configuration option `state.backend.rocksdb.timer-service.factory` to `heap` (rather than the default, `rocksdb`) to store timers on heap.

<span class="label label-info">Note</span> *The combination RocksDB state backend with heap-based timers currently does NOT support asynchronous snapshots for the timers state. Other state like keyed state is still snapshotted asynchronously.*

### Enabling RocksDB Native Metrics

You can optionally access RockDB's native metrics through Flink's metrics system, by enabling certain metrics selectively.
See [configuration docs]({{ site.baseurl }}/ops/config.html#rocksdb-native-metrics) for details.

<div class="alert alert-warning">
  <strong>Note:</strong> Enabling RocksDB's native metrics may have a negative performance impact on your application.
</div>

### Predefined Per-ColumnFamily Options

<span class="label label-info">Note</span> With the introduction of [memory management for RocksDB](#memory-management) this mechanism should be mainly used for *expert tuning* or *trouble shooting*.

With *Predefined Options*, users can apply some predefined config profiles on each RocksDB Column Family, configuring for example memory use, thread, compaction settings, etc. There is currently one Column Family per each state in each operator.

There are two ways to select predefined options to be applied:
  - Set the option's name in `flink-conf.yaml` via `state.backend.rocksdb.predefined-options`.
  - Set the predefined options programmatically: `RocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)`.

The default value for this option is `DEFAULT` which translates to `PredefinedOptions.DEFAULT`.

<span class="label label-info">Note</span> Predefined options set programmatically would override the ones configured via `flink-conf.yaml`.

### Passing Options Factory to RocksDB

<span class="label label-info">Note</span> With the introduction of [memory management for RocksDB](#memory-management) this mechanism should be mainly used for *expert tuning* or *trouble shooting*.

To manually control RocksDB's options, you need to configure an `RocksDBOptionsFactory`. This mechanism gives you fine-grained control over the settings of the Column Families, for example memory use, thread, compaction settings, etc. There is currently one Column Family per each state in each operator.

There are two ways to pass a RocksDBOptionsFactory to the RocksDB State Backend:

  - Configure options factory class name in the `flink-conf.yaml` via `state.backend.rocksdb.options-factory`.
  
  - Set the options factory programmatically, e.g. `RocksDBStateBackend.setOptions(new MyOptionsFactory());`

<span class="label label-info">Note</span> Options factory which set programmatically would override the one configured via `flink-conf.yaml`,
and options factory has a higher priority over the predefined options if ever configured or set.

<span class="label label-info">Note</span> RocksDB is a native library that allocates memory directly from the process,
and not from the JVM. Any memory you assign to RocksDB will have to be accounted for, typically by decreasing the JVM heap size
of the TaskManagers by the same amount. Not doing that may result in YARN/Mesos/etc terminating the JVM processes for
allocating more memory than configured.

**Reading Column Family Options from flink-conf.yaml**

When a `RocksDBOptionsFactory` implements the `ConfigurableRocksDBOptionsFactory` interface, it can directly read settings from the configuration (`flink-conf.yaml`).

The default value for `state.backend.rocksdb.options-factory` is in fact `org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory` which picks up all config options [defined here]({{ site.baseurl }}/ops/config.html#advanced-rocksdb-state-backends-options) by default. Hence, you can configure low-level Column Family options simply by turning off managed memory for RocksDB and putting the relevant entries in the configuration.

Below is an example how to define a custom ConfigurableOptionsFactory (set class name under `state.backend.rocksdb.options-factory`).

{% highlight java %}

public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private static final long DEFAULT_SIZE = 256 * 1024 * 1024;  // 256 MB
    private long blockCacheSize = DEFAULT_SIZE;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setIncreaseParallelism(4)
               .setUseFsync(false);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
        ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setTableFormatConfig(
            new BlockBasedTableConfig()
                .setBlockCacheSize(blockCacheSize)
                .setBlockSize(128 * 1024));            // 128 KB
    }

    @Override
    public RocksDBOptionsFactory configure(Configuration configuration) {
        this.blockCacheSize =
            configuration.getLong("my.custom.rocksdb.block.cache.size", DEFAULT_SIZE);
        return this;
    }
}
{% endhighlight %}

{% top %}
