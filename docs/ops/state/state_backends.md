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

## Available State Backends

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
this state backend.

RocksDBStateBackend is currently the only backend that offers incremental checkpoints (see [here](large_state_tuning.html)). 

## Configuring a State Backend

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


### Setting Default State Backend

A default state backend can be configured in the `flink-conf.yaml`, using the configuration key `state.backend`.

Possible values for the config entry are *jobmanager* (MemoryStateBackend), *filesystem* (FsStateBackend), *rocksdb* (RocksDBStateBackend), or the fully qualified class
name of the class that implements the state backend factory [FsStateBackendFactory](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/state/filesystem/FsStateBackendFactory.java),
such as `org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory` for RocksDBStateBackend.

In the case where the default state backend is set to *filesystem*, the entry `state.backend.fs.checkpointdir` defines the directory where the checkpoint data will be stored.

A sample section in the configuration file could look as follows:

{% highlight yaml %}
# The backend that will be used to store operator state checkpoints

state.backend: filesystem


# Directory for storing checkpoints

state.backend.fs.checkpointdir: hdfs://namenode:40010/flink/checkpoints
{% endhighlight %}

{% top %}
