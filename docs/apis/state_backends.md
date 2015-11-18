---
title:  "State Backends"
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

Programs written in the [Data Stream API]({{ site.baseurl }}/apis/streaming_guide.html) often hold state in various forms:

- Windows gather elements or aggregates until they are triggered
- Transformation functions may use the key/value state interface to store values
- Transformation functions may implement the `Checkpointed` interface to make their local variables fault tolerant

See also [Working with State]({{ site.baseurl }}/apis/streaming_guide.html#working_with_state) in the streaming API guide.

When checkpointing is activated, such state is persisted upon checkpoints to guard against data loss and recover consistently.
How the state is represented internally, and how and where it is persisted upon checkpoints depends on the
chosen **State Backend**.


## Available State Backends

Out of the box, Flink bundles two state backends: *MemoryStateBacked* and *FsStateBackend*. If nothing else is configured,
the system will use the MemoryStateBacked.


### The MemoryStateBackend

The *MemoryStateBacked* holds data internally as objects on the Java heap. Key/value state and window operators hold hash tables
that store the values, triggers, etc.

Upon checkpoints, this state backend will snapshot the state and send it as part of the checkpoint acknowledgement messages to the
JobManager (master), which stores it on its heap as well.

Limitations of the MemoryStateBackend:

  - The size of each individual state is by default limited to 5 MB. This value can be increased in the constructor of the MemoryStateBackend.
  - Irrespective of the configured maximal state size, the state cannot be larger than the akka frame size (see [Configuration]({{ site.baseurl }}/setup/config.html)).
  - The aggregate state must fit into the JobManager memory.

The MemoryStateBackend is encouraged for:

  - Local development and debugging
  - Jobs that do hold little state, such as jobs that consist only of record-at-a-time functions (Map, FlatMap, Filter, ...). The Kafka Consumer requires very little state.


### The FsStateBackend

The *FsStateBackend* (FileSystemStateBackend) is configured with a file system URL (type, address, path), such as for example "hdfs://namenode:40010/flink/checkpoints" or "file:///data/flink/checkpoints". 

The FsStateBackend holds in-flight data in the TaskManager's memory. Upon checkpoints, it writes state snapshots into files in the configured file system and directory. Minimal metadata is stored in the JobManager's memory (or, in high-availability mode, in the metadata checkpoint).

The FsStateBackend is encouraged for:

  - Jobs with large state, long windows, large key/value states.
  - All high-availability setups.


## Configuring a State Backend

State backends can be configured per job. In addition, you can define a default state backend to be used when the
job does not explicitly define a state backend.


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

Possible values for the config entry are *jobmanager* (MemoryStateBackend), *filesystem* (FsStateBackend), or the fully qualified class
name of the class that implements the state backend factory [FsStateBackendFactory](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/state/filesystem/FsStateBackendFactory.java).

In the case where the default state backend is set to *filesystem*, the entry `state.backend.fs.checkpointdir` defines the directory where the checkpoint data will be stored.

A sample section in the configuration file could look as follows:

~~~
# The backend that will be used to store operator state checkpoints

state.backend: filesystem


# Directory for storing checkpoints

state.backend.fs.checkpointdir: hdfs://namenode:40010/flink/checkpoints
~~~

