---
title: "Checkpointing"
weight: 4
type: docs
aliases:
  - /dev/stream/state/checkpointing.html
  - /apis/streaming/fault_tolerance.html
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

# Checkpointing

Every function and operator in Flink can be **stateful** (see [working with state]({{< ref "docs/concepts/stateful-stream-processing" >}}) for details).
Stateful functions store data across the processing of individual elements/events, making state a critical building block for
any type of more elaborate operation.

In order to make state fault tolerant, Flink needs to **checkpoint** the state. Checkpoints allow Flink to recover state and positions
in the streams to give the application the same semantics as a failure-free execution.

The [documentation on streaming fault tolerance]({{< ref "docs/learn-flink/fault_tolerance" >}}) describes in detail the technique behind Flink's streaming fault tolerance mechanism.

## Prerequisites

Flink's checkpointing mechanism interacts with durable storage for streams and state. In general, it requires:

  - A *persistent* (or *durable*) data source that can replay records for a certain amount of time. Examples for such sources are persistent messages queues (e.g., Apache Kafka, RabbitMQ, Amazon Kinesis, Google PubSub) or file systems (e.g., HDFS, S3, GFS, NFS, Ceph, ...).
  - A persistent storage for state, typically a distributed filesystem (e.g., HDFS, S3, GFS, NFS, Ceph, ...)


## Enabling and Configuring Checkpointing

By default, checkpointing is disabled. To enable checkpointing, call `enableCheckpointing(n)` on the `StreamExecutionEnvironment`, where *n* is the [checkpoint interval]({{< ref "docs/ops/production_ready#choose-the-right-checkpoint-interval" >}}) in milliseconds.

Other parameters for checkpointing include:

  - *checkpoint storage*: You can set the location where checkpoint snapshots are made durable. By default Flink will use the JobManager's heap. For production deployments it is recommended to instead use a durable filesystem. See [checkpoint storage]({{< ref "docs/ops/state/checkpoints#checkpoint-storage" >}}) for more details on the available options for job-wide and cluster-wide configuration.

  - *exactly-once vs. at-least-once*: You can optionally pass a mode to the `enableCheckpointing(n)` method to choose between the two guarantee levels.
    Exactly-once is preferable for most applications. At-least-once may be relevant for certain super-low-latency (consistently few milliseconds) applications.

  - *checkpoint timeout*: The time after which a checkpoint-in-progress is aborted, if it did not complete by then.

  - *minimum time between checkpoints*: To make sure that the streaming application makes a certain amount of progress between checkpoints,
    one can define how much time needs to pass between checkpoints. If this value is set for example to *5000*, the next checkpoint will be
    started no sooner than 5 seconds after the previous checkpoint completed, regardless of the checkpoint duration and the checkpoint interval.
    Note that this implies that the checkpoint interval will never be smaller than this parameter.
    
    It is often easier to configure applications by defining the "time between checkpoints" than the checkpoint interval, because the "time between checkpoints"
    is not susceptible to the fact that checkpoints may sometimes take longer than on average (for example if the target storage system is temporarily slow).

    Note that this value also implies that the number of concurrent checkpoints is *one*.

  - *tolerable checkpoint failure number*: This defines how many consecutive checkpoint failures will
    be tolerated, before the whole job is failed over. The default value is `0`, which means no
    checkpoint failures will be tolerated, and the job will fail on first reported checkpoint failure.
    This only applies to the following failure reasons: IOException on the Job Manager, failures in
    the async phase on the Task Managers and checkpoint expiration due to a timeout. Failures
    originating from the sync phase on the Task Managers are always forcing failover of an affected
    task. Other types of checkpoint failures (such as checkpoint being subsumed) are being ignored.

  - *number of concurrent checkpoints*: By default, the system will not trigger another checkpoint while one is still in progress.
    This ensures that the topology does not spend too much time on checkpoints and not make progress with processing the streams.
    It is possible to allow for multiple overlapping checkpoints, which is interesting for pipelines that have a certain processing delay
    (for example because the functions call external services that need some time to respond) but that still want to do very frequent checkpoints
    (100s of milliseconds) to re-process very little upon failures.

    This option cannot be used when a minimum time between checkpoints is defined.

  - *externalized checkpoints*: You can configure periodic checkpoints to be persisted externally. Externalized checkpoints write their meta data out to persistent storage and are *not* automatically cleaned up when the job fails. This way, you will have a checkpoint around to resume from if your job fails. There are more details in the [deployment notes on externalized checkpoints]({{< ref "docs/ops/state/checkpoints" >}}#externalized-checkpoints).

  - *unaligned checkpoints*: You can enable [unaligned checkpoints]({{< ref "docs/ops/state/checkpointing_under_backpressure" >}}#unaligned-checkpoints) to greatly reduce checkpointing times under backpressure. This only works for exactly-once checkpoints and with one concurrent checkpoint.

  - *checkpoints with finished tasks*: By default Flink will continue performing checkpoints even if parts of the DAG have finished processing all of their records. Please refer to [important considerations](#checkpointing-with-parts-of-the-graph-finished) for details.

{{< tabs "4b9c6a74-8a45-4ad2-9e80-52fe44a85991" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// start a checkpoint every 1000 ms
env.enableCheckpointing(1000);

// advanced options:

// set mode to exactly-once (this is the default)
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// make sure 500 ms of progress happen between checkpoints
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// checkpoints have to complete within one minute, or are discarded
env.getCheckpointConfig().setCheckpointTimeout(60000);

// only two consecutive checkpoint failures are tolerated
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained
// after job cancellation
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// enables the unaligned checkpoints
env.getCheckpointConfig().enableUnalignedCheckpoints();

// sets the checkpoint storage where checkpoint snapshots will be written
env.getCheckpointConfig().setCheckpointStorage("hdfs:///my/checkpoint/dir");

// enable checkpointing with finished tasks
Configuration config = new Configuration();
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// start a checkpoint every 1000 ms
env.enableCheckpointing(1000)

// advanced options:

// set mode to exactly-once (this is the default)
env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// make sure 500 ms of progress happen between checkpoints
env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

// checkpoints have to complete within one minute, or are discarded
env.getCheckpointConfig.setCheckpointTimeout(60000)

// only two consecutive checkpoint failures are tolerated
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2)

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

// enable externalized checkpoints which are retained 
// after job cancellation
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

// enables the unaligned checkpoints
env.getCheckpointConfig.enableUnalignedCheckpoints()

// sets the checkpoint storage where checkpoint snapshots will be written
env.getCheckpointConfig.setCheckpointStorage("hdfs:///my/checkpoint/dir")

// enable checkpointing with finished tasks
val config = new Configuration()
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true)
env.configure(config)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

# start a checkpoint every 1000 ms
env.enable_checkpointing(1000)

# advanced options:

# set mode to exactly-once (this is the default)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# make sure 500 ms of progress happen between checkpoints
env.get_checkpoint_config().set_min_pause_between_checkpoints(500)

# checkpoints have to complete within one minute, or are discarded
env.get_checkpoint_config().set_checkpoint_timeout(60000)

# only two consecutive checkpoint failures are tolerated
env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)

# allow only one checkpoint to be in progress at the same time
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

# enable externalized checkpoints which are retained after job cancellation
env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

# enables the unaligned checkpoints
env.get_checkpoint_config().enable_unaligned_checkpoints()
```
{{< /tab >}}
{{< /tabs >}}

### Related Config Options

Some more parameters and/or defaults may be set via `conf/flink-conf.yaml` (see [configuration]({{< ref "docs/deployment/config" >}}) for a full guide):

{{< generated/checkpointing_configuration >}}

{{< top >}}

## Selecting Checkpoint Storage

Flink's [checkpointing mechanism]({{< ref "docs/learn-flink/fault_tolerance" >}}) stores consistent snapshots
of all the state in timers and stateful operators, including connectors, windows, and any [user-defined state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}).
Where the checkpoints are stored (e.g., JobManager memory, file system, database) depends on the configured
**Checkpoint Storage**. 

By default, checkpoints are stored in memory in the JobManager. For proper persistence of large state,
Flink supports various approaches for checkpointing state in other locations.
The choice of checkpoint storage can be configured via `StreamExecutionEnvironment.getCheckpointConfig().setCheckpointStorage(…)`.
It is strongly encouraged that checkpoints be stored in a highly-available filesystem for production deployments. 

See [checkpoint storage]({{< ref "docs/ops/state/checkpoints#checkpoint-storage" >}}) for more details on the available options for job-wide and cluster-wide configuration.


## State Checkpoints in Iterative Jobs

Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a special flag when enabling checkpointing: `env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force = true)`.

Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during failure.

## Checkpointing with parts of the graph finished

Starting from Flink 1.14 it is possible to continue performing checkpoints even if parts of the job
graph have finished processing all data, which might happen if it contains bounded sources. This feature
is enabled by default since 1.15, and it could be disabled via a feature flag:

```java
Configuration config = new Configuration();
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```

Once the tasks/subtasks are finished, they do not contribute to the checkpoints any longer. This is 
an important consideration when implementing any custom operators or UDFs (User-Defined Functions).

In order to support checkpointing with tasks that finished, we adjusted the [task lifecycle]({{<ref "docs/internals/task_lifecycle" >}}) 
and introduced the {{< javadoc file="org/apache/flink/streaming/api/operators/StreamOperator.html#finish--" name="StreamOperator#finish" >}} method. 
This method is expected to be a clear cutoff point for flushing
any remaining buffered state. All checkpoints taken after the finish method has been called should
be empty (in most cases) and should not contain any buffered data since there will be no way to emit
this data. One notable exception is if your operator has some pointers to transactions in external
systems (i.e. order to implement the exactly-once semantic). In such a case, checkpoints taken after
invoking the `finish()` method should keep a pointer to the last transaction(s) that will be committed
in the final checkpoint before the operator is closed. A good built-in example of this are
exactly-once sinks and the `TwoPhaseCommitSinkFunction`.

### How does this impact the operator state?

There is a special handling for `UnionListState`, which has often been used to implement a global
view over offsets in an external system (i.e. storing current offsets of Kafka partitions). If we
had discarded a state for a single subtask that had its `close` method called, we would have lost
the offsets for partitions that it had been assigned. In order to work around this problem, we let
checkpoints succeed only if none or all subtasks that use `UnionListState` are finished.

We have not seen `ListState` used in a similar way, but you should be aware that any state
checkpointed after the `close` method will be discarded and not be available after a restore.

Any operator that is prepared to be rescaled should work well with tasks that partially finish.
Restoring from a checkpoint where only a subset of tasks finished is equivalent to restoring such a
task with the number of new subtasks equal to the number of running tasks.

### Waiting for the final checkpoint before task exit

To ensure all the records could be committed for operators using the two-phase commit, 
the tasks would wait for the final checkpoint completed successfully after all the operators finished. 
The final checkpoint would be triggered immediately after all operators have reached end of data, 
without waiting for periodic triggering, but the job will need to wait for this final checkpoint 
to be completed.

{{< top >}}
