---
title: "Checkpointing"
nav-parent_id: streaming_state
nav-pos: 3
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

* ToC
{:toc}

Every function and operator in Flink can be **stateful** (see [working with state](state.html) for details).
Stateful functions store data across the processing of individual elements/events, making state a critical building block for
any type of more elaborate operation.

In order to make state fault tolerant, Flink needs to **checkpoint** the state. Checkpoints allow Flink to recover state and positions
in the streams to give the application the same semantics as a failure-free execution.

The [documentation on streaming fault tolerance]({{ site.baseurl }}/internals/stream_checkpointing.html) describes in detail the technique behind Flink's streaming fault tolerance mechanism.


## Prerequisites

Flink's checkpointing mechanism interacts with durable storage for streams and state. In general, it requires:

  - A *persistent* (or *durable*) data source that can replay records for a certain amount of time. Examples for such sources are persistent messages queues (e.g., Apache Kafka, RabbitMQ, Amazon Kinesis, Google PubSub) or file systems (e.g., HDFS, S3, GFS, NFS, Ceph, ...).
  - A persistent storage for state, typically a distributed filesystem (e.g., HDFS, S3, GFS, NFS, Ceph, ...)


## Enabling and Configuring Checkpointing

By default, checkpointing is disabled. To enable checkpointing, call `enableCheckpointing(n)` on the `StreamExecutionEnvironment`, where *n* is the checkpoint interval in milliseconds.

Other parameters for checkpointing include:

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

  - *number of concurrent checkpoints*: By default, the system will not trigger another checkpoint while one is still in progress.
    This ensures that the topology does not spend too much time on checkpoints and not make progress with processing the streams.
    It is possible to allow for multiple overlapping checkpoints, which is interesting for pipelines that have a certain processing delay
    (for example because the functions call external services that need some time to respond) but that still want to do very frequent checkpoints
    (100s of milliseconds) to re-process very little upon failures.

    This option cannot be used when a minimum time between checkpoints is defined.

  - *externalized checkpoints*: You can configure periodic checkpoints to be persisted externally. Externalized checkpoints write their meta data out to persistent storage and are *not* automatically cleaned up when the job fails. This way, you will have a checkpoint around to resume from if your job fails. There are more details in the [deployment notes on externalized checkpoints]({{ site.baseurl }}/ops/state/checkpoints.html#externalized-checkpoints).

  - *fail/continue task on checkpoint errors*: This determines if a task will be failed if an error occurs in the execution of the task's checkpoint procedure. This is the default behaviour. Alternatively, when this is disabled, the task will simply decline the checkpoint to the checkpoint coordinator and continue running.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained after job cancellation
env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
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

// prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
env.getCheckpointConfig.setFailTasksOnCheckpointingErrors(false)

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
{% endhighlight %}
</div>
</div>

### Related Config Options

Some more parameters and/or defaults may be set via `conf/flink-conf.yaml` (see [configuration]({{ site.baseurl }}/ops/config.html) for a full guide):

{% include generated/checkpointing_configuration.html %}

{% top %}


## Selecting a State Backend

Flink's [checkpointing mechanism]({{ site.baseurl }}/internals/stream_checkpointing.html) stores consistent snapshots
of all the state in timers and stateful operators, including connectors, windows, and any [user-defined state](state.html).
Where the checkpoints are stored (e.g., JobManager memory, file system, database) depends on the configured
**State Backend**. 

By default, state is kept in memory in the TaskManagers and checkpoints are stored in memory in the JobManager. For proper persistence of large state,
Flink supports various approaches for storing and checkpointing state in other state backends. The choice of state backend can be configured via `StreamExecutionEnvironment.setStateBackend(…)`.

See [state backends]({{ site.baseurl }}/ops/state/state_backends.html) for more details on the available state backends and options for job-wide and cluster-wide configuration.


## State Checkpoints in Iterative Jobs

Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a special flag when enabling checkpointing: `env.enableCheckpointing(interval, force = true)`.

Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during failure.

{% top %}


## Restart Strategies

Flink supports different restart strategies which control how the jobs are restarted in case of a failure. For more 
information, see [Restart Strategies]({{ site.baseurl }}/dev/restart_strategies.html).

{% top %}

