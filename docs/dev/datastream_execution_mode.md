---
title: "Execution Mode (Batch/Streaming)"
nav-id: datastream_execution_mode
nav-parent_id: streaming
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

The DataStream API supports different runtime execution modes from which you
can choose depending on the requirements of your use case and the
characteristics of your job.

There is the "classic" execution behavior of the DataStream API, which we call
`STREAMING` execution mode. This should be used for unbounded jobs that require
continuous incremental processing and are expected to stay online indefinitely.

Additionally, there is a batch-style execution mode that we call `BATCH`
execution mode. This executes jobs in a way that is more reminiscent of batch
processing frameworks such as MapReduce. This should be used for bounded jobs
for which you have a known fixed input and which do not run continuously.

Apache Flink's unified approach to stream and batch processing means that a
DataStream application executed over bounded input will produce the same
results regardless of the configured execution mode. By enabling `BATCH`
execution, we allow Flink to apply additional optimizations that we can only do
when we know that our input is bounded. For example, different join/aggregation
strategies can be used, in addition to a different shuffle implementation that
allows more efficient task scheduling and failure recovery behavior. We will go
into some of the details of the execution behavior below.

* This will be replaced by the TOC
{:toc}

## When can/should I use BATCH execution mode?

The `BATCH` execution mode can only be used for Jobs/Flink Programs that are
_bounded_. Boundedness is a property of a data source that tells us whether all
the input coming from that source is known before execution or whether new data
will show up, potentially indefinitely. A job, in turn, is bounded if all its
sources are bounded, and unbounded otherwise.

`STREAMING` execution mode, on the other hand, can be used for both bounded and
unbounded jobs.

As a rule of thumb, you should be using `BATCH` execution mode when your program
is bounded because this will be more efficient. You have to use `STREAMING`
execution mode when your program is unbounded because only this mode is general
enough to be able to deal with continuous data streams.

One obvious outlier is when you want to use a bounded job to bootstrap some job
state that you then want to use in an unbounded job. For example, by running a
bounded job using `STREAMING` mode, taking a savepoint, and then restoring that
savepoint on an unbounded job. This is a very specific use case and one that
might soon become obsolete when we allow producing a savepoint as additional
output of a `BATCH` execution job.

Another case where you might run a bounded job using `STREAMING` mode is when
writing tests for code that will eventually run with unbounded sources. For
testing it can be more natural to use a bounded source in those cases.

## Configuring BATCH execution mode

The execution mode can be configured via the `execution.runtime-mode` setting.
There are three possible values:

 - `STREAMING`: The classic DataStream execution mode (default)
 - `BATCH`: Batch-style execution on the DataStream API
 - `AUTOMATIC`: Let the system decide based on the boundedness of the sources

This can be configured via command line parameters of `bin/flink run ...`, or
programmatically when creating/configuring the `StreamExecutionEnvironment`.

Here's how you can configure the execution mode via the command line:

```bash
$ bin/flink run -Dexecution.runtime-mode=BATCH examples/streaming/WordCount.jar
```

This example shows how you can configure the execution mode in code:

 ```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
 ```

<div class="alert alert-info">
We recommend users to NOT set the runtime mode in their program but to instead
set it using the command-line when submitting the application. Keeping the
application code configuration-free allows for more flexibility as the same
application can be executed in any execution mode.
</div>

## Execution Behavior

This section provides an overview of the execution behavior of `BATCH`
execution mode and contrasts it with `STREAMING` execution mode. For more
details, please refer to the FLIPs that introduced this feature:
[FLIP-134](https://cwiki.apache.org/confluence/x/4i94CQ) and
[FLIP-140](https://cwiki.apache.org/confluence/x/kDh4CQ).

### Task Scheduling And Network Shuffle

Flink jobs consist of different operations that are connected together in a
dataflow graph. The system decides how to schedule the execution of these
operations on different processes/machines (TaskManagers) and how data is
shuffled (sent) between them.

Multiple operations/operators can be chained together using a feature called
[chaining]({% link dev/stream/operators/index.md
%}#task-chaining-and-resource-groups). A group of one or multiple (chained)
operators that Flink considers as a unit of scheduling is called a _task_.
Often the term _subtask_ is used to refer to the individual instances of tasks
that are running in parallel on multiple TaskManagers but we will only use the
term _task_ here.

Task scheduling and network shuffles work differently for `BATCH` and
`STREAMING` execution mode. Mostly due to the fact that we know our input data
is bounded in `BATCH` execution mode, which allows Flink to use more efficient
data structures and algorithms.

We will use this example to explain the differences in task scheduling and
network transfer:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStreamSource<String> source = env.fromElements(...);

source.name("source")
	.map(...).name("map1")
	.map(...).name("map2")
	.rebalance()
	.map(...).name("map3")
	.map(...).name("map4")
	.keyBy((value) -> value)
	.map(...).name("map5")
	.map(...).name("map6")
	.sinkTo(...).name("sink");
```

Operations that imply a 1-to-1 connection pattern between operations, such as
`map()`, `flatMap()`, or `filter()` can just forward data straight to the next
operation, which allows these operations to be chained together. This means
that Flink would not normally insert a network shuffle between them.

Operation such as `keyBy()` or `rebalance()` on the other hand require data to
be shuffled between different parallel instances of tasks. This induces a
network shuffle.

For the above example Flink would group operations together as tasks like this:

- Task1: `source`, `map1`, and `map2`
- Task2: `map3`, `map4`
- Task3: `map5`, `map6`, and `sink`

And we have a network shuffle between Tasks 1 and 2, and also Tasks 2 and 3.
This is a visual representation of that job:

<img src="{% link fig/datastream-example-job-graph.svg %}" alt="Example Job Graph" />

#### STREAMING Execution Mode

In `STREAMING` execution mode, all tasks need to be online/running all the
time.  This allows Flink to immediately process new records through the whole
pipeline, which we need for continuous and low-latency stream processing. This
also means that the TaskManagers that are allotted to a job need to have enough
resources to run all the tasks at the same time.

Network shuffles are _pipelined_, meaning that records are immediately sent to
downstream tasks, with some buffering on the network layer. Again, this is
required because when processing a continuous stream of data there are no
natural points (in time) where data could be materialized between tasks (or
pipelines of tasks). This contrasts with `BATCH` execution mode where
intermediate results can be materialized, as explained below.

#### BATCH Execution Mode

In `BATCH` execution mode, the tasks of a job can be separated into stages that
can be executed one after another. We can do this because the input is bounded
and Flink can therefore fully process one stage of the pipeline before moving
on to the next. In the above example the job would have three stages that
correspond to the three tasks that are separated by the shuffle barriers.

Instead of sending records immediately to downstream tasks, as explained above
for `STREAMING` mode, processing in stages requires Flink to materialize
intermediate results of tasks to some non-ephemeral storage which allows
downstream tasks to read them after upstream tasks have already gone off line.
This will increase the latency of processing but comes with other interesting
properties. For one, this allows Flink to backtrack to the latest available
results when a failure happens instead of restarting the whole job. Another
side effect is that `BATCH` jobs can execute on fewer resources (in terms of
available slots at TaskManagers) because the system can execute tasks
sequentially one after the other.

TaskManagers will keep intermediate results at least as long as downstream
tasks have not consumed them. (Technically, they will be kept until the
consuming *pipelined regions* have produced their output.) After
that, they will be kept for as long as space allows in order to allow the
aforementioned backtracking to earlier results in case of a failure.

### State Backends / State

In `STREAMING` mode, Flink uses a [StateBackend]({% link
dev/stream/state/state_backends.md %}) to control how state is stored and how
checkpointing works.

In `BATCH` mode, the configured state backend is ignored. Instead, the input of
a keyed operation is grouped by key (using sorting) and then we process all
records of a key in turn. This allows keeping only the state of only one key at
the same time. State for a given key will be discarded when moving on to the
next key.

See [FLIP-140](https://cwiki.apache.org/confluence/x/kDh4CQ) for background
information on this.

### Event Time / Watermarks

### Processing Time

### Failure Recovery

In `STREAMING` execution mode, Flink uses checkpoints for failure recovery.
Take a look at the [checkpointing documentation]({% link
dev/stream/state/checkpointing.md %}) for hands-on documentation about this and
how to configure it. There is also a more introductory section about [fault
tolerance via state snapshots]({% link learn-flink/fault_tolerance.md %}) that
explains the concepts at a higher level.

One of the characteristics of checkpointing for failure recovery is that Flink
will restart all the running tasks from a checkpoint in case of a failure. This
can be more costly than what we have to do in `BATCH` mode (as explained
below), which is one of the reasons that you should use `BATCH` execution mode
if your job allows it.

In `BATCH` execution mode, Flink will try and backtrack to previous processing
stages for which intermediate results are still available. Potentially, only
the tasks that failed (or their predecessors in the graph) will have to be
restarted, which can improve processing efficiency and overall processing time
of the job compared to restarting all tasks from a checkpoint.

## Important Considerations

What doesn't work:
 - broadcast state/pattern
 - iterations
 - operations that rely on checkpointing
 - this includes most "regular" sinks
