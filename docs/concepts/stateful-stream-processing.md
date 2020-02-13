---
title: Stateful Stream Processing
nav-id: stateful-stream-processing
nav-pos: 2
nav-title: Stateful Stream Processing
nav-parent_id: concepts
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

While many operations in a dataflow simply look at one individual *event at a
time* (for example an event parser), some operations remember information
across multiple events (for example window operators).  These operations are
called **stateful**.

Stateful functions and operators store data across the processing of individual
elements/events, making state a critical building block for any type of more
elaborate operation.

For example:

  - When an application searches for certain event patterns, the state will
    store the sequence of events encountered so far.
  - When aggregating events per minute/hour/day, the state holds the pending
    aggregates.
  - When training a machine learning model over a stream of data points, the
    state holds the current version of the model parameters.
  - When historic data needs to be managed, the state allows efficient access
    to events that occurred in the past.

Flink needs to be aware of the state in order to make state fault tolerant
using [checkpoints]({{ site.baseurl}}{% link dev/stream/state/checkpointing.md
%}) and to allow [savepoints]({{ site.baseurl }}{%link ops/state/savepoints.md
%}) of streaming applications.

Knowledge about the state also allows for rescaling Flink applications, meaning
that Flink takes care of redistributing state across parallel instances.

The [queryable state]({{ site.baseurl }}{% link
dev/stream/state/queryable_state.md %}) feature of Flink allows you to access
state from outside of Flink during runtime.

When working with state, it might also be useful to read about [Flink's state
backends]({{ site.baseurl }}{% link ops/state/state_backends.md %}). Flink
provides different state backends that specify how and where state is stored.
State can be located on Java's heap or off-heap. Depending on your state
backend, Flink can also *manage* the state for the application, meaning Flink
deals with the memory management (possibly spilling to disk if necessary) to
allow applications to hold very large state. State backends can be configured
without changing your application logic.

* This will be replaced by the TOC
{:toc}

## What is State?

`TODO: expand this section`

There are different types of state in Flink, the most-used type of state is
*Keyed State*. For special cases you can use *Operator State* and *Broadcast
State*. *Broadcast State* is a special type of *Operator State*.

{% top %}

## State in Stream & Batch Processing

`TODO: What is this section about? Do we even need it?`

{% top %}

## Keyed State

Keyed state is maintained in what can be thought of as an embedded key/value
store.  The state is partitioned and distributed strictly together with the
streams that are read by the stateful operators. Hence, access to the key/value
state is only possible on *keyed streams*, after a *keyBy()* function, and is
restricted to the values associated with the current event's key. Aligning the
keys of streams and state makes sure that all state updates are local
operations, guaranteeing consistency without transaction overhead.  This
alignment also allows Flink to redistribute the state and adjust the stream
partitioning transparently.

<img src="{{ site.baseurl }}/fig/state_partitioning.svg" alt="State and Partitioning" class="offset" width="50%" />

Keyed State is further organized into so-called *Key Groups*. Key Groups are
the atomic unit by which Flink can redistribute Keyed State; there are exactly
as many Key Groups as the defined maximum parallelism.  During execution each
parallel instance of a keyed operator works with the keys for one or more Key
Groups.

`TODO: potentially leave out Operator State and Broadcast State from concepts documentation`

## Operator State

*Operator State* (or *non-keyed state*) is state that is is bound to one
parallel operator instance.  The [Kafka Connector]({{ site.baseurl }}{% link
dev/connectors/kafka.md %}) is a good motivating example for the use of
Operator State in Flink. Each parallel instance of the Kafka consumer maintains
a map of topic partitions and offsets as its Operator State.

The Operator State interfaces support redistributing state among parallel
operator instances when the parallelism is changed. There can be different
schemes for doing this redistribution.

## Broadcast State

*Broadcast State* is a special type of *Operator State*.  It was introduced to
support use cases where some data coming from one stream is required to be
broadcasted to all downstream tasks, where it is stored locally and is used to
process all incoming elements on the other stream. As an example where
broadcast state can emerge as a natural fit, one can imagine a low-throughput
stream containing a set of rules which we want to evaluate against all elements
coming from another stream. Having the above type of use cases in mind,
broadcast state differs from the rest of operator states in that:
 1. it has a map format,
 2. it is only available to specific operators that have as inputs a
    *broadcasted* stream and a *non-broadcasted* one, and
 3. such an operator can have *multiple broadcast states* with different names.

{% top %}

## State Persistence

Flink implements fault tolerance using a combination of **stream replay** and
**checkpointing**. A checkpoint is related to a specific point in each of the
input streams along with the corresponding state for each of the operators. A
streaming dataflow can be resumed from a checkpoint while maintaining
consistency *(exactly-once processing semantics)* by restoring the state of the
operators and replaying the events from the point of the checkpoint.

The checkpoint interval is a means of trading off the overhead of fault
tolerance during execution with the recovery time (the number of events that
need to be replayed).

The fault tolerance mechanism continuously draws snapshots of the distributed
streaming data flow. For streaming applications with small state, these
snapshots are very light-weight and can be drawn frequently without much impact
on performance.  The state of the streaming applications is stored at a
configurable place (such as the master node, or HDFS).

In case of a program failure (due to machine-, network-, or software failure),
Flink stops the distributed streaming dataflow.  The system then restarts the
operators and resets them to the latest successful checkpoint. The input
streams are reset to the point of the state snapshot. Any records that are
processed as part of the restarted parallel dataflow are guaranteed to not have
been part of the previously checkpointed state.

{% info Note %} By default, checkpointing is disabled. See [Checkpointing]({{
site.baseurl }}{% link dev/stream/state/checkpointing.md %}) for details on how
to enable and configure checkpointing.

{% info Note %} For this mechanism to realize its full guarantees, the data
stream source (such as message queue or broker) needs to be able to rewind the
stream to a defined recent point. [Apache Kafka](http://kafka.apache.org) has
this ability and Flink's connector to Kafka exploits this ability. See [Fault
Tolerance Guarantees of Data Sources and Sinks]({{ site.baseurl }}{% link
dev/connectors/guarantees.md %}) for more information about the guarantees
provided by Flink's connectors.

{% info Note %} Because Flink's checkpoints are realized through distributed
snapshots, we use the words *snapshot* and *checkpoint* interchangeably.

### Checkpointing

The central part of Flink's fault tolerance mechanism is drawing consistent
snapshots of the distributed data stream and operator state.  These snapshots
act as consistent checkpoints to which the system can fall back in case of a
failure. Flink's mechanism for drawing these snapshots is described in
"[Lightweight Asynchronous Snapshots for Distributed
Dataflows](http://arxiv.org/abs/1506.08603)". It is inspired by the standard
[Chandy-Lamport
algorithm](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf)
for distributed snapshots and is specifically tailored to Flink's execution
model.


### Barriers

A core element in Flink's distributed snapshotting are the *stream barriers*.
These barriers are injected into the data stream and flow with the records as
part of the data stream. Barriers never overtake records, they flow strictly in
line.  A barrier separates the records in the data stream into the set of
records that goes into the current snapshot, and the records that go into the
next snapshot. Each barrier carries the ID of the snapshot whose records it
pushed in front of it. Barriers do not interrupt the flow of the stream and are
hence very lightweight. Multiple barriers from different snapshots can be in
the stream at the same time, which means that various snapshots may happen
concurrently.

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/stream_barriers.svg" alt="Checkpoint barriers in data streams" style="width:60%; padding-top:10px; padding-bottom:10px;" />
</div>

Stream barriers are injected into the parallel data flow at the stream sources.
The point where the barriers for snapshot *n* are injected (let's call it
<i>S<sub>n</sub></i>) is the position in the source stream up to which the
snapshot covers the data. For example, in Apache Kafka, this position would be
the last record's offset in the partition. This position <i>S<sub>n</sub></i>
is reported to the *checkpoint coordinator* (Flink's JobManager).

The barriers then flow downstream. When an intermediate operator has received a
barrier for snapshot *n* from all of its input streams, it emits a barrier for
snapshot *n* into all of its outgoing streams. Once a sink operator (the end of
a streaming DAG) has received the barrier *n* from all of its input streams, it
acknowledges that snapshot *n* to the checkpoint coordinator. After all sinks
have acknowledged a snapshot, it is considered completed.

Once snapshot *n* has been completed, the job will never again ask the source
for records from before <i>S<sub>n</sub></i>, since at that point these records
(and their descendant records) will have passed through the entire data flow
topology.

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/stream_aligning.svg" alt="Aligning data streams at operators with multiple inputs" style="width:100%; padding-top:10px; padding-bottom:10px;" />
</div>

Operators that receive more than one input stream need to *align* the input
streams on the snapshot barriers. The figure above illustrates this:

  - As soon as the operator receives snapshot barrier *n* from an incoming
    stream, it cannot process any further records from that stream until it has
    received the barrier *n* from the other inputs as well. Otherwise, it would
    mix records that belong to snapshot *n* and with records that belong to
    snapshot *n+1*.
  - Streams that report barrier *n* are temporarily set aside. Records that are
    received from these streams are not processed, but put into an input
    buffer.
  - Once the last stream has received barrier *n*, the operator emits all
    pending outgoing records, and then emits snapshot *n* barriers itself.
  - After that, it resumes processing records from all input streams,
    processing records from the input buffers before processing the records
    from the streams.

### Snapshotting Operator State

When operators contain any form of *state*, this state must be part of the
snapshots as well.

Operators snapshot their state at the point in time when they have received all
snapshot barriers from their input streams, and before emitting the barriers to
their output streams. At that point, all updates to the state from records
before the barriers will have been made, and no updates that depend on records
from after the barriers have been applied. Because the state of a snapshot may
be large, it is stored in a configurable *[state backend]({{ site.baseurl }}{%
link ops/state/state_backends.md %})*. By default, this is the JobManager's
memory, but for production use a distributed reliable storage should be
configured (such as HDFS). After the state has been stored, the operator
acknowledges the checkpoint, emits the snapshot barrier into the output
streams, and proceeds.

The resulting snapshot now contains:

  - For each parallel stream data source, the offset/position in the stream
    when the snapshot was started
  - For each operator, a pointer to the state that was stored as part of the
    snapshot

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/checkpointing.svg" alt="Illustration of the Checkpointing Mechanism" style="width:100%; padding-top:10px; padding-bottom:10px;" />
</div>

### Asynchronous State Snapshots

Note that the above described mechanism implies that operators stop processing
input records while they are storing a snapshot of their state in the *state
backend*. This *synchronous* state snapshot introduces a delay every time a
snapshot is taken.

It is possible to let an operator continue processing while it stores its state
snapshot, effectively letting the state snapshots happen *asynchronously* in
the background. To do that, the operator must be able to produce a state object
that should be stored in a way such that further modifications to the operator
state do not affect that state object. For example, *copy-on-write* data
structures, such as are used in RocksDB, have this behavior.

After receiving the checkpoint barriers on its inputs, the operator starts the
asynchronous snapshot copying of its state. It immediately emits the barrier to
its outputs and continues with the regular stream processing. Once the
background copy process has completed, it acknowledges the checkpoint to the
checkpoint coordinator (the JobManager). The checkpoint is now only complete
after all sinks have received the barriers and all stateful operators have
acknowledged their completed backup (which may be after the barriers reach the
sinks).

See [State Backends]({{ site.baseurl }}{% link ops/state/state_backends.md %})
for details on the state snapshots.

### Recovery

Recovery under this mechanism is straightforward: Upon a failure, Flink selects
the latest completed checkpoint *k*. The system then re-deploys the entire
distributed dataflow, and gives each operator the state that was snapshotted as
part of checkpoint *k*. The sources are set to start reading the stream from
position <i>S<sub>k</sub></i>. For example in Apache Kafka, that means telling
the consumer to start fetching from offset <i>S<sub>k</sub></i>.

If state was snapshotted incrementally, the operators start with the state of
the latest full snapshot and then apply a series of incremental snapshot
updates to that state.

See [Restart Strategies]({{ site.baseurl }}{% link dev/task_failure_recovery.md
%}#restart-strategies) for more information.

### State Backends

`TODO: expand this section`

The exact data structures in which the key/values indexes are stored depends on
the chosen [state backend]({{ site.baseurl }}{% link
ops/state/state_backends.md %}). One state backend stores data in an in-memory
hash map, another state backend uses [RocksDB](http://rocksdb.org) as the
key/value store.  In addition to defining the data structure that holds the
state, the state backends also implement the logic to take a point-in-time
snapshot of the key/value state and store that snapshot as part of a
checkpoint.

<img src="{{ site.baseurl }}/fig/checkpoints.svg" alt="checkpoints and snapshots" class="offset" width="60%" />

{% top %}

### Savepoints

`TODO: expand this section`

Programs written in the Data Stream API can resume execution from a
**savepoint**. Savepoints allow both updating your programs and your Flink
cluster without losing any state. 

[Savepoints]({{ site.baseurl }}{% link ops/state/savepoints.md %}) are
**manually triggered checkpoints**, which take a snapshot of the program and
write it out to a state backend. They rely on the regular checkpointing
mechanism for this. During execution programs are periodically snapshotted on
the worker nodes and produce checkpoints. For recovery only the last completed
checkpoint is needed and older checkpoints can be safely discarded as soon as a
new one is completed.

Savepoints are similar to these periodic checkpoints except that they are
**triggered by the user** and **don't automatically expire** when newer
checkpoints are completed. Savepoints can be created from the [command line]({{
site.baseurl }}{% link ops/cli.md %}#savepoints) or when cancelling a job via
the [REST API]({{ site.baseurl }}{% link monitoring/rest_api.md
%}#cancel-job-with-savepoint).

{% top %}

### Exactly Once vs. At Least Once

The alignment step may add latency to the streaming program. Usually, this
extra latency is on the order of a few milliseconds, but we have seen cases
where the latency of some outliers increased noticeably. For applications that
require consistently super low latencies (few milliseconds) for all records,
Flink has a switch to skip the stream alignment during a checkpoint. Checkpoint
snapshots are still drawn as soon as an operator has seen the checkpoint
barrier from each input.

When the alignment is skipped, an operator keeps processing all inputs, even
after some checkpoint barriers for checkpoint *n* arrived. That way, the
operator also processes elements that belong to checkpoint *n+1* before the
state snapshot for checkpoint *n* was taken.  On a restore, these records will
occur as duplicates, because they are both included in the state snapshot of
checkpoint *n*, and will be replayed as part of the data after checkpoint *n*.

{% info Note %} Alignment happens only for operators with multiple predecessors
(joins) as well as operators with multiple senders (after a stream
repartitioning/shuffle).  Because of that, dataflows with only embarrassingly
parallel streaming operations (`map()`, `flatMap()`, `filter()`, ...) actually
give *exactly once* guarantees even in *at least once* mode.

{% top %}

## End-to-end Exactly-Once Programs

`TODO: add`

## State and Fault Tolerance in Batch Programs

Flink executes [batch programs](../dev/batch/index.html) as a special case of
streaming programs, where the streams are bounded (finite number of elements).
A *DataSet* is treated internally as a stream of data. The concepts above thus
apply to batch programs in the same way as well as they apply to streaming
programs, with minor exceptions:

  - [Fault tolerance for batch programs](../dev/batch/fault_tolerance.html)
    does not use checkpointing.  Recovery happens by fully replaying the
    streams.  That is possible, because inputs are bounded. This pushes the
    cost more towards the recovery, but makes the regular processing cheaper,
    because it avoids checkpoints.

  - Stateful operations in the DataSet API use simplified in-memory/out-of-core
    data structures, rather than key/value indexes.

  - The DataSet API introduces special synchronized (superstep-based)
    iterations, which are only possible on bounded streams. For details, check
    out the [iteration docs]({{ site.baseurl }}/dev/batch/iterations.html).

{% top %}
