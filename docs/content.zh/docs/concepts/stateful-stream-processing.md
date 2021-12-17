---
title: 有状态流处理
weight: 2
type: docs
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

# 有状态流处理

## What is State?

While many operations in a dataflow simply look at one individual *event at a
time* (for example an event parser), some operations remember information
across multiple events (for example window operators). These operations are
called **stateful**.

Some examples of stateful operations:

  - When an application searches for certain event patterns, the state will
    store the sequence of events encountered so far.
  - When aggregating events per minute/hour/day, the state holds the pending
    aggregates.
  - When training a machine learning model over a stream of data points, the
    state holds the current version of the model parameters.
  - When historic data needs to be managed, the state allows efficient access
    to events that occurred in the past.

Flink needs to be aware of the state in order to make it fault tolerant using
[checkpoints]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})
and [savepoints]({{< ref "docs/ops/state/savepoints" >}}).

Knowledge about the state also allows for rescaling Flink applications, meaning
that Flink takes care of redistributing state across parallel instances.

[Queryable state]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) allows you to access state from outside of Flink during runtime.

When working with state, it might also be useful to read about [Flink's state
backends]({{< ref "docs/ops/state/state_backends" >}}). Flink
provides different state backends that specify how and where state is stored.

{{< top >}}

## Keyed State

Keyed state is maintained in what can be thought of as an embedded key/value
store.  The state is partitioned and distributed strictly together with the
streams that are read by the stateful operators. Hence, access to the key/value
state is only possible on *keyed streams*, i.e. after a keyed/partitioned data
exchange, and is restricted to the values associated with the current event's
key. Aligning the keys of streams and state makes sure that all state updates
are local operations, guaranteeing consistency without transaction overhead.
This alignment also allows Flink to redistribute the state and adjust the
stream partitioning transparently.

{{< img src="/fig/state_partitioning.svg" alt="State and Partitioning" class="offset" width="50%" >}}

Keyed State is further organized into so-called *Key Groups*. Key Groups are
the atomic unit by which Flink can redistribute Keyed State; there are exactly
as many Key Groups as the defined maximum parallelism.  During execution each
parallel instance of a keyed operator works with the keys for one or more Key
Groups.

## State Persistence

Flink implements fault tolerance using a combination of **stream replay** and
**checkpointing**. A checkpoint marks a specific point in each of the
input streams along with the corresponding state for each of the operators. A
streaming dataflow can be resumed from a checkpoint while maintaining
consistency *(exactly-once processing semantics)* by restoring the state of the
operators and replaying the records from the point of the checkpoint.

The checkpoint interval is a means of trading off the overhead of fault
tolerance during execution with the recovery time (the number of records that
need to be replayed).

The fault tolerance mechanism continuously draws snapshots of the distributed
streaming data flow. For streaming applications with small state, these
snapshots are very light-weight and can be drawn frequently without much impact
on performance.  The state of the streaming applications is stored at a
configurable place, usually in a distributed file system.

In case of a program failure (due to machine-, network-, or software failure),
Flink stops the distributed streaming dataflow.  The system then restarts the
operators and resets them to the latest successful checkpoint. The input
streams are reset to the point of the state snapshot. Any records that are
processed as part of the restarted parallel dataflow are guaranteed to not have
affected the previously checkpointed state.

{{< hint warning >}}
By default, checkpointing is disabled. See [Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) for details on how to enable and configure checkpointing.
{{< /hint >}}

{{< hint info >}}
For this mechanism to realize its full guarantees, the data
stream source (such as message queue or broker) needs to be able to rewind the
stream to a defined recent point. [Apache Kafka](http://kafka.apache.org) has
this ability and Flink's connector to Kafka exploits this. See [Fault
Tolerance Guarantees of Data Sources and Sinks]({{< ref "docs/connectors/datastream/guarantees" >}}) for more information about the guarantees
provided by Flink's connectors.
{{< /hint >}}

{{< hint info >}} 
Because Flink's checkpoints are realized through distributed
snapshots, we use the words *snapshot* and *checkpoint* interchangeably. Often
we also use the term *snapshot* to mean either *checkpoint* or *savepoint*.
{{< /hint >}}

### Checkpointing

The central part of Flink's fault tolerance mechanism is drawing consistent
snapshots of the distributed data stream and operator state.  These snapshots
act as consistent checkpoints to which the system can fall back in case of a
failure. Flink's mechanism for drawing these snapshots is described in
"[Lightweight Asynchronous Snapshots for Distributed
Dataflows](http://arxiv.org/abs/1506.08603)". It is inspired by the standard
[Chandy-Lamport algorithm](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf)
for distributed snapshots and is specifically tailored to Flink's execution
model.

Keep in mind that everything to do with checkpointing can be done
asynchronously. The checkpoint barriers don't travel in lock step and
operations can asynchronously snapshot their state.

Since Flink 1.11, checkpoints can be taken with or without alignment. In this 
section, we describe aligned checkpoints first.

#### Barriers

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
  {{< img src="/fig/stream_barriers.svg" alt="Checkpoint barriers in data streams" width="60%" >}}
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
  {{< img src="/fig/stream_aligning.svg" alt="Aligning data streams at operators with multiple inputs" width="60%" >}}
</div>

Operators that receive more than one input stream need to *align* the input
streams on the snapshot barriers. The figure above illustrates this:

  - As soon as the operator receives snapshot barrier *n* from an incoming
    stream, it cannot process any further records from that stream until it has
    received the barrier *n* from the other inputs as well. Otherwise, it would
    mix records that belong to snapshot *n* and with records that belong to
    snapshot *n+1*.
  - Once the last stream has received barrier *n*, the operator emits all
    pending outgoing records, and then emits snapshot *n* barriers itself.
  - It snapshots the state and resumes processing records from all input streams,
    processing records from the input buffers before processing the records
    from the streams.
  - Finally, the operator writes the state asynchronously to the state backend.
  
Note that the alignment is needed for all operators with multiple inputs and for 
operators after a shuffle when they consume output streams of multiple upstream 
subtasks.

#### Snapshotting Operator State

When operators contain any form of *state*, this state must be part of the
snapshots as well.

Operators snapshot their state at the point in time when they have received all
snapshot barriers from their input streams, and before emitting the barriers to
their output streams. At that point, all updates to the state from records
before the barriers have been made, and no updates that depend on records
from after the barriers have been applied. Because the state of a snapshot may
be large, it is stored in a configurable *[state backend]({{< ref "docs/ops/state/state_backends" >}})*. By default, this is the JobManager's
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
  {{< img src="/fig/checkpointing.svg" alt="Illustration of the Checkpointing Mechanism" width="75%" >}}
</div>

#### Recovery

Recovery under this mechanism is straightforward: Upon a failure, Flink selects
the latest completed checkpoint *k*. The system then re-deploys the entire
distributed dataflow, and gives each operator the state that was snapshotted as
part of checkpoint *k*. The sources are set to start reading the stream from
position <i>S<sub>k</sub></i>. For example in Apache Kafka, that means telling
the consumer to start fetching from offset <i>S<sub>k</sub></i>.

If state was snapshotted incrementally, the operators start with the state of
the latest full snapshot and then apply a series of incremental snapshot
updates to that state.

See [Restart Strategies]({{< ref "docs/ops/state/task_failure_recovery" >}}#restart-strategies) for more information.

### Unaligned Checkpointing

Checkpointing can also be performed unaligned.
The basic idea is that checkpoints can overtake all in-flight data as long as 
the in-flight data becomes part of the operator state.

Note that this approach is actually closer to the [Chandy-Lamport algorithm
](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf), but
Flink still inserts the barrier in the sources to avoid overloading the
checkpoint coordinator.

{{< img src="/fig/stream_unaligning.svg" alt="Unaligned checkpointing" >}}

The figure depicts how an operator handles unaligned checkpoint barriers:

- The operator reacts on the first barrier that is stored in its input buffers.
- It immediately forwards the barrier to the downstream operator by adding it 
  to the end of the output buffers.
- The operator marks all overtaken records to be stored asynchronously and 
  creates a snapshot of its own state.
 
Consequently, the operator only briefly stops the processing of input to mark
the buffers, forwards the barrier, and creates the snapshot of the other state.
  
Unaligned checkpointing ensures that barriers are arriving at the sink as fast 
as possible. It's especially suited for applications with at least one slow 
moving data path, where alignment times can reach hours. However, since it's
adding additional I/O pressure, it doesn't help when the I/O to the state 
backends is the bottleneck. See the more in-depth discussion in 
[ops]({{< ref "docs/ops/state/checkpoints" >}}#unaligned-checkpoints)
for other limitations.

Note that savepoints will always be aligned.

#### Unaligned Recovery

Operators first recover the in-flight data before starting processing any data
from upstream operators in unaligned checkpointing. Aside from that, it 
performs the same steps as during [recovery of aligned checkpoints](#recovery).

### State Backends

The exact data structures in which the key/values indexes are stored depends on
the chosen [state backend]({{< ref "docs/ops/state/state_backends" >}}). One state backend stores data in an in-memory
hash map, another state backend uses [RocksDB](http://rocksdb.org) as the
key/value store.  In addition to defining the data structure that holds the
state, the state backends also implement the logic to take a point-in-time
snapshot of the key/value state and store that snapshot as part of a
checkpoint. State backends can be configured without changing your application
logic.

{{< img src="/fig/checkpoints.svg" alt="checkpoints and snapshots" class="offset" width="60%" >}}

{{< top >}}

### Savepoints

All programs that use checkpointing can resume execution from a **savepoint**.
Savepoints allow both updating your programs and your Flink cluster without
losing any state.

[Savepoints]({{< ref "docs/ops/state/savepoints" >}}) are
**manually triggered checkpoints**, which take a snapshot of the program and
write it out to a state backend. They rely on the regular checkpointing
mechanism for this.

Savepoints are similar to checkpoints except that they are
**triggered by the user** and **don't automatically expire** when newer
checkpoints are completed.

{{< top >}}

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

{{< hint info >}}
Alignment happens only for operators with multiple predecessors
(joins) as well as operators with multiple senders (after a stream
repartitioning/shuffle).  Because of that, dataflows with only embarrassingly
parallel streaming operations (`map()`, `flatMap()`, `filter()`, ...) actually
give *exactly once* guarantees even in *at least once* mode.
{{< /hint >}}

{{< top >}}

## State and Fault Tolerance in Batch Programs

Flink executes [batch programs]({{< ref "docs/dev/dataset/overview" >}}) as a special case of
streaming programs, where the streams are bounded (finite number of elements).
A *DataSet* is treated internally as a stream of data. The concepts above thus
apply to batch programs in the same way as well as they apply to streaming
programs, with minor exceptions:

  - [Fault tolerance for batch programs]({{< ref "docs/ops/state/task_failure_recovery" >}})
    does not use checkpointing.  Recovery happens by fully replaying the
    streams.  That is possible, because inputs are bounded. This pushes the
    cost more towards the recovery, but makes the regular processing cheaper,
    because it avoids checkpoints.

  - Stateful operations in the DataSet API use simplified in-memory/out-of-core
    data structures, rather than key/value indexes.

  - The DataSet API introduces special synchronized (superstep-based)
    iterations, which are only possible on bounded streams. For details, check
    out the [iteration docs]({{< ref "docs/dev/dataset/iterations" >}}).

{{< top >}}
