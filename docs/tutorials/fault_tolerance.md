---
title: Fault Tolerance via State Snapshots
nav-id: fault-tolerance
nav-pos: 6
nav-title: Fault Tolerance
nav-parent_id: tutorials
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

* This will be replaced by the TOC
{:toc}

## State Backends

The keyed state managed by Flink is a sort of sharded, key/value store, and the working copy of each
item of keyed state is kept somewhere local to the taskmanager responsible for that key. Operator
state is also local to the machine(s) that need(s) it. Flink periodically takes persistent snapshots
of all the state and copies these snapshots somewhere more durable, such as a distributed file
system.

In the event of the failure, Flink can restore the complete state of your application and resume
processing as though nothing had gone wrong.

This state that Flink manages is stored in a _state backend_. Two implementations of state backends
are available -- one based on RocksDB, an embedded key/value store that keeps its working state on
disk, and another heap-based state backend that keeps its working state in memory, on the Java heap.
This heap-based state backend comes in two flavors: the FsStateBackend that persists its state
snapshots to a distributed file system, and the MemoryStateBackend that uses the JobManager's heap.

<img src="{{ site.baseurl }}/fig/state-backends.png" alt="State backends" class="center" width="90%" />

When working with state kept in a heap-based state backend, accesses and updates involve reading and
writing objects on the heap. But for objects kept in the RocksDBStateBackend, accesses and updates
involve serialization and deserialization, and so are much more expensive. But the amount of state
you can have with RocksDB is limited only by the size of the local disk. Note also that only the
RocksDBStateBackend is able to do incremental snapshotting, which is a significant benefit for
applications with large amounts of slowly changing state.

Both state backends are able to do asynchronous snapshotting, meaning that they can take a snapshot without impeding the ongoing stream processing.

{% top %}

## State Snapshots

### Definitions

* _Snapshot_ -- a generic term referring to a global, consistent image of the state of a Flink job.
  A snapshot includes a pointer into each of the data sources (e.g., an offset into a file or Kafka
  partition), as well as a copy of the state from each of the job's stateful operators that resulted
  from having processed all of the events up to those positions in the sources.
* _Checkpoint_ -- a snapshot taken automatically by Flink for the purpose of being able to recover
  from faults. Checkpoints can be incremental, and are optimized for being restored quickly.
* _Externalized Checkpoint_ -- normally checkpoints are not intended to be manipulated by users.
  Flink retains only the _n_-most-recent checkpoints (_n_ being configurable) while a job is
  running, and deletes them when a job is cancelled. But you can configure them to be retained
  instead, in which case you can manually resume from them.
* _Savepoint_ -- a snapshot triggered manually by a user (or an API call) for some operational
  purpose, such as a stateful redeploy/upgrade/rescaling operation. Savepoints are always complete,
  and are optimized for operational flexibility.

### How does State Snapshotting Work?

Flink uses a variant of the [Chandy-Lamport
algorithm](https://en.wikipedia.org/wiki/Chandy-Lamport_algorithm) known as _asynchronous barrier
snapshotting_.

When a task manager is instructed by the checkpoint coordinator (part of the job manager) to begin a
checkpoint, it has all of the sources record their offsets and insert numbered _checkpoint barriers_
into their streams. These barriers flow through the job graph, indicating the part of the stream
before and after each checkpoint. 

<img src="{{ site.baseurl }}/fig/stream_barriers.svg" alt="Checkpoint barriers are inserted into the streams" class="center" width="80%" />

Checkpoint _n_ will contain the state of each operator that resulted from having consumed **every
event before checkpoint barrier _n_, and none of the events after it**.

As each operator in the job graph receives one of these barriers, it records its state. Operators
with two input streams (such as a CoProcessFunction) perform _barrier alignment_ so that the
snapshot will reflect the state resulting from consuming events from both input streams up to (but
not past) both barriers.

<img src="{{ site.baseurl }}/fig/stream_aligning.svg" alt="Barrier alignment" class="center" width="80%" />

Flink's state backends use a copy-on-write mechanism to allow stream processing to continue
unimpeded while older versions of the state are being asynchronously snapshotted. Only when the
snapshots have been durably persisted will these older versions of the state be garbage collected.

### Exactly Once Guarantees

When things go wrong in a stream processing application, it's possible to have either lost, or
duplicated results. With Flink, depending on the choices you make for your application and the
cluster you run it on, any of these outcomes is possible:

- Flink makes no effort to recover from failures (_at most once_)
- Nothing is lost, but you may experience duplicated results (_at least once_)
- Nothing is lost or duplicated (_exactly once_)

Given that Flink recovers from faults by rewinding and replaying the source data streams, when the
ideal situation is described as **exactly once** this does *not* mean that every event will be
processed exactly once. Instead, it means that _every event will affect the state being managed by
Flink exactly once_. 

If you don't need exactly once semantics, you can gain some performance by disabling barrier
alignment. This is done by configuring Flink to use `CheckpointingMode.AT_LEAST_ONCE`.

### Exactly Once End-to-end

To achieve exactly once end-to-end, so that every event from the sources affects the sinks exactly
once, the following must be true:

1. your sources must be replayable, and
2. your sinks must be transactional (or idempotent)

{% top %}

## Hands-on

The [Flink Operations Playground]({{ site.baseurl }}{% link
getting-started/docker-playgrounds/flink-operations-playground.md %}) includes a section on
[Observing Failure & Recovery]({{ site.baseurl }}{% link
getting-started/docker-playgrounds/flink-operations-playground.md %}#observing-failure--recovery).

{% top %}

## Further Reading

- [Stateful Stream Processing]({{ site.baseurl }}{% link concepts/stateful-stream-processing.md %})
- [State Backends]({{ site.baseurl }}{% link ops/state/state_backends.md %})
- [Fault Tolerance Guarantees of Data Sources and Sinks]({{ site.baseurl }}{% link dev/connectors/guarantees.md %})
- [Enabling and Configuring Checkpointing]({{ site.baseurl }}{% link dev/stream/state/checkpointing.md %})
- [Checkpoints]({{ site.baseurl }}{% link ops/state/checkpoints.md %})
- [Savepoints]({{ site.baseurl }}{% link ops/state/savepoints.md %})
- [Tuning Checkpoints and Large State]({{ site.baseurl }}{% link ops/state/large_state_tuning.md %})
- [Monitoring Checkpointing]({{ site.baseurl }}{% link monitoring/checkpoint_monitoring.md %})
- [Task Failure Recovery]({{ site.baseurl }}{% link dev/task_failure_recovery.md %})

{% top %}
