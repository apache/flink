---
title: Fault Tolerance
weight: 7
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

# Fault Tolerance via State Snapshots

## State Backends

The keyed state managed by Flink is a sort of sharded, key/value store, and the working copy of each
item of keyed state is kept somewhere local to the taskmanager responsible for that key. Operator
state is also local to the machine(s) that need(s) it.

This state that Flink manages is stored in a _state backend_. 
Two implementations of state backends are available -- one based on RocksDB, an embedded key/value store that keeps its working state on
disk, and another heap-based state backend that keeps its working state in memory, on the Java heap.

<center>
  <table class="table table-bordered">
    <thead>
      <tr class="book-hint info">
        <th class="text-left">Name</th>
        <th class="text-left">Working State</th>
        <th class="text-left">Snapshotting</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <th class="text-left">EmbeddedRocksDBStateBackend</th>
        <td class="text-left">Local disk (tmp dir)</td>
        <td class="text-left">Full / Incremental</td>
      </tr>
      <tr>
        <td colspan="4" class="text-left">
          <ul>
            <li>Supports state larger than available memory</li>
            <li>Rule of thumb: 10x slower than heap-based backends</li>
          </ul>
        </td>
      </tr>
      <tr>
        <th class="text-left">HashMapStateBackend</th>
        <td class="text-left">JVM Heap</td>
        <td class="text-left">Full</td>
      </tr>
      <tr>
        <td colspan="4" class="text-left">
          <ul>
            <li>Fast, requires large heap</li>
            <li>Subject to GC</li>
          </ul>
        </td>
      </tr>
    </tbody>
  </table>
</center>

When working with state kept in a heap-based state backend, accesses and updates involve reading and
writing objects on the heap. But for objects kept in the `EmbeddedRocksDBStateBackend`, accesses and updates
involve serialization and deserialization, and so are much more expensive. But the amount of state
you can have with RocksDB is limited only by the size of the local disk. Note also that only the
`EmbeddedRocksDBStateBackend` is able to do incremental snapshotting, which is a significant benefit for
applications with large amounts of slowly changing state.

Both of these state backends are able to do asynchronous snapshotting, meaning that they can take a
snapshot without impeding the ongoing stream processing.

## Checkpoint Storage

Flink periodically takes persistent snapshots of all the state in every operator and copies these snapshots somewhere more durable, such as a distributed file system. In the event of the failure, Flink can restore the complete state of your application and resume
processing as though nothing had gone wrong.

The location where these snapshots are stored is defined via the jobs _checkpoint storage_.
Two implementations of checkpoint storage are available - one that persists its state snapshots
to a distributed file system, and another that users the JobManager's heap. 

<center>
  <table class="table table-bordered">
    <thead>
      <tr class="book-hint info">
        <th class="text-left">Name</th>
        <th class="text-left">State Backup</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <th class="text-left">FileSystemCheckpointStorage</th>
        <td class="text-left">Distributed file system</td>
      </tr>
      <tr>
        <td colspan="4" class="text-left">
          <ul>
            <li>Supports very large state size</li>
            <li>Highly durable</li>
            <li>Recommended for production deployments</li>
          </ul>
        </td>
      </tr>
      <tr>
        <th class="text-left">JobManagerCheckpointStorage</th>
        <td class="text-left">JobManager JVM Heap</td>
      </tr>
      <tr>
        <td colspan="4" class="text-left">
          <ul>
            <li>Good for testing and experimentation with small state (locally)</li>
          </ul>
        </td>
      </tr>
    </tbody>
  </table>
</center>

{{< top >}}

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

Flink uses a variant of the [Chandy-Lamport algorithm](https://en.wikipedia.org/wiki/Chandy-Lamport_algorithm) known as _asynchronous barrier
snapshotting_.

When a task manager is instructed by the checkpoint coordinator (part of the job manager) to begin a
checkpoint, it has all of the sources record their offsets and insert numbered _checkpoint barriers_
into their streams. These barriers flow through the job graph, indicating the part of the stream
before and after each checkpoint. 

{{< img src="/fig/stream_barriers.svg" alt="Checkpoint barriers are inserted into the streams" width="80%" >}}

Checkpoint _n_ will contain the state of each operator that resulted from having consumed **every
event before checkpoint barrier _n_, and none of the events after it**.

As each operator in the job graph receives one of these barriers, it records its state. Operators
with two input streams (such as a `CoProcessFunction`) perform _barrier alignment_ so that the
snapshot will reflect the state resulting from consuming events from both input streams up to (but
not past) both barriers.

{{< img src="/fig/stream_aligning.svg" alt="Barrier alignment" width="100%" >}}

Flink's state backends use a copy-on-write mechanism to allow stream processing to continue
unimpeded while older versions of the state are being asynchronously snapshotted. Only when the
snapshots have been durably persisted will these older versions of the state be garbage collected.

### Exactly Once Guarantees

When things go wrong in a stream processing application, it is possible to have either lost, or
duplicated results. With Flink, depending on the choices you make for your application and the
cluster you run it on, any of these outcomes is possible:

- Flink makes no effort to recover from failures (_at most once_)
- Nothing is lost, but you may experience duplicated results (_at least once_)
- Nothing is lost or duplicated (_exactly once_)

Given that Flink recovers from faults by rewinding and replaying the source data streams, when the
ideal situation is described as **exactly once** this does *not* mean that every event will be
processed exactly once. Instead, it means that _every event will affect the state being managed by
Flink exactly once_. 

Barrier alignment is only needed for providing exactly once guarantees. If you don't need this, you
can gain some performance by configuring Flink to use `CheckpointingMode.AT_LEAST_ONCE`, which has
the effect of disabling barrier alignment.

### Exactly Once End-to-end

To achieve exactly once end-to-end, so that every event from the sources affects the sinks exactly
once, the following must be true:

1. your sources must be replayable, and
2. your sinks must be transactional (or idempotent)

{{< top >}}

## Hands-on

The [Flink Operations Playground]({{< ref "docs/try-flink/flink-operations-playground" >}}) includes a section on
[Observing Failure & Recovery]({{< ref "docs/try-flink/flink-operations-playground" >}}#observing-failure--recovery).

{{< top >}}

## Further Reading

- [Stateful Stream Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})
- [State Backends]({{< ref "docs/ops/state/state_backends" >}})
- [Fault Tolerance Guarantees of Data Sources and Sinks]({{< ref "docs/connectors/datastream/guarantees" >}})
- [Enabling and Configuring Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})
- [Checkpoints]({{< ref "docs/ops/state/checkpoints" >}})
- [Savepoints]({{< ref "docs/ops/state/savepoints" >}})
- [Tuning Checkpoints and Large State]({{< ref "docs/ops/state/large_state_tuning" >}})
- [Monitoring Checkpointing]({{< ref "docs/ops/monitoring/checkpoint_monitoring" >}})
- [Task Failure Recovery]({{< ref "docs/ops/state/task_failure_recovery" >}})

{{< top >}}
