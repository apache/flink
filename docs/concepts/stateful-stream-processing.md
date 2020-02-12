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

## Stateful Operations

While many operations in a dataflow simply look at one individual *event at a
time* (for example an event parser), some operations remember information
across multiple events (for example window operators).  These operations are
called **stateful**.

* This will be replaced by the TOC
{:toc}

### What is State?

{% top %}

## State in Stream & Batch Processing

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

{% top %}

## State Types (Keyed State, Broadcast State)

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

The description of the [fault tolerance internals]({{ site.baseurl
}}/internals/stream_checkpointing.html) provides more information about how
Flink manages checkpoints and related topics.  Details about enabling and
configuring checkpointing are in the [checkpointing API
docs](../dev/stream/state/checkpointing.html).


{% top %}

### Asynchronous Barrier Snapshots
### Recovery
### State Backends (Working Data Structure vs Checkpoint Storage; not describing the existing backends though)

{% top %}

## Delivery Guarantees & “Exactly Once Semantics”

###End-to-end exactly once semantics

{% top %}
