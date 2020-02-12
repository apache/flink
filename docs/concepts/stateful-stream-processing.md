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
