---
title: "Glossary"
type: docs
weight: 5
bookToc: false
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

# Glossary

#### Aggregation

Aggregation is an operation that takes multiple values and returns a single value. When working with 
streams, it generally makes more sense to think in terms of aggregations over finite windows, rather 
than over the entire stream.

#### (Flink) Application

A Flink application is any user program that submits one or multiple [Flink Jobs](#flink-job) from its
`main()` method. The execution of these jobs can happen in a local JVM or on a remote setup of clusters 
with multiple machines.

The jobs of an application can either be submitted to a long-running [Session Cluster](#session-cluster),
to a dedicated [Application Cluster](#application-cluster), or to a [Job Cluster](#job-cluster).

#### Application Cluster

A Flink application cluster is a dedicated [Flink cluster](#(flink)-cluster) that only executes 
[Flink jobs](#flink-job) from one [Flink application](#(flink)-application). The lifetime of the Flink
cluster is bound to the lifetime of the Flink application.

#### Asynchronous Snapshotting

A form of [snapshotting](#snapshot) that doesn't impede the ongoing stream processing by allowing an 
operator to continue processing while it stores its state snapshot, effectively letting the state 
snapshots happen asynchronously in the background.

#### At-least-once

A fault-tolerance guarantee and data delivery approach where multiple attempts are made at delivering
an event such that at least one succeeds. This guarantees that nothing is lost, but you may experience 
duplicated results.

#### At-most-once

A data delivery approach where each event is delivered zero or one times. There is lower latency but
events may be lost.

#### Backpressure

A situation where a system is receiving data at a higher rate than it can process during a temporary 
load spike.

#### Barrier Alignment

For providing exactly-once guarantees, Flink aligns the streams at operators that receive multiple 
input streams, so that the snapshot will reflect the state resulting from consuming events from both 
input streams up to (but not past) both barriers. 

#### Batch Processing

When the processing and analysis happens on a set of data that have already been stored over a period 
of time. The results are usually not available in real-time. Flink executes batch programs as a special 
case of streaming programs.

#### Bounded Streams

#### Category

#### Checkpoint

A [snapshot](#snapshot) taken automatically by Flink for the purpose of being able to recover from 
faults. A checkpoint marks a specific point in each of the input streams along with the corresponding 
state for each of the operators. Checkpoints can be incremental and unaligned, and are optimized for 
being restored quickly.

#### Checkpoint Barrier

A special marker that flows along the graph and triggers the checkpointing process on each of the 
parallel instances of the operators. Checkpoint barriers are injected into the source operators and 
flow together with the data. If an operator has multiple outputs, it gets "split" into both of them.

#### Checkpoint Coordinator

This coordinates the distributed snapshots of operators and state. It is part of the JobManager and 
instructs the TaskManager when to begin a checkpoint by sending the messages to the relevant tasks 
and collecting the checkpoint acknowledgements.

#### Checkpoint Storage

The location where the [state backend](#state-backend) will store its snapshot during a checkpoint. 
This could be on the Java heap of the [JobManager](#flink-jobmanager) or on a file system.

#### (Flink) Client

This is not part of the runtime and program execution but is used to prepare and send a dataflow graph 
to the JobManager. The Flink client runs either as part of the program that triggers the execution or 
in the command line process via `./bin/flink run`.

#### (Flink) Cluster

A distributed system consisting of (typically) one [JobManager](#jobmanager) and one or more
[TaskManager](#taskmanager) processes.

#### Connected Streams

A pattern in Flink where a single operator has two input streams. Connected streams can also be used 
to implement streaming joins.

#### Connectors

#### Data Parallelism

#### Dataflow

See [logical graph](#logical-graph).

#### DataStream

This is a collection of data in a Flink application. You can think of them as immutable collections 
of data that can contain duplicates. This data can either be finite or unbounded.

#### Directed Acyclic Graph (DAG)

#### Dispatcher

This is a component of the [JobManager](#jobmanager) and provides a REST interface to submit Flink 
applications for execution and starts a new [JobMaster](#jobmaster) for each submitted job. It also 
runs the Flink web UI to provide information about job executions.

#### Event

An event is a statement about a change of the state of the domain modelled by the application. Events
can be input and/or output of a stream processing application. Events are special types of
[records](#Record).

#### Event Time

The time when an [event](#event) occurred, as recorded by the device producing (or storing) the event.
For reproducible results, you should use event time because the result does not depend on when the 
calculation is performed.

If you want to use event time, you will also need to supply a Timestamp Extractor and Watermark Generator 
that Flink will use to track the progress of event time.

#### Exactly-once

A fault-tolerance guarantee and data delivery approach where nothing is lost or duplicated. This does 
not mean that every event will be processed exactly once. Instead, it means that every event will affect 
the state being managed by Flink exactly once.

#### ExecutionGraph

See [Physical Graph](#physical-graph).

#### Externalized Checkpoint

A checkpoint that is configured to be retained instead of being deleted when a job is cancelled. 
Flink normally retains only the n-most-recent checkpoints (n being configurable) while a job is running 
and deletes them when a job is cancelled. 

You can manually resume from an externalized checkpoint. 

#### Ingestion Time

A timestamp recorded by Flink at the moment it ingests the event.

#### (Flink) Job

This is the runtime representation of a [logical graph](#logical-graph) (also often called dataflow
graph) that is created and submitted by calling `execute()` in a [Flink application](#flink-application).

#### Job Cluster

This is a dedicated [Flink cluster](#(flink)-cluster) that only executes a single [Flink job](#(flink)-job). 
The lifetime of the Flink cluster is bound to the lifetime of the Flink job. This deployment mode has 
been deprecated since Flink 1.15.

#### JobGraph

See [Logical Graph](#logical-graph).

#### JobManager

The JobManager is the orchestrator of a [Flink cluster](#(flink)-cluster). It contains three distinct
components: ResourceManager, Dispatcher, and a [JobMaster](#jobmaster) per running [Flink job](#(flink)-job).

There is always at least one JobManager. A high-availability setup might have multiple JobManagers, 
one of which is always the leader.

#### JobMaster

This is one of the components that run in the [JobManager](#jobmanager). It is responsible for supervising 
the execution of the [tasks](#task) of a single [job](#(flink)-job). Multiple jobs can run simultaneously 
in a [Flink cluster](#(flink)-cluster), each having its own JobMaster.

#### JobResultStore

The JobResultStore is a Flink component that persists the results of globally terminated (i.e. finished, 
cancelled or failed) jobs to a filesystem, allowing the results to outlive a finished job. These results 
are then used by Flink to determine whether jobs should be subject to recovery in highly-available clusters.

#### Key Group

These are the atomic unit by which Flink can redistribute [keyed state](#keyed-state). There are 
exactly as many key groups as the defined maximum parallelism. During execution, each parallel instance 
of a keyed operator works with the keys for one or more key groups.

#### Keyed State

Keyed state is one of the two basic types of state in Apache Flink (the other being operator state).
In order to have all events with the same value of an attribute grouped together, you can partition 
a stream around that attribute, and maintain it as an embedded key/value store. This results in a keyed
state. 

A keyed state is always bound to keys and is only available to functions and operators that process
data from a keyed stream.

Flink supports several different types of keyed state, with the simplest one being [ValueState](#valuestate).

#### Keyed Stream

A keyed stream is a [DataStream](#DataStream) on which [operator state](#operator-state) is partitioned 
by a key. Typical operations supported by a DataStream are also possible on a keyed stream, except for 
partitioning methods such as shuffle, forward, and keyBy.

#### Lateness

Lateness is defined relative to the [watermarks](#watermark). A watermark(t) asserts that the stream 
is complete up through to time t. Any event is considered late if it comes after the watermark whose 
timestamp is ≤ t.

#### ListState<T>

This is a type of [keyed state](#keyed-state) that keeps a list of elements. You can append elements 
and retrieve an Iterable over all currently stored elements. Elements are added using add(T) or 
addAll(List<T>). The Iterable can be retrieved using Iterable<T> get().

#### Logical Graph

This is a directed graph where the nodes are [operators](#operator) and the edges define input/output 
relationships of the operators and correspond to [DataStreams](#datastreams). A logical graph is created 
by submitting jobs to a [Flink cluster](#(flink)-cluster) from a [Flink application](#(flink)-application).

Logical graphs are also often referred to as [dataflow](#dataflow).

#### Managed State

Managed state is application state which has been registered with the stream processing framework, 
which will take care of the persistence and rescaling of this state.  

This type of state is represented in data structures controlled by the Flink runtime, such as internal 
hash tables, or RocksDB. Flink’s runtime encodes the states and writes them into the checkpoints.

[Keyed state](#keyed-state) and [operator state](#operator-state) exist in two forms: managed and [raw](#raw-state).

#### MapState<UK, UV>

This is a type of [keyed state](#keyed-state) that keeps a list of mappings. You can put key-value 
pairs into the state and retrieve an Iterable over all currently stored mappings. Mappings are added 
using put(UK, UV) or putAll(Map<UK, UV>). The value associated with a key can be retrieved using get(UK).

#### Non-keyed State

This type of state is bound to one parallel operator instance and is also called [operator state](#operator-state). 

It is possible to work with [managed state](#managed-state) in non-keyed contexts but it is unusual 
for user-defined functions to need non-keyed state and the interfaces involved would be different. 

This feature is most often used in the implementation of [sources](#source) and [sinks](#sink).

#### Offset

A number identifying how far you are from the beginning of a certain [DataStream](#datastream). 

#### Operator

An operator is a node of a [logical graph](#logical-graph). An operator performs a certain operation, 
which is usually executed by a [function](#function). Sources and sinks are special operators for data
ingestion and data egress.

#### Operator Chain

An operator chain consists of two or more consecutive [operators](#operator) without any
repartitioning in between. Operators within the same operator chain forward records to each other
directly without going through serialization or Flink's network stack. This is a useful optimization
and increases overall throughput while decreasing latency. The chaining behavior can be configured.

#### Operator State

#### Parallelism 

#### Partition

A partition is an independent subset of the overall [DataStream](#datastream). A DataStream is divided 
into partitions by assigning each [record](#record) to one or more partitions via keys. Partitions of 
DataStreams are consumed by [tasks](#task) during runtime. A transformation that changes the way a 
DataStream is partitioned is often called repartitioning.

#### Physical Graph

A physical graph is the result of translating a [logical graph](#logical-graph) for execution in a
distributed runtime. The nodes are [tasks](#task) and the edges indicate input/output relationships
or [partitions](#partition) of DataStreams.

#### POJO

This is a composite data type and can be serialized with Flink's serializer. Flink recognizes a data 
type as a POJO type (and allows “by-name” field referencing) if the following conditions are met:

- the class is public and standalone (no non-static inner class)
- the class has a public no-argument constructor
- all non-static, non-transient fields in the class (and all superclasses) are either public (and 
  non-final) or have public getter- and setter- methods that follow the Java naming conventions for 
  getters and setters

#### Process Functions

This type of function combines event processing with timers and state and is the basis for creating 
event-driven applications with Flink.

#### Processing Time

The time when a specific operator in your pipeline is processing the event. Computing analytics based 
on processing time can cause inconsistencies and make it difficult to re-analyze historic data or test 
new implementations.

#### Queryable State 

This is managed keyed (partitioned) state that can be accessed from outside of Flink during runtime.

#### Raw State

This is state that operators keep in their own data structures. When checkpointed, only a sequence of 
bytes is written into the checkpoint and Flink knows nothing about the state’s data structures and will 
see only the raw bytes.

[Keyed state](#keyed-state) and [operator state](#operator-state) exist in two forms: [managed](#managed-state) and raw.

#### Record

Records are the elements that make up a [DataStream](#datastream). [Operators](#operator) and [functions](#function) 
receive records as input and emit records as output.

#### ResourceManager

This is part of the [JobManager](#JobManager) and is responsible for resource de-/allocation and 
provisioning in a Flink cluster.

#### Rich Functions

A RichFunction is a "rich" variant of Flink's function interfaces for data transformation. These functions 
have some additional methods needed for working with managed keyed state such as `open(Configuration c)`, 
`close()`, `getRuntimeContext()`.

#### Rolling Total

The sum of a sequence of numbers which is updated each time a new number is added to the sequence, 
by adding the value of the new number to the previous rolling total.

#### (Runtime) Execution Mode

DataStream API programs can be executed in one of two execution modes: `BATCH` or `STREAMING`. 
See [Execution Mode]({{< ref "/docs/dev/datastream/execution_mode" >}}) for more details.

#### Savepoint

A [snapshot](#snapshot) triggered manually by a user (or an API call) for some operational purpose, 
such as a stateful redeploy/upgrade/rescaling. Savepoints are always complete and aligned and are 
optimized for operational flexibility.

#### Schema

#### Serialization

#### Session Cluster

A long-running [Flink cluster](#(flink)-cluster) which accepts multiple [Flink jobs](#(flink)-job) for
execution. The lifetime of this cluster is not bound to the lifetime of any Flink job. Formerly, a 
Session Cluster was also known as a Flink Cluster in *session mode*. 

#### Session Windows

This is a [window](#window) that groups elements by sessions of activity. Session windows do not overlap 
and do not have a fixed start and end time, in contrast to [tumbling windows](#tumbling-window) and 
[sliding windows](#sliding-window). A session window closes when it does not receive elements for a 
certain period of time (i.e. when a gap of inactivity occurred).

#### Shuffling

#### Side Outputs

This is an extra output stream from a Flink operator. Beyond error reporting, side outputs are also 
a good way to implement an n-way split of a stream.

#### Sink

#### Sliding Window

This is a [window](#window) that groups elements to windows of fixed length. Similar to [tumbling windows](#tumbling-window), 
the size of sliding windows are configured by the window size parameter. An additional window slide 
parameter controls how frequently a sliding window is started. Hence, sliding windows can be overlapping 
if the slide is smaller than the window size. In this case, elements are assigned to multiple windows.

#### Snapshot

A generic term referring to a global, consistent image of the state of a [Flink job](#(flink)-job). 
A snapshot can be full or incremental and includes a pointer into each of the data sources as well as 
a copy of the state from each of the job’s stateful operators that resulted from having processed all 
the events up to those positions in the sources.

Flink periodically takes persistent snapshots of all the state in every operator and copies these 
snapshots somewhere more durable, such as a distributed file system.

Flink uses a variant of the Chandy-Lamport algorithm known as asynchronous barrier snapshotting.

#### Source

#### Spilling

#### State Backend

For stream processing programs, the state backend of a [Flink job](#(flink)-job) determines how its
[state](#managed-state) is stored on each [TaskManager](#taskmanager).

Two implementations of state backends are available. One is based on RocksDB, an embedded key/value 
store that keeps its working state on disk, and the other is heap-based that keeps its working state 
in memory, on the Java heap.

#### Stream Barriers

A core element of Flink's distributed snapshotting. Stream barriers are injected into the [DataStream](#datastream) 
and flow with the [records](#record) as part of the DataStream. Barriers never overtake records and 
flow strictly in line. A barrier separates the records in the DataStream into the set of records that 
goes into the current snapshot, and the records that go into the next snapshot.

#### Subtask

A subtask is a [task](#task) responsible for processing a [partition](#partition) of the [DataStream](#datastream). 
The term "subtask" emphasizes that there are multiple parallel tasks for the same [operator](#operator) 
or [operator chain](#operator-chain).

#### Table Program

A generic term for pipelines declared with Flink's relational APIs (Table API or SQL).

#### Task

This is a node in a [physical graph](#physical-graph). A task is the basic unit of work which is executed 
by Flink's runtime. Tasks encapsulate exactly one parallel instance of an [operator](#operator) or
[operator chain](#operator-chain).

#### Task Chaining

#### Task Parallelism

#### Task Slot

This is one unit of resource scheduling in a [Flink cluster](#(flink)-cluster). Each task slot 
represents a fixed subset of resources of the [TaskManager](#taskmanager). The number of task slots 
in a TaskManager indicates the number of concurrent processing tasks.

#### TaskManager

TaskManagers are the worker processes of a [Flink cluster](#flink-cluster), execute the tasks of a 
dataflow, and buffer and exchange the [DataStreams](#datastreams). They connect to [JobManagers](#jobmanagers), 
announce themselves as available, and are assigned work. [Tasks](#task) are scheduled to TaskManagers 
for execution. They communicate with each other to exchange data between subsequent tasks. Each TaskManager 
is a JVM process and may execute one or more subtasks in separate threads.

There must always be at least one TaskManager. The smallest unit of resource scheduling in a TaskManager 
is a [task slot](#task-slot). 

#### Timer

Timers allow applications to react to changes in [processing time](#processing-time) and in [event time](#event-time).
There are at most one timer per key and per second.

Timers are fault-tolerant and checkpointed along with the state of the application. In case of a failure 
recovery or when starting an application from a [savepoint](#savepoint), timers are restored.

#### Transformation

A transformation is applied to one or more [DataStreams](#datastreams) and results in one or more 
output DataStreams. A transformation might change a DataStream on a [per-record](#record) basis, but 
might also only change its [partitioning](#partition) or perform an [aggregation](#aggregation). While 
[operators](#operator) and [functions](#function) are the "physical" parts of Flink's API, transformations 
are an API concept. Specifically, most transformations are implemented by certain [operators](#operator).

#### Tumbling Window

This is a [window](#window) that groups elements by a specified window size. Tumbling windows have a 
fixed size and do not overlap. For example, if you specify a tumbling window with a size of 5 minutes, 
the current window will be evaluated and a new window will be started every five minutes.

#### Tuple

A composite data type that has a finite ordered list of immutable elements. 

#### Unbounded streams

#### (User-Defined) Functions

Functions are implemented by the user and encapsulate the application logic of a [Flink application](#(flink)-application). 
Most functions are wrapped by a corresponding [operator](#operator).

#### User-Defined Aggregate Function (UDAF)

#### User-Defined Scalar Function (UDSF)

#### User-Defined Table-valued Function (UDTF)

#### ValueState<T>

This is a type of [keyed state](#keyed-state) where Flink will store a single object for each key.
ValueState keeps a value that can be updated and retrieved (scoped to key of the input element so there 
will possibly be one value for each key that the operation sees). The value can be set using update(T) 
and retrieved using T value().

#### Watermark

This is the mechanism in Flink to measure progress in event time. Watermarks are special timestamped 
elements that get inserted by watermark generators into a [DataStream](#datastream). They flow as part 
of the DataStream and carry a timestamp t. A watermark for time t is an assertion that the stream is 
(probably) now complete up through time t. 

Watermarks give you control over the tradeoff between latency and completeness. You can either configure
your watermark with a short-bounded delay and risk producing results with incomplete knowledge of the input
or you can wait longer and produce results that take advantage of a more complete knowledge of the input
DatasStream.

#### Windows

Flink features very expressive window semantics.

used to compute aggregates on unbounded streams,

Windows can be time driven (example: every 30 seconds) or data driven (example: every 100 elements).

#### Window Assigner

An abstraction that assigns events to windows and creates new window objects as necessary. Flink has 
several built-in types of window assigners. 

#### Window Function 

An abstraction that is applied to the events that are assigned to a window.
