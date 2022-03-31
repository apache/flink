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

Aggregation is an operation that takes multiple [records](#record) and aggregates them into a single 
record using a user-provided aggregation logic (i.e. counting the number of records). When working with 
[DataStreams](#datastream), it generally makes more sense to think in terms of aggregations over finite 
[windows](#window), rather than over the entire DataStream, since records availability cannot be assumed 
over time, like with traditional database systems.

#### (Flink) Application

A Flink application is any user program that submits one or multiple [Flink Jobs](#flink-job) from its
`main()` method. The execution of these jobs can happen in a local JVM or on a remote setup of clusters 
with multiple machines.

The jobs of an application can either be submitted to a long-running [Session Cluster](#session-cluster),
to a dedicated [Application Cluster](#application-cluster), or to a [Job Cluster](#job-cluster).

#### Application Cluster

A Flink application cluster is a dedicated [Flink cluster](#(flink)-cluster) that only executes 
[Flink jobs](#flink-job) from one [Flink application](#flink-application). The lifetime of the Flink
cluster is bound to the lifetime of the Flink application.

#### Asynchronous Snapshotting

A form of [snapshotting](#snapshot) that doesn't impede the ongoing stream processing by allowing an 
operator to continue processing while it stores its state snapshot, effectively letting the state 
snapshots happen asynchronously in the background.

#### At-least-once Delivery Guarantee

A fault-tolerance guarantee and data delivery approach where multiple attempts are made at delivering
an event such that at least one succeeds. This guarantees that nothing is lost, but you may experience 
duplicated (correct) results.

#### At-Most-Once Delivery Guarantee

A data delivery approach where each event is delivered zero or one times. There is lower latency but
events may be lost.

#### Backpressure

A situation where a system is receiving data at a higher rate than it can process during a temporary 
load spike or if the cluster does not have sufficient resources.

#### Barrier Alignment

For providing exactly-once guarantees, Flink aligns the streams at operators that receive multiple 
input streams, so that the snapshot will reflect the state resulting from consuming events from both 
input streams up to (but not past) both barriers. 

See [checkpoint barrier](#checkpoint-barrier).

#### Batch Processing

This commonly describes the processing and analysis on a set of data that have already been stored 
over a period of time (i.e. in groups or batches). The results are usually not available in real-time.
In the context of Flink, batch processing refers to a [Job](#flink-job) which is run with `ExecutionMode.BATCH`.
This implies that the data is not only bounded, but that the engine is taking advantage of the bounded 
nature of the data by applying batch processing techniques instead of stream processing techniques.

#### Bounded Streams

Flink treats [bounded data](https://flink.apache.org/flink-architecture.html) as bounded [DataStreams](#datastream), 
which have a defined start and end, known before the beginning of processing. Bounded streams can be 
processed by ingesting all data before performing any computations. Ordered ingestion is not required 
to process bounded streams because a bounded data set can always be sorted. 

When doing stream processing, Flink can apply optimizations to bounded streams, while when doing [batch 
processing](#batch-processing), Flink requires that every input stream is a bounded stream.

#### Checkpoint

A [snapshot](#snapshot) taken automatically by Flink for the purpose of being able to recover from 
faults. A checkpoint marks a specific point in each of the input streams along with the corresponding 
state for each of the operators. Checkpoints are optimized for being restored quickly.

#### Checkpoint Barrier

For providing exactly-once guarantees, Flink aligns the streams at operators that receive multiple
input streams. A checkpoint barrier is a special marker that flows along the graph and triggers the 
checkpointing process on each of the parallel instances of the operators. Checkpoint barriers are 
injected into the source operators and flow together with the data. If an operator has multiple outputs, 
it is sent to all of them.

#### Checkpoint Coordinator

This coordinates the distributed snapshots of operators and state. It is part of the JobManager and 
instructs the TaskManager when to begin a checkpoint by sending the messages to the relevant tasks 
and collecting the checkpoint acknowledgements.

#### Checkpoint Storage

The location where the [state backend](#state-backend) will store its snapshot during a checkpoint. 
This could be on the Java heap of the [JobManager](#flink-jobmanager) or on a file system.

#### (Flink) Client

This is not part of the runtime and program execution but is used to prepare and send a [dataflow](#dataflow) 
graph to the [JobManager](#jobmanager) (though in [application mode](#application-cluster), this runs 
in the JobManager process). The Flink client runs either as part of the program that triggers the 
execution or in the command line process via `./bin/flink run`.

#### (Flink) Cluster

A distributed system consisting of (typically) one [JobManager](#jobmanager) and one or more
[TaskManager](#taskmanager) processes.

#### Connected Streams

A pattern in Flink where a single operator has two input streams. Connected streams can, for example, 
be used to implement streaming joins.

#### Connectors

Connectors allow [Flink applications](#flink-applications) to read from and write to various external 
systems. They support multiple [formats](#format) in order to decode/encode data from/to the existing/desired 
format of external systems.

This is a term often used to describe a [sink](#sink)/[source](#source) implementation for a specific 
external system. For example, the Kafka connector is the sink/source implementation for Kafka.

#### Dataflow

See [logical graph](#logical-graph).

#### DataStream

DataStream refers to a class in Flink that provides a specific API used to represent and work with  
[streams](#stream) in a [Flink application](#flink-application). You can think of a DataStream as immutable 
collections of data that can contain duplicates. This data can either be [bounded](#bounded-streams) 
or [unbounded](#unbounded-streams). You can create an initial DataStream by adding a [source](#source).

Table is another class in Flink that provides a (relational) API for operating on [streams](#stream), 
but at a higher level of abstraction, one based on interpreting streams as [dynamic tables](#dynamic-table).

#### Delivery Guarantee

This is a message delivery guarantee that a processing framework offers between two systems. They can 
be divided into three groups (with costs and considerations for each) which include “[at-most-once](#at-most-once-delivery-guarantee)”, 
“[at-least-once](#at-least-once-delivery-guarantee)”, and “[exactly-once](#exactly-once-delivery-guarantee)”. 
Delivery guarantees are typically not considered in [batch processing](#batch-processing) systems, 
because they always ensure atomicity between reading input data, processing, and delivery of results. 
This means that each [record](#record) within a batch is processed exactly-once.

#### Directed Acyclic Graph (DAG)

This is a graph that is directed and without cycles connecting the other edges. It can be used to 
conceptually represent a [logical graph](#logical-graph) where you never look back to previous events.

#### Dispatcher

This is a component of the [JobManager](#jobmanager) and provides a REST interface to submit Flink 
applications for execution and starts a new [JobMaster](#jobmaster) for each submitted job. It also 
runs the Flink web UI to provide information about job executions.

#### Dynamic Table

#### Event

An event is a statement about a change of the state of the domain modelled by the application. Events
can be input and/or output of a stream processing application. Events are special types of
[records](#Record).

#### Event Time

The time when an [event](#event) occurred, as recorded by the device producing (or storing) the event.
When developing streaming applications, it is good practice for the source of the event to attach the 
event time to the event, in order for the stream processor to achieve reproducible results that do not 
depend on when the calculation is performed.

The various Flink APIs provides different ways to specify how to extract the event time from the event 
instances (and track its progress), such as {{< javadoc file="org/apache/flink/streaming/api/functions/TimestampExtractor.html" name="Timestamp Extractor" >}} 
for DataStream API and timestamp column for SQL/Table API.

#### Exactly-Once Delivery Guarantee

A delivery guarantee and data delivery approach where nothing is lost or duplicated. This does 
not mean that every event will be processed exactly once. Instead, it means that every event will affect 
the state being managed by Flink exactly once.

#### ExecutionGraph

See [Physical Graph](#physical-graph).

#### Format

A format is a way to define how to map binary data from one source to another.  For example, table 
formats define how to store and map binary data onto table columns.  When [records](#record) come from
"transient" sources in a [DataStream](#datastream) application, formats can help map binary data to 
data types such as [POJOs](#pojo). 

Flink comes with a variety of built-in output formats that can be used with table [connectors](#connector).

#### Ingestion Time

A timestamp recorded by Flink at the moment it ingests the event.

#### (Flink) Job

This is the runtime representation of a [logical graph](#logical-graph) (also often called dataflow
graph) that is created and submitted by calling `execute()` in a [Flink application](#flink-application).

#### Job Cluster

This is a dedicated [Flink cluster](#(flink)-cluster) that only executes a single [Flink job](#(flink)-job). 
The lifetime of the Flink cluster is bound to the lifetime of the Flink job. This deployment mode has 
been deprecated since Flink 1.15.

#### Job Graph

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

This is a disjoint subset of keys, for a given key type, spanning a certain range of keys. Flink creates 
a [keyed state](#keyed-state) bucket for each key group, always assigning the state of a specific key 
to a specific bucket.

There are exactly as many key groups as the defined maximum parallelism. During execution, each parallel 
instance of a keyed operator works with the keys for one or more key groups.

#### Keyed State

Keyed state is one of the two basic types of state in Apache Flink (the other being [operator state](#operator-state)).
In order to have all events with the same value of an attribute grouped together, you can partition 
a stream around that attribute, and maintain it as an embedded key/value store. This results in a keyed
state. 

A keyed state is always bound to keys and is only available to functions and operators that process
data from a keyed stream.

Flink supports several different types of keyed state, with the simplest one being [ValueState](#valuestate).

#### Keyed Stream

A keyed stream is a [stream](#stream) on which [operator state](#operator-state) is partitioned 
by a key attribute. 

Typical operations supported by a [DataStream](#datastream) are also possible on a keyed stream, 
except for partitioning methods such as shuffle, forward, and keyBy.

#### Lateness

Lateness is defined relative to the [watermarks](#watermark). A `watermark(t)` asserts that the [stream](#stream) 
is complete up through to time `t`. A [record](#record) with timestamp `s` is considered late if it 
arrives after any watermark whose timestamp is `≤ s`.

#### List State

This is a type of [keyed state](#keyed-state) that keeps a list of elements per key. You can append 
elements and retrieve an Iterable over all currently stored elements.

#### Logical Graph

This is a directed graph where the nodes are [operators](#operator) and the edges define input/output 
relationships of the operators and correspond to [DataStreams](#datastreams) or SQL queries. A logical 
graph is created by submitting jobs to a [Flink cluster](#(flink)-cluster) from a [Flink application](#flink-application).

Logical graphs are also often referred to as [dataflow](#dataflow).

#### Managed State

Managed state is application state which has been registered with the stream processing framework, 
which will take care of the persistence and rescaling of this state.  

This type of state is represented in data structures controlled by the Flink runtime, such as internal 
hash tables, or RocksDB. Flink’s runtime encodes the states and writes them into the checkpoints.

[Keyed state](#keyed-state) and [operator state](#operator-state) exist in two forms: managed and [raw](#raw-state).

#### Map State

This is a type of [keyed state](#keyed-state) that keeps a list of mappings. You can put key-value 
pairs into the state and retrieve an Iterable over all currently stored mappings.

#### Non-Keyed State

This type of state is bound to one parallel operator instance and is also called [operator state](#operator-state). 

It is possible to work with [managed state](#managed-state) in non-keyed contexts but it is unusual 
for user-defined functions to need non-keyed state and the interfaces involved would be different. 

This feature is often used in the implementation of [sources](#source) and [sinks](#sink).

#### Offset

A number identifying how far you are from the beginning of a certain [stream](#stream). 

#### Operator

An operator is a node of a [logical graph](#logical-graph) executing a specific stream processing logic. 
In general, Flink's [stream](#stream) operators can have `N` input streams, `N` output streams, and 
an arbitrary amount of [keyed state](#keyed-state)/global state assigned to it. Operator behavior can 
usually be customized by user-provided [functions](#function). 

An example is the map operator, which is a 1-input/1-output stream that transforms the input records 
with a user-provided function. [Sources](#source) and [sinks](#sink) are special operators for data 
ingestion and data egress.

#### Operator Chain

An operator chain consists of two or more consecutive [operators](#operator) without any repartitioning 
in between. This can allow Flink to forward records to each operator in the operator chain directly 
without going through serialization or Flink's network stack. This is a useful optimization and increases 
overall throughput while decreasing latency. The chaining behavior can be configured.

#### Operator State

See [non-keyed state](#non-keyed-state).

#### Parallelism 

This is a technique for making programs run faster by performing several computations simultaneously.

#### Partition

A partition is an independent subset of the overall [stream](#stream). Partitions of streams are 
consumed by [tasks](#task) during runtime. A transformation that changes the way a stream is partitioned 
is often called repartitioning.

#### Physical Graph

A physical graph is the result of translating a [logical graph](#logical-graph) for execution in a
distributed runtime. The nodes are [tasks](#task) and the edges indicate input/output relationships
or [partitions](#partition) of [streams](#stream).

#### POJO

This is a composite data type and can be serialized with Flink's serializer. Flink recognizes a data 
type as a POJO type (and allows “by-name” field referencing) if certain [conditions]({{< ref "/docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}#pojos) are met.
  
Flink analyzes the structure of POJO types and can process POJOs more efficiently than general types.

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

A record is a collection of named fields of different data types. [Streams](#stream) of data are organized 
into records. [Operators](#operator) and [functions](#function) receive records as input and emit records 
as output.

#### ResourceManager

This component is part of the [JobManager](#JobManager) and is responsible for resource de-/allocation 
and provisioning in a Flink cluster by communicating with external resource management frameworks like 
Kubernetes or YARN.

#### Retained Checkpoint

A checkpoint that is configured to be retained instead of being deleted when a job is cancelled.
Flink normally retains only the n-most-recent checkpoints (n being configurable) while a job is running
and deletes them when a job is cancelled.

You can manually resume from a retained checkpoint.

#### Rich Functions

A RichFunction is a "rich" variant of Flink's function interfaces for data transformation. These functions 
have some additional methods needed for working with managed keyed state such as `open(Configuration c)`, 
`close()`, `getRuntimeContext()`.

#### (Runtime) Execution Mode

DataStream API programs and [Table programs](#table-program) can be executed in one of two execution 
modes: `BATCH` or `STREAMING`. See [Execution Mode]({{< ref "/docs/dev/datastream/execution_mode" >}}) 
for more details.

#### Savepoint

A [snapshot](#snapshot) triggered manually by a user (or an API call) for some operational purpose,
such as a stateful redeploy/upgrade/rescaling. Unlike [checkpoints](#checkpoint), the lifecycle of a 
savepoint is controlled by the user.

#### Serialization

This is the process of turning a data element in memory into a stream of bytes so that you can store 
it on disk or send it over the network.

Flink automatically generates serializers for most data types and handles [data types and 
serialization]({{< ref "/docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}) 
in a unique way, containing its own type descriptors, generic type extraction, and type serialization 
framework.

#### Session Cluster

A long-running [Flink cluster](#(flink)-cluster) which accepts multiple [Flink jobs](#(flink)-job) for
execution. The lifetime of this cluster is not bound to the lifetime of any Flink job. Formerly, a 
Session Cluster was also known as a Flink Cluster in *session mode*. 

#### Session Window

This is a [window](#window) that groups elements by sessions of activity. Session windows do not overlap 
and do not have a fixed start and end time, in contrast to [tumbling windows](#tumbling-window) and 
[sliding windows](#sliding-window). A session window closes when it does not receive elements for a 
certain period of time (i.e. when a gap of inactivity occurred).

#### Shuffling

This is a process of redistributing data across [partitions](#partition) (aka repartitioning) that 
may or may not cause moving data across JVM processes or over the network.

#### Side Outputs

This is an extra output stream from a Flink operator. Beyond error reporting, side outputs are also 
a good way to implement an n-way split of a stream.

#### Sink

A sink is a component that consumes incoming processed [streams](#stream) from Flink and forwards them 
to files, sockets, external systems, or print them. 

A few predefined data sinks are built into Flink, such as support for writing to files, to stdout/stderr, 
and to sockets. Other sinks are available through additional [connectors](#connector).

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

This is the source of the data that gets piped into a [Flink application](#flink-application) to be 
processed. As long as data keeps flowing in, Flink can keep performing calculations. 

A few basic data sources are built into Flink and are always available, such as reading from files, 
directories, sockets, and ingesting data from collections and iterators. Other sources are available 
through additional [connectors](#connector).

#### Spilling

This is a technique where state data is spilled to disk before JVM heap memory is exhausted.

#### State Backend

For stream processing programs, the state backend of a [Flink job](#(flink)-job) determines how its
[state](#managed-state) is stored on each [TaskManager](#taskmanager).

Two implementations of state backends are available. One is based on RocksDB, an embedded key/value 
store that keeps its working state on disk, and the other is heap-based that keeps its working state 
in memory, on the Java heap.

#### Stream

This is a concept describing a continuous flow of data (generated by various sources) that is fed into 
stream processing systems to be processed and analyzed in real time. It consists of a series of data 
elements ordered in time. 

[DataStream](#flink-datastream) is the representation of this concept inside Flink's DataStream API 
and Table API.

#### Stream Barriers

A core element of Flink's distributed snapshotting. Stream barriers are injected into the [DataStream](#datastream) 
and flow with the [records](#record) as part of the DataStream. Barriers never overtake records and 
flow strictly in line. A barrier separates the records in the DataStream into the set of records that 
goes into the current snapshot, and the records that go into the next snapshot.

#### Subtask

A subtask is an abstract concept that describes a [task](#task) responsible for processing a [partition](#partition) 
of the [DataStream](#datastream). The term "subtask" emphasizes that there are multiple parallel tasks 
for the same [operator](#operator) or [operator chain](#operator-chain).

#### Table Program

A generic term for pipelines declared with Flink's relational APIs (Table API or SQL).

#### Task

This is a node in a [physical graph](#physical-graph). A task is the basic unit of work which is executed 
by Flink's runtime. Tasks encapsulate exactly one parallel instance of an [operator](#operator) or
[operator chain](#operator-chain).

#### Task Chaining

This is an optimization where Flink puts two subsequent [transformations](#transformation) in the same thread, if possible. 

#### Task Parallelism

This is the number of parallel instances of a task. A [Flink application](#flink-application) consists 
of multiple [tasks](#task) ([transformations](#transformation), [operators](#operator), [sources](#source), 
[sinks](#sink)). A task is split into several parallel instances for execution and each parallel instance 
processes a subset of the task's input data. 

#### Task Slot

This is one unit of resource scheduling in a [Flink cluster](#(flink)-cluster). Each task slot 
represents a fixed subset of resources of the [TaskManager](#taskmanager). The number of task slots 
in a TaskManager indicates the number of concurrent processing tasks.

#### TaskManager

TaskManagers are the worker processes of a [Flink cluster](#flink-cluster), execute the tasks of a 
dataflow, and buffer and exchange the [streams](#streams). They connect to [JobManagers](#jobmanagers), 
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

A transformation is applied to one or more [streams](#stream) and results in one or more output streams. 
A transformation might change a stream on a [per-record](#record) basis, but might also only change 
its [partitioning](#partition) or perform an [aggregation](#aggregation). While [operators](#operator) 
and [functions](#function) are the "physical" parts of Flink's API, transformations are an API concept. 
Specifically, most transformations are implemented by certain [operators](#operator).

#### Tumbling Window

This is a [window](#window) that groups elements by a specified window size. Tumbling windows have a 
fixed size and do not overlap. For example, if you specify a tumbling window with a size of 5 minutes, 
the current window will be evaluated and a new window will be started every five minutes.

#### Unbounded streams

Flink treats [unbounded data](https://flink.apache.org/flink-architecture.html) as unbounded [DataStreams](#datastream),
which have a start but no defined end. They do not terminate, provide data as it is generated, and must 
be continuously processed. 

#### (User-Defined) Functions

Functions are implemented by the user and encapsulate the application logic of a [Flink application](#flink-application). 
Most functions are wrapped by a corresponding [operator](#operator).

#### User-Defined Aggregate Function (UDAF)

In Table API/SQL, this type of [user-defined function]({{< ref "/docs/dev/table/functions/udfs" >}}) 
aggregates multiple values into a single value.

#### User-Defined Scalar Function (UDSF)

In Table API/SQL, this type of [user-defined function]({{< ref "/docs/dev/table/functions/udfs" >}}) 
maps zero, one, or more [scalar](#scalar) values to a new scalar value.

#### User-Defined Table-valued Function (UDTF)

In Table API/SQL, this type of [user-defined function]({{< ref "/docs/dev/table/functions/udfs" >}}) 
uses zero, one, or multiple [scalar](#scalar) values as input parameters (including variable-length 
parameters). A UDTF returns any number of rows, rather than a single value. The returned rows can 
consist of one or more columns.

#### ValueState<T>

This is a type of [keyed state](#keyed-state) where Flink will store a single object for each key.
ValueState keeps a value that can be updated and retrieved (scoped to key of the input element so there 
will possibly be one value for each key that the operation sees). The value can be set using update(T) 
and retrieved using T value().

#### Watermark

This is the mechanism in Flink to measure progress in event time. Watermarks are special timestamped 
elements that get inserted by watermark generators into a [stream](#stream). They flow as part of the
DataStream and carry a timestamp `t`. A watermark for time `t` is an assertion that the stream is 
(probably) now complete up through time `t`. 

Watermarks give you control over the tradeoff between latency and completeness. You can either configure
your watermark with a short-bounded delay and risk producing results with incomplete knowledge of the input
or you can wait longer and produce results that take advantage of a more complete knowledge of the input
DatasStream.

#### Windows

These are vital to processing unbounded streams. Windows split the [DataStream](#datastream) into 
“buckets” of finite size, over which computations can be applied. 

Flink features very expressive window semantics (i.e. [tumbling windows](#tumbling-window), 
[sliding windows](#sliding-window), [session windows](#session-window)). Windows can be time-driven 
(i.e. every 30 seconds) or data-driven (i.e. every 100 elements).

#### Window Assigner

This is an abstraction that assigns [events](#event) to [windows](#window) and creates new window 
objects as necessary. Flink has several built-in types of window assigners. 

#### Window Function 

This is an abstraction that is applied to the [events](#event) that are assigned to a [window](#window).
