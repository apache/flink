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

A Flink Application Cluster is a dedicated [Flink Cluster](#flink-cluster) that
only executes [Flink Jobs](#flink-job) from one [Flink
Application](#flink-application). The lifetime of the [Flink
Cluster](#flink-cluster) is bound to the lifetime of the Flink Application.

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

#### Barrier alignment

For providing exactly-once guarantees, Flink aligns the streams at operators that receive multiple 
input streams, so that the snapshot will reflect the state resulting from consuming events from both 
input streams up to (but not past) both barriers. 

#### Batch processing

When the processing and analysis happens on a set of data that have already been stored over a period 
of time. The results are usually not available in real-time. Flink executes batch programs as a special 
case of streaming programs.

#### Bounded streams

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

#### Connected streams

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

#### Exactly-Once

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
graph) that is created and submitted by calling `execute()` in a [Flink Application](#flink-application).

#### Job Cluster

This is a dedicated [Flink Cluster](#flink-cluster) that only executes a single [Flink Job](#flink-job). 
The lifetime of the [Flink Cluster](#flink-cluster) is bound to the lifetime of the Flink Job. This 
deployment mode has been deprecated since Flink 1.15.

#### JobGraph

See [Logical Graph](#logical-graph).

#### JobManager

The JobManager is the orchestrator of a [Flink Cluster](#(flink)-cluster). It contains three distinct
components: ResourceManager, Dispatcher, and a [JobMaster](#jobmaster) per running [Flink Job](#(flink)-job).

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

These are the atomic unit by which Flink can redistribute [Keyed State](#keyed-state). There are 
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

#### List State

#### Logical Graph

This is a directed graph where the nodes are [operators](#operator) and the edges define input/output 
relationships of the operators and correspond to [DataStreams](#datastreams). A logical graph is created 
by submitting jobs to a [Flink cluster](#(flink)-cluster) from a [Flink application](#(flink)-application).

Logical graphs are also often referred to as [dataflow](#dataflow).

#### Managed State

Managed State describes application state which has been registered with the stream processing framework. 
Apache Flink will take care of the persistence and rescaling of the managed state.  

Managed State is represented in data structures controlled by the Flink runtime, such as internal hash tables, or RocksDB. Examples are “ValueState”, “ListState”, etc. Flink’s runtime encodes the states and writes them into the checkpoints.

Keyed State and Operator State exist in two forms: managed and raw.

#### Map State

#### Non-keyed state

This type of state is bound to one parallel operator instance and is also called [operator state](#operator-state). 

It is also possible to work with managed state in non-keyed contexts. The interfaces involved are somewhat different, and since it is unusual for user-defined functions to need non-keyed state, it is not covered here. This feature is most often used in the implementation of sources and sinks.

#### Offset

a number identifying how far you're from the beginning of a certain information stream (e.g. when you open a file, the offset determines where you wanna start reading the file, counting from the beginning of the file itself)

#### Operator

An operator is a node of a [logical graph](#logical-graph). An operator performs a certain operation, 
which is usually executed by a [function](#function). Sources and sinks are special operators for data
ingestion and data egress.

#### Operator Chain

An operator chain consists of two or more consecutive [operators](#operator) without any
repartitioning in between. Operators within the same operator chain forward records to each other
directly without going through serialization or Flink's network stack.

For distributed execution, Flink chains operator subtasks together into tasks. Each task is executed by one thread. Chaining operators together into tasks is a useful optimization: it reduces the overhead of thread-to-thread handover and buffering, and increases overall throughput while decreasing latency. The chaining behavior can be configured

#### Operator State

#### Parallelism 

#### Partition

A partition is an independent subset of the overall data stream. A data stream is divided into 
partitions by assigning each [record](#record) to one or more partitions via keys. Partitions of data 
streams are consumed by [Tasks](#task) during runtime. A transformation which changes the way a data 
stream is partitioned is often called repartitioning.

#### Physical Graph

A physical graph is the result of translating a [logical graph](#logical-graph) for execution in a
distributed runtime. The nodes are [Tasks](#task) and the edges indicate input/output relationships
or [partitions](#partition) of data streams.

#### POJO

This is a composite data type.  It can be serialized

Flink recognizes a data type as a POJO type (and allows “by-name” field referencing) if the following conditions are fulfilled:

    The class is public and standalone (no non-static inner class)
    The class has a public no-argument constructor
    All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final) or have public getter- and setter- methods that follow the Java beans naming conventions for getters and setters.

#### Process Functions

A ProcessFunction combines event processing with timers and state, making it a powerful building block for stream processing applications. This is the basis for creating event-driven applications with Flink.

#### Processing Time

the time when a specific operator in your pipeline is processing the event
Computing analytics based on processing time causes inconsistencies, and makes it difficult to re-analyze historic data or test new implementations.

#### Queryable state 

allows you to access state from outside of Flink during runtime.

#### Raw State

Keyed State and Operator State exist in two forms: managed and raw.

Raw State is state that operators keep in their own data structures. When checkpointed, they only write a sequence of bytes into the checkpoint. Flink knows nothing about the state’s data structures and sees only the raw bytes.

#### Record

Records are the constituent elements of a data stream. [Operators](#operator) and [functions](#function) 
receive records as input and emit records as output.

#### ResourceManager

part of JobManager

responsible for resource de-/allocation and provisioning in a Flink cluster

#### Rich Functions

At this point you have already seen several of Flink’s function interfaces, including FilterFunction, MapFunction, and FlatMapFunction. These are all examples of the Single Abstract Method pattern.

For each of these interfaces, Flink also provides a so-called “rich” variant, e.g., RichFlatMapFunction, which has some additional methods, including:

    open(Configuration c)
    close()
    getRuntimeContext()

has access to the open and getRuntimeContext methods needed for working with managed keyed state.

#### Rolling total

running sum

#### (Runtime) Execution Mode

DataStream API programs can be executed in one of two execution modes: `BATCH`
or `STREAMING`. See the [Execution Mode]({{< ref "/docs/dev/datastream/execution_mode" >}}) for more details.

#### Savepoint

a snapshot triggered manually by a user (or an API call) for some operational purpose, such as a stateful redeploy/upgrade/rescaling operation. Savepoints are always complete, and are optimized for operational flexibility.

ARe always aligned

#### Schema

#### Serialization

#### Session Cluster

A long-running [Flink Cluster](#flink-cluster) which accepts multiple [Flink Jobs](#flink-job) for
execution. The lifetime of this Flink Cluster is not bound to the lifetime of any Flink Job.
Formerly, a Flink Session Cluster was also known as a Flink Cluster in *session mode*. Compare to
[Flink Application Cluster](#flink-application-cluster).

#### Session Windows

punctuated by a gap of inactivity

#### Shuffling

#### Side Outputs

more than one output stream from a Flink operator, Beyond error reporting, side outputs are also a good way to implement an n-way split of a stream.

#### Sink

#### Sliding Window

with overlap

#### Snapshot

a generic term referring to a global, consistent image of the state of a Flink job. A snapshot includes a pointer into each of the data sources (e.g., an offset into a file or Kafka partition), as well as a copy of the state from each of the job’s stateful operators that resulted from having processed all of the events up to those positions in the sources.

Flink periodically takes persistent snapshots of all the state in every operator and copies these snapshots somewhere more durable, such as a distributed file system.

full or incremental

Flink uses a variant of the Chandy-Lamport algorithm known as asynchronous barrier snapshotting.

#### Source

#### Spilling

#### State Backend

For stream processing programs, the State Backend of a [Flink Job](#flink-job) determines how its
[state](#managed-state) is stored on each TaskManager (Java Heap of TaskManager or (embedded)
RocksDB).

Two implementations of state backends are available – one based on RocksDB, an embedded key/value store that keeps its working state on disk, and another heap-based state backend that keeps its working state in memory, on the Java heap.

#### Stream barriers

A core element of Flink's distributed snapshotting. are injected into the data stream and flow with the records as part of the data stream. Barriers never overtake records, they flow strictly in line. A barrier separates the records in the data stream into the set of records that goes into the current snapshot, and the records that go into the next snapshot.

#### Subtask

A subtask is a [Task](#task) responsible for processing a [partition](#partition) of
the data stream. The term "subtask" emphasizes that there are multiple parallel Tasks for the same
[Operator](#operator) or [Operator Chain](#operator-chain).

#### Table Program

A generic term for pipelines declared with Flink's relational APIs (Table API or SQL).

#### Task

Node of a [Physical Graph](#physical-graph). A task is the basic unit of work, which is executed by
Flink's runtime. Tasks encapsulate exactly one parallel instance of an [operator](#operator) or
[operator Chain](#operator-chain).

#### Task Chaining

#### Task Parallelism

#### Task Slot

unit of resource scheduling in a Flink cluster

Each task slot represents a fixed subset of resources of the TaskManager.

#### TaskManager

TaskManagers are the worker processes of a [Flink Cluster](#flink-cluster). [Tasks](#task) are
scheduled to TaskManagers for execution. They communicate with each other to exchange data between
subsequent Tasks.

TaskManagers connect to JobManagers, announcing themselves as available, and are assigned work.

execute the tasks of a dataflow, and buffer and exchange the data streams.

There must always be at least one TaskManager. The smallest unit of resource scheduling in a TaskManager is a task slot. The number of task slots in a TaskManager indicates the number of concurrent processing tasks.

Each worker (TaskManager) is a JVM process, and may execute one or more subtasks in separate threads.

#### Timer

The timers allow applications to react to changes in processing time and in event time. Every call to the function processElement(...) gets a Context object which gives access to the element’s event time timestamp, and to the TimerService. The TimerService can be used to register callbacks for future event-/processing-time instants.

Timers are fault tolerant and checkpointed along with the state of the application. In case of a failure recovery or when starting an application from a savepoint, the timers are restored.

there are at most one timer per key and second

#### Transformation

A transformation is applied on one or more data streams and results in one or more output data streams. 
A transformation might change a data stream on a per-record basis, but might also only change its 
partitioning or perform an aggregation. While [operators](#operator) and [functions](#function) are 
the "physical" parts of Flink's API, transformations are only an API concept. Specifically, most 
transformations are implemented by certain [operators](#operator).

#### Tumbling Window

A type of window
no overlap

#### Tuple

A composite data type that has a finite ordered list of immutable elements. 

#### Unbounded streams

#### (User-Defined) Functions

Functions are implemented by the user and encapsulate the application logic of a Flink program. Most
functions are wrapped by a corresponding [operator](#operator).

#### User-Defined Aggregate Function (UDAF)

#### User-Defined Scalar Function (UDSF)

#### User-Defined Table-valued Function (UDTF)

#### ValueState

This means that for each key, Flink will store a single object – in this case, an object of type Boolean.

#### Watermark

The mechanism in Flink to measure progress in event time is watermarks. Watermarks flow as part of the data stream and carry a timestamp t.

they define when to stop waiting for earlier events.
Event time processing in Flink depends on watermark generators that insert special timestamped elements into the stream, called watermarks. A watermark for time t is an assertion that the stream is (probably) now complete up through time t.

Another way to think about watermarks is that they give you, the developer of a streaming application, control over the tradeoff between latency and completeness. Unlike in batch processing, where one has the luxury of being able to have complete knowledge of the input before producing any results, with streaming you must eventually stop waiting to see more of the input, and produce some sort of result.

You can either configure your watermarking aggressively, with a short bounded delay, and thereby take the risk of producing results with rather incomplete knowledge of the input – i.e., a possibly wrong result, produced quickly. Or you can wait longer, and produce results that take advantage of having more complete knowledge of the input stream(s).

#### Windows

Flink features very expressive window semantics.

used to compute aggregates on unbounded streams,

Windows can be time driven (example: every 30 seconds) or data driven (example: every 100 elements).

#### Window Assigner

An abstraction that assigns events to windows and creates new window objects as necessary. Flink has 
several built-in types of window assigners. 

#### Window Function 

An abstraction that is applied to the events that are assigned to a window.
