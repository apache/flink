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

#### Flink Application

A Flink Application is a Java or Python program, written against the DataStream API or the Table API,
that submits one or multiple [Flink Jobs](#flink-job) from the `main()` method (or by some other
means). Submitting Jobs is usually done by calling `execute()` on an execution environment.

The Jobs of an Application can either be submitted to a long-running [Flink
Session Cluster](#flink-session-cluster), to a dedicated [Flink Application
Cluster](#flink-application-cluster), or to a [Flink Job
Cluster](#flink-job-cluster).

See [Flink Session Cluster](#flink-session-cluster) for comparison.

#### Flink Application Cluster

A Flink Application Cluster is a dedicated [Flink Cluster](#flink-cluster) that
only executes [Flink Jobs](#flink-job) from one [Flink
Application](#flink-application). The lifetime of the [Flink
Cluster](#flink-cluster) is bound to the lifetime of the Flink Application.

#### ApplicationResultStore

The ApplicationResultStore is a Flink component that persists the results of terminated
(i.e. finished, cancelled or failed) Applications to a filesystem, allowing the results to outlive
a terminated Application. Each result contains the Application's identifier, final state, name,
etc. These results are then used by Flink to determine whether Applications should
be subject to recovery in highly-available Clusters.

#### Channel

Also *Stream Partitions*.

A Channel is the physical link between a [Sub-Task](#sub-task) and a downstream Sub-Task, and the
edge of a [Physical Graph](#physical-graph). Parts of the documentation refer to Channels as *Stream
Partitions*, in the sense of internal, physical Partitions.

Channels carry data records as well as signals such as [Watermarks](#watermark), Watermark Status
updates and Checkpoint barriers. Transmission over a Channel is always unidirectional (upstream to
downstream) and asynchronous.

A Sub-Task may have one or more input Channels and one or more output Channels. Source Sub-Tasks have
no input Channels, since they begin the graph, and Sink Sub-Tasks have no output Channels, since they
end it.

A Sub-Task routes each record to one of its output Channels according to the [Physical
Partitioning](#partition) of the stream. Hash partitioning (`keyBy()` in the DataStream API, `GROUP
BY` in SQL) routes a record to the Channel connected to the downstream Sub-Task that handles the
record's key, whereas `rebalance()` or `rescale()` may round-robin records across output Channels.

A Channel is *local* when both Sub-Tasks run in the same [Flink
TaskManager](#flink-taskmanager), in which case records are handed over through an in-memory buffer,
or *remote* when the Sub-Tasks run in different TaskManagers, in which case the data crosses the
network.

#### Checkpoint

A consistent snapshot of the State of a [Flink Job](#flink-job) at a logical point in time, taken
with a variant of the Chandy-Lamport algorithm and written to [Checkpoint
Storage](#checkpoint-storage).

A Checkpoint contains the [State](#managed-state) of all stateful [Operators](#operator). This also
includes source positions (for example Kafka partition offsets), assignment of [Source Splits](#source-split)
to [Sub-Tasks](#sub-task),  and Sink transaction metadata. Async I/O in-flight data and buffered data
of some asynchronous Sink connectors are also part of the [Operator](#operator) [State](#managed-state)
and are saved in the Checkpoint.
When [Unaligned Checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#unaligned-checkpointing)
are enabled, it may also contain data in flight between Sub-Tasks.

Checkpoints are triggered automatically and periodically while the Job is running, and are used to
recover from failures such as a TaskManager crash or a network problem: the Job restarts from the
latest completed Checkpoint. They are designed for low overhead and run mostly asynchronously,
without blocking record processing, apart from a synchronous phase in each Sub-Task.
Transactional [Sources and Sinks](#operator) tie their transactions to the Checkpoint; the
Kafka Sink, for instance, commits its Kafka transactions when a Checkpoint completes.

Checkpoints are only used in the `STREAMING` [Execution Mode](#runtime-execution-mode). In `BATCH`
mode, Flink recovers instead by backtracking to previous processing stages whose intermediate results
are still available, so that potentially only the failed [Tasks](#task) and their predecessors are
restarted. As a consequence, Sinks that rely on Checkpoints to commit their transactions do not work
in `BATCH` mode unless they are implemented with the Unified Sink API, which commits once the whole
input has been processed.

Compare to [Savepoint](#savepoint).

Checkpoints and [Savepoints](#savepoint) are also referred to, collectively, as *State Snapshots* or 
*Snapshots*.

#### Checkpoint Storage

The durable location where [Checkpoints](#checkpoint) and [Savepoints](#savepoint) are saved. It can
be either the Java Heap of the [Flink JobManager](#flink-jobmanager) or a filesystem. Production
deployments use a filesystem, typically remote object storage, since Checkpoint Storage is what makes
State survive the loss of a [TaskManager](#flink-taskmanager) or of the whole [Flink Cluster](#flink-cluster).

The relationship between the State Backend and Checkpoint Storage changes with [Disaggregated
State]({{< ref "docs/ops/state/disaggregated_state" >}}), where remote storage becomes the primary
location of the State and the local State Backend acts as a cache, the two being synchronized
asynchronously.

#### Flink Cluster

A distributed system consisting of (typically) one [JobManager](#flink-jobmanager) and one or more
[Flink TaskManager](#flink-taskmanager) processes. Each of these processes runs in a separate JVM,
usually on a separate container or machine, although this is not a requirement.

#### Event

An Event is a statement about a change of the state of the domain modelled by the
Application. Events can be input and/or output of a stream or batch processing Application.
Events are special types of records.

#### Execution Graph

Also *ExecutionGraph*.

See [Physical Graph](#physical-graph)

#### Function

Functions are implemented by the user, in Java or Python, and encapsulate the
application logic of a Flink program. Most Functions are wrapped by a corresponding
[Operator](#operator). In the DataStream API, Functions are passed to the
[Transformations](#transformation) they implement. In the Table API and SQL, they are declared
separately as [User-Defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}) (UDF) or
[Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}) (PTF).

#### History Server

The History Server is a standalone service that serves the detailed history of completed Flink
Applications and Jobs, using archives generated by the JobManager. Unlike the
[ApplicationResultStore](#applicationresultstore) and [JobResultStore](#jobresultstore), which store
minimal metadata for internal recovery decisions in highly-available Clusters, the History Server
provides detailed archives for analysis via Web UI or REST API after the Cluster has been shut down.

#### Instance

The term *instance* is used to describe a specific instance of a specific type (usually
[Operator](#operator) or [Function](#function)) during runtime. As Apache Flink is mostly written in
Java, this corresponds to the definition of *Instance* or *Object* in Java. In the context of Apache
Flink, the term *parallel instance* is also frequently used to emphasize that multiple instances of
the same [Operator](#operator) or [Function](#function) type are running in parallel.

#### Flink Job

A Flink Job is the unit of data processing execution in Flink: a Job as a whole is submitted,
started, stopped and resumed, although under some conditions Flink may restart a Job only partially.

A Job is submitted either by a [Flink Application](#flink-application), by calling `execute()` on an
execution environment, or as a single [Flink SQL Statement](#flink-sql-statement) or [Statement
Set](#statement-set).

A Flink Job is the runtime representation of a [Logical Graph](#logical-graph) (also often called
*Dataflow Graph*). The Logical Graph is optimized into a [Job Graph](#job-graph), from which the
[Physical Graph](#physical-graph) that actually runs in a [Flink Cluster](#flink-cluster) is derived.

#### Flink Job Cluster

A Flink Job Cluster is a dedicated [Flink Cluster](#flink-cluster) that only
executes a single [Flink Job](#flink-job). The lifetime of the
[Flink Cluster](#flink-cluster) is bound to the lifetime of the Flink Job.
This deployment mode has been deprecated since Flink 1.15.

#### Job Graph

Also *JobGraph*.

A Job Graph is the optimized representation of a [Logical Graph](#logical-graph), and the
representation that a [Flink Application](#flink-application) submits to the [Flink
Cluster](#flink-cluster).

Producing the Job Graph is mainly a matter of chaining: consecutive [Operators](#operator) that are
not separated by a repartitioning are merged into a single [Task](#task). The nodes of a Job Graph
are therefore [Tasks](#task), each implementing one Operator or one [Operator
Chain](#operator-chain).

The Job Graph is translated into a [Physical Graph](#physical-graph) for execution.

Job Graph is sometimes referred to as *Optimized Dataflow*.

#### Flink JobManager

Also *Job Manager*.

The JobManager is the orchestrator of a [Flink Cluster](#flink-cluster). It does not process any
data itself: it translates the submitted [Job Graph](#job-graph) into a [Physical
Graph](#physical-graph), schedules the resulting [Sub-Tasks](#sub-task) on the
[TaskManagers](#flink-taskmanager), and coordinates [Checkpoints](#checkpoint) and
[Savepoints](#savepoint). It contains three distinct components: Flink Resource Manager, Flink
Dispatcher and one [Flink JobMaster](#flink-jobmaster) per running [Flink Job](#flink-job).

#### Flink JobMaster

JobMasters are one of the components running in the [JobManager](#flink-jobmanager). A JobMaster is
responsible for supervising the execution of the [Sub-Tasks](#sub-task) of a single Job. It derives
the [Physical Graph](#physical-graph) from the Job's [Job Graph](#job-graph), requests the slots
needed to run it, deploys the Sub-Tasks to the [TaskManagers](#flink-taskmanager), and triggers the
Job's [Checkpoints](#checkpoint).

#### JobResultStore

The JobResultStore is a Flink component that persists the results of globally terminated
(i.e. finished, cancelled or failed) Jobs to a filesystem, allowing the results to outlive
a finished Job. Each result contains the Job's identifier, final state, name, the Application it
belongs to, etc. These results are then used by Flink to determine whether Jobs should
be subject to recovery in highly-available Clusters.

#### Key Group

A Key Group is the atomic unit of key distribution and state assignment across parallel
[Sub-Tasks](#sub-task). Every key is mapped deterministically to a Key Group based on
`keyGroupIndex = MathUtils.murmurHash(key.hashCode()) % maxParallelism`.
This allows stateful [Operators](#operator) to rescale without rehashing individual keys.

The total number of Key Groups is equal to the `maxParallelism` configuration, set at [Job](#flink-job)
level or overridden at [Operator](#operator) level.
A contiguous range of Key Groups is assigned to each [Sub-Task](#sub-task), and Key Groups are evenly
distributed across all [Sub-Tasks](#sub-task).

#### Logical Graph

A Logical Graph is a Directed Acyclic Graph (DAG) where the nodes are [Operators](#operator)
and the edges define input/output-relationships of the Operators and correspond
to data streams or data sets. A Logical Graph is created by submitting Jobs
from a [Flink Application](#flink-application). For the Table API and SQL, the Logical Graph is the
result of parsing and optimizing the [Table Program](#table-program) in the table planner.

Logical Graphs are also often referred to as *Dataflow Graphs* or, for the DataStream API, as
*StreamGraphs*. A Logical Graph is optimized into a [Job Graph](#job-graph) before execution.

#### Managed State

Managed State describes Application State which has been registered with the framework. This includes
both [keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#using-keyed-state) and
non-keyed state (also known as [Operator State]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#operator-state)).
For Managed State, Apache Flink takes care of persistence and rescaling, among other things.

#### Operator

Node of a [Logical Graph](#logical-graph). An Operator performs a certain operation, such as a join,
an aggregation or a stateless transformation, which is usually executed by a [Function](#function).

Sources and Sinks are special Operators for data ingestion and data egress: a Logical Graph always
begins with one or more Source Operators and ends with one or more Sink Operators.

Note that parts of the Flink documentation and of the Web UI use the term *Operator* loosely, also
referring to a [Task](#task) or a [Sub-Task](#sub-task), leaving the precise meaning to be inferred
from the context.

#### Operator Chain

An Operator Chain consists of two or more consecutive [Operators](#operator) without any
repartitioning in between. Operators within the same Operator Chain forward records to each other
directly without going through serialization or Flink's network stack, which removes the overhead of
the handover between them.

An Operator Chain becomes a single [Task](#task) in the [Job Graph](#job-graph). Chains are
recognizable in graphical representations of the Job Graph, such as the Flink Web UI, because the
name of the Task is the composition of the names of the chained Operators.

#### Parallelism

The number of parallel flows Flink uses to process the data, and therefore the way a [Flink
Job](#flink-job) scales horizontally. The Parallelism of an [Operator](#operator) determines the
number of [Sub-Tasks](#sub-task) and of [Physical Partitions](#partition) it is executed with.

The *Job Parallelism* is the default Parallelism of all Operators of a Job. The *Operator
Parallelism* may override it for an individual Operator.

Parallelism is a property of the Job, independent of the number of [Flink
TaskManagers](#flink-taskmanager) in the [Flink Cluster](#flink-cluster).

#### Partition

A Partition is an independent subset of the overall data stream or data set. A data stream or
data set is divided into Partitions by assigning each record to one or more Partitions.
A [Transformation](#transformation) which changes the way a data stream or data set is partitioned is
often called repartitioning.

*Logical Partitioning* is how records and State are divided in the [Logical
Graph](#logical-graph) and the [Job Graph](#job-graph), in order to implement the semantics of an
operation. A `JOIN` or `GROUP BY` in SQL, or a `keyBy()` in DataStream API, for example, requires the 
data to be logically partitioned by a key, and the number of Logical Partitions is then the number of
distinct keys. Keyed State is isolated per Logical Partition: a [Function](#function) can only access
the State of the key of the record or timer it is currently processing. Operator State and Broadcast
State, in contrast, are not keyed.

*Physical Partitioning* is how records and State are divided in the [Physical
Graph](#physical-graph), across the [Sub-Tasks](#sub-task) that Flink executes in parallel. Each
Sub-Task handles exactly one Physical Partition and holds only the State belonging to it, so the
number of Physical Partitions equals the [Parallelism](#parallelism) of the [Operators](#operator)
the Sub-Task implements. Physical Partitioning follows from Logical Partitioning: in a stream
partitioned by key, each Physical Partition holds a fixed subset of the keys.

Note that the Flink documentation uses the word *partition* both for these internal Partitions and
for the partitions of an external system, such as the Kafka partitions of a source topic. The
intended meaning has to be inferred from the context.

#### Physical Graph

A Physical Graph is the result of translating a [Job Graph](#job-graph) for execution in a
distributed runtime, taking [Parallelism](#parallelism) into account. The nodes are
[Sub-Tasks](#sub-task) and the edges are the [Channels](#channel) connecting them.

Physical Graphs are also referred to as *Parallel Dataflows* or as *ExecutionGraphs*.

#### Record

Records are the constituent elements of a data set or data stream. [Operators](#operator) and
[Functions](#function) receive records as input and emit records as output.

#### (Runtime) Execution Mode

DataStream API programs can be executed in one of two Execution Modes: `BATCH`
or `STREAMING`. See [Execution Mode]({{< ref "/docs/dev/datastream/execution_mode" >}}) for more details.

In `STREAMING` mode, Flink processes unbounded data as it arrives, uses [Watermarks](#watermark) to implement
event-time semantics, keeps State in the [State Backend](#state-backend) and relies on
[Checkpoints](#checkpoint) for fault tolerance.

In `BATCH` mode, Flink processes a bounded data set with a known beginning and end. Operators may
consume their entire input before emitting any output, and Watermarks are not used for event-time
semantics. The configured State Backend is ignored: the input of a keyed operation is instead grouped
by key through sorting, so that Flink only has to hold the State of one key at a time, spilling to
local disk when memory is insufficient.

Note that a bounded data set can also be processed in `STREAMING` mode, for example by setting
`scan.bounded.mode` on the Kafka Source (SQL and Table API) or `.setBounded()` in DataStream API.

#### Savepoint

A consistent snapshot of the [State](#managed-state) of a [Flink Job](#flink-job), triggered on
demand. A Job can be resumed from a Savepoint later, for instance across an Application upgrade or a
Flink version upgrade.

When a Job is *stopped with a Savepoint*, every [Sub-Task](#sub-task) stops right after its State has
been snapshotted, which minimizes the chances of records being reprocessed when the Job is resumed.

Savepoints are similar to [Checkpoints](#checkpoint). See
[Checkpoints vs. Savepoints]({{< ref "docs/ops/state/checkpoints_vs_savepoints" >}}) for a detailed
comparison.

[Checkpoints](#checkpoint) and Savepoints are also referred to, collectively, as *State Snapshots* or 
*Snapshots*.

#### Flink Session Cluster

A long-running [Flink Cluster](#flink-cluster) which accepts multiple [Flink Jobs](#flink-job) for
execution. The lifetime of this Flink Cluster is not bound to the lifetime of any Flink Job.
Formerly, a Flink Session Cluster was also known as a Flink Cluster in *session mode*. Compare to
[Flink Application Cluster](#flink-application-cluster).

#### Source Split

Also *Split*.

A Source Split is the unit of work a [Source Operator](#operator) distributes across its parallel
[Sub-Tasks](#sub-task): the smallest portion of the input that one Source Sub-Task reads
independently. Splits are what make reading from an external system parallelizable. For example, in
the Kafka Source, a Source Split is one topic partition.

#### Flink SQL Statement

The unit of execution submitted to Flink when using SQL. A single data-processing Statement, such as
an `INSERT INTO ... SELECT`, is executed as one [Flink Job](#flink-job). Several Statements can be
submitted as a single Job by grouping them into a [Statement Set](#statement-set).

#### State Backend

For stream processing programs, the State Backend of a [Flink Job](#flink-job) holds the
[keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#using-keyed-state) of the
[Job](#flink-job)'s [Operators](#operator). This is local to each [TaskManager](#flink-taskmanager):
either on the Java Heap of the TaskManager (`HashMapStateBackend`) or in off-heap memory and on
local disk (`EmbeddedRocksDBStateBackend`).

The State Backend is working storage, not long-term storage: it is [Checkpoint
Storage](#checkpoint-storage) that makes the State durable and recoverable.

Note that non-keyed state (also known as
[Operator State]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#operator-state)) is always
maintained in memory, in the JVM heap of the [TaskManager](#flink-taskmanager), regardless of the
configured State Backend.

#### Statement Set

A group of SQL DML Statements wrapped in `EXECUTE STATEMENT SET BEGIN ... END`, which Flink submits
and optimizes as a single Statement and executes as a single [Flink Job](#flink-job).

Because the Statements are optimized together, they may share some Source and Sink
[Operators](#operator), avoiding reading the same data more than once. See [INSERT
Statement]({{< ref "docs/sql/reference/dml/insert" >}}#insert-into-multiple-tables) for the syntax.

#### StreamExchange Operator

An [Operator](#operator) that only appears in [Logical Graphs](#logical-graph) generated from
[Table Programs](#table-program). It repartitions a stream, and is the equivalent of a hash (key-by)
connection between two Operators in the DataStream API.

#### StreamGraphs

See [Logical Graph](#logical-graph)

#### Sub-Task

Also *Subtask*.

A Sub-Task is a node of the [Physical Graph](#physical-graph) and the smallest unit of execution in
the Flink runtime, distributed across the [Flink Cluster](#flink-cluster) to process data. Each
[Task](#task) results in as many Sub-Tasks as the [Parallelism](#parallelism) of the
[Operators](#operator) it implements, which is why the term emphasizes that there are multiple
parallel Sub-Tasks for the same Task.

Because a Task may implement a single Operator or a whole [Operator Chain](#operator-chain), a
Sub-Task may execute one or more Operators. All Operators in a Chain necessarily share the same
Parallelism, otherwise they would not have been chained.

Each Sub-Task processes one [Physical Partition](#partition) of the data and holds only the State
belonging to that Partition. Within a Sub-Task, a single thread generally carries a record through
all the Operators the Sub-Task implements, although some internal buffering happens and some
Operators are partly asynchronous.

Operators that call a user-defined [Function](#function) create a separate instance of that Function
per Sub-Task, and processing within one instance always runs on a single thread, so instance fields
of a Function implementation are not subject to concurrent access.

#### Table Program

A generic term for pipelines declared with Flink's relational APIs (Table API or SQL).

#### Task

A Task is a node of the [Job Graph](#job-graph), implementing either a single [Operator](#operator)
or several Operators [chained](#operator-chain) together. Tasks are the blocks shown in the graphical
representation of a Job in the Flink Web UI.

At runtime, each Task is executed as one [Sub-Task](#sub-task) per [Physical
Partition](#partition) of the data.

#### Flink TaskManager

Also *Task Manager*.

TaskManagers are the worker processes of a [Flink Cluster](#flink-cluster), and the processes that do
the actual data processing. [Sub-Tasks](#sub-task) are scheduled to TaskManagers for execution, and
TaskManagers communicate with each other over [Channels](#channel) to exchange data between
subsequent Sub-Tasks.

Each TaskManager manages its own local [State Backend](#state-backend), and reads from and writes to
[Checkpoint Storage](#checkpoint-storage) independently during [Checkpoints](#checkpoint) and
[Savepoints](#savepoint).

#### Transformation

A Transformation is applied on one or more data streams or data sets and results in one or more
output data streams or data sets. A Transformation might change a data stream or data set on a
per-record basis, but might also only change its Partitioning or perform an aggregation. While
[Operators](#operator) and [Functions](#function) are the "physical" parts of Flink's API,
Transformations are only an API concept. Most Transformations, like `map()` or `filter()` for
example, are implemented by specific [Operators](#operator). Others, like `keyBy()` or `broadcast()`,
correspond to repartitioning between [Operators](#operator).

#### UID

A unique identifier of an [Operator](#operator), either provided by the user or determined from the
structure of the Job. When the [Application](#flink-application) is submitted this is converted to
a [UID Hash](#uid-hash).

#### UID Hash

A unique identifier of an [Operator](#operator) at runtime, otherwise known as "Operator ID" or
"Vertex ID" and generated from a [UID](#uid).
It is commonly exposed in logs, the REST API or metrics, and most importantly is how
[Operators](#operator) are identified in state snapshots ([Checkpoints](#checkpoint) and
[Savepoints](#savepoint)).

#### Watermark

Watermarks are the mechanism Flink uses to measure the progress of *event time*, the time at which an
[Event](#event) actually happened, as opposed to *processing time*, the wall-clock time at which
Flink processes it.

Watermarks flow inline with the records, over the same [Channels](#channel), and carry a
timestamp *t*. A `Watermark(t)` declares that event time has reached *t* in that stream, and
therefore that no further records with a timestamp *t' <= t* are expected. This is what allows an
[Operator](#operator) to decide that an event-time window can be closed, or that an event-time timer
must fire. A record that arrives after the Watermark has already passed its timestamp is a *late* record.

Watermarks are emitted at the Sources, based on a `WatermarkStrategy`. Each Source [Sub-Task](#sub-task) 
generates its own Watermarks independently. Event time advances independently in each 
[Physical Partition](#partition). When a Watermark reaches a Sub-Task, the Sub-Task advances its internal 
event-time clock and emits a new Watermark to its downstream Sub-Tasks. A Sub-Task with several input 
Channels takes the *minimum* of the event times of its active inputs, which means a single lagging input 
holds back event time for the whole downstream graph.

An input that receives no records cannot advance its Watermark, and would otherwise stall event time
downstream indefinitely. To prevent this, a `WatermarkStrategy` can declare an input *idle*, which
propagates a *Watermark Status* signal along the Channels so that downstream Sub-Tasks exclude that
input when computing their minimum.

Watermarks are only used in the `STREAMING` [Execution Mode](#runtime-execution-mode).

See [Timely Stream Processing]({{< ref "docs/concepts/time" >}}#event-time-and-watermarks) for the
concepts and [Generating Watermarks]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}) 
for how to configure them.
