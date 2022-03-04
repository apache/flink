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

#### (Flink) Application

A Flink application is a Java application that submits one or multiple [Flink Jobs](#flink-job) from
the `main()` method (or by some other means). Submitting jobs is usually done by calling `execute()`
on an execution environment.

The jobs of an application can either be submitted to a long running [Session Cluster](#session-cluster),
to a dedicated [Application Cluster](#application-cluster), or to a [Job Cluster](#job-cluster).

#### Application Cluster

A Flink Application Cluster is a dedicated [Flink Cluster](#flink-cluster) that
only executes [Flink Jobs](#flink-job) from one [Flink
Application](#flink-application). The lifetime of the [Flink
Cluster](#flink-cluster) is bound to the lifetime of the Flink Application.

#### At-least-once

#### At-most-once

#### Backpressure

#### Batch processing

#### Bounded streams

#### Category

#### Checkpoint

#### Checkpoint Storage

The location where the [state backend](#state-backend) will store its snapshot during a checkpoint 
(Java Heap of [JobManager](#flink-jobmanager) or Filesystem).

#### (Flink) Cluster

A distributed system consisting of (typically) one [JobManager](#jobmanager) and one or more
[TaskManager](#taskmanager) processes.

#### Connectors

#### Data Parallelism

#### Dataflow


#### Determinism

#### Directed Acyclic Graph (DAG)

#### Event

An event is a statement about a change of the state of the domain modelled by the application. Events
can be input and/or output of a stream or batch processing application. Events are special types of
[records](#Record).

#### Event Time

#### Exactly-Once

#### ExecutionGraph

see [Physical Graph](#physical-graph)

#### Instance

The term *instance* is used to describe a specific instance of a specific type (usually
[operator](#operator) or [function](#function)) during runtime. Since Flink is mostly written in
Java, this corresponds to the definition of *instance* or *object* in Java. In the context of Flink,
the term *parallel instance* is also frequently used to emphasize that multiple instances of the
same [operator](#operator) or [function](#function) type are running in parallel.

#### JVM Heap

#### (Flink) Job

A Flink Job is the runtime representation of a [logical graph](#logical-graph) (also often called dataflow
graph) that is created and submitted by calling `execute()` in a [Flink Application](#flink-application).

#### Job Cluster

A Flink Job Cluster is a dedicated [Flink Cluster](#flink-cluster) that only executes a single 
[Flink Job](#flink-job). The lifetime of the [Flink Cluster](#flink-cluster) is bound to the lifetime 
of the Flink Job. This deployment mode has been deprecated since Flink 1.15.

#### JobGraph

see [Logical Graph](#logical-graph)

#### JobManager

The JobManager is the orchestrator of a [Flink Cluster](#flink-cluster). It contains three distinct
components: Flink Resource Manager, Flink Dispatcher and one [Flink JobMaster](#flink-jobmaster)
per running [Flink Job](#flink-job).

#### JobMaster

JobMasters are one of the components running in the [JobManager](#flink-jobmanager). A JobMaster is
responsible for supervising the execution of the [Tasks](#task) of a single job.

#### JobResultStore

The JobResultStore is a Flink component that persists the results of globally terminated (i.e. finished, 
cancelled or failed) jobs to a filesystem, allowing the results to outlive a finished job. These results 
are then used by Flink to determine whether jobs should be subject to recovery in highly-available clusters.

#### Keyed State

#### Latency

#### Lazy Evaluation

#### List State

#### Logical Graph

A logical graph is a directed graph where the nodes are [operators](#operator) and the edges define 
input/output relationships of the operators and correspond to data streams. A logical graph is created 
by submitting jobs to a [Flink cluster](#flink-cluster) from a [Flink application](#flink-application).

Logical graphs are also often referred to as *dataflow graphs*.

#### Managed State

Managed State describes application state which has been registered with the stream processing framework. 
Apache Flink will take care of the persistence and rescaling of the managed state.  

#### Map State

#### Offset

#### Operator

An operator is a node of a [logical graph](#logical-graph). An operator performs a certain operation, 
which is usually executed by a [function](#function). Sources and sinks are special operators for data
ingestion and data egress.

#### Operator Chain

An operator chain consists of two or more consecutive [operators](#operator) without any
repartitioning in between. Operators within the same operator chain forward records to each other
directly without going through serialization or Flink's network stack.

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


#### Processing Time

#### Record

Records are the constituent elements of a data stream. [Operators](#operator) and [functions](#function) 
receive records as input and emit records as output.

#### Rich Functions

#### (Runtime) Execution Mode

DataStream API programs can be executed in one of two execution modes: `BATCH`
or `STREAMING`. See the [Execution Mode]({{< ref "/docs/dev/datastream/execution_mode" >}}) for more details.

#### Savepoint

#### Schema

#### Serialization

#### Session Cluster

A long-running [Flink Cluster](#flink-cluster) which accepts multiple [Flink Jobs](#flink-job) for
execution. The lifetime of this Flink Cluster is not bound to the lifetime of any Flink Job.
Formerly, a Flink Session Cluster was also known as a Flink Cluster in *session mode*. Compare to
[Flink Application Cluster](#flink-application-cluster).

#### Sharding

#### Shuffling

#### Side Outputs

#### Sink

#### Snapshot

#### Source

#### Spilling

#### State Backend

For stream processing programs, the State Backend of a [Flink Job](#flink-job) determines how its
[state](#managed-state) is stored on each TaskManager (Java Heap of TaskManager or (embedded)
RocksDB).

#### Stream

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

#### TaskManager

TaskManagers are the worker processes of a [Flink Cluster](#flink-cluster). [Tasks](#task) are
scheduled to TaskManagers for execution. They communicate with each other to exchange data between
subsequent Tasks.

#### Throughput

#### Transformation

A transformation is applied on one or more data streams and results in one or more output data streams. 
A transformation might change a data stream on a per-record basis, but might also only change its 
partitioning or perform an aggregation. While [operators](#operator) and [functions](#function) are 
the "physical" parts of Flink's API, transformations are only an API concept. Specifically, most 
transformations are implemented by certain [operators](#operator).

#### Tuple

This is a composite data type. 

#### Unbounded streams

#### (User-Defined) Functions

Functions are implemented by the user and encapsulate the application logic of a Flink program. Most
functions are wrapped by a corresponding [operator](#operator).

#### User-Defined Aggregate Function (UDAF)

#### User-Defined Scalar Function (UDSF)

#### User-Defined Table-valued Function (UDTF)

#### Value State

#### Watermark

