---
title: Glossary
nav-pos: 10
nav-title: Glossary
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

#### Flink Application Cluster

A Flink Application Cluster is a dedicated [Flink Cluster](#flink-cluster) that
only executes [Flink Jobs](#flink-job) from one [Flink
Application](#flink-application). The lifetime of the [Flink
Cluster](#flink-cluster) is bound to the lifetime of the Flink Application.

#### Flink Job Cluster

A Flink Job Cluster is a dedicated [Flink Cluster](#flink-cluster) that only
executes a single [Flink Job](#flink-job). The lifetime of the
[Flink Cluster](#flink-cluster) is bound to the lifetime of the Flink Job.

#### Flink Cluster

A distributed system consisting of (typically) one [Flink Master](#flink-master) and one or more
[Flink TaskManager](#flink-taskmanager) processes.

#### Event

An event is a statement about a change of the state of the domain modelled by the
application. Events can be input and/or output of a stream or batch processing application.
Events are special types of [records](#Record).

#### ExecutionGraph

see [Physical Graph](#physical-graph)

#### Function

Functions are implemented by the user and encapsulate the
application logic of a Flink program. Most Functions are wrapped by a corresponding
[Operator](#operator).

#### Instance

The term *instance* is used to describe a specific instance of a specific type (usually
[Operator](#operator) or [Function](#function)) during runtime. As Apache Flink is mostly written in
Java, this corresponds to the definition of *Instance* or *Object* in Java. In the context of Apache
Flink, the term *parallel instance* is also frequently used to emphasize that multiple instances of
the same [Operator](#operator) or [Function](#function) type are running in parallel.

#### Flink Application

A Flink application is a Java Application that submits one or multiple [Flink
Jobs](#flink-job) from the `main()` method (or by some other means). Submitting
jobs is usually done by calling `execute()` on an execution environment.

The jobs of an application can either be submitted to a long running [Flink
Session Cluster](#flink-session-cluster), to a dedicated [Flink Application
Cluster](#flink-application-cluster), or to a [Flink Job
Cluster](#flink-job-cluster).

#### Flink Job

A Flink Job is the runtime representation of a [logical graph](#logical-graph)
(also often called dataflow graph) that is created and submitted by calling
`execute()` in a [Flink Application](#flink-application).

#### JobGraph

see [Logical Graph](#logical-graph)

#### Flink JobManager

JobManagers are one of the components running in the [Flink Master](#flink-master). A JobManager is
responsible for supervising the execution of the [Tasks](#task) of a single job. Historically, the
whole [Flink Master](#flink-master) was called JobManager.

#### Logical Graph

A logical graph is a directed graph where the nodes are  [Operators](#operator)
and the edges define input/output-relationships of the operators and correspond
to data streams or data sets. A logical graph is created by submitting jobs
from a [Flink Application](#flink-application).

Logical graphs are also often referred to as *dataflow graphs*.

#### Managed State

Managed State describes application state which has been registered with the framework. For
Managed State, Apache Flink will take care about persistence and rescaling among other things.

#### Flink Master

The Flink Master is the master of a [Flink Cluster](#flink-cluster). It contains three distinct
components: Flink Resource Manager, Flink Dispatcher and one [Flink JobManager](#flink-jobmanager)
per running [Flink Job](#flink-job).

#### Operator

Node of a [Logical Graph](#logical-graph). An Operator performs a certain operation, which is
usually executed by a [Function](#function). Sources and Sinks are special Operators for data
ingestion and data egress.

#### Operator Chain

An Operator Chain consists of two or more consecutive [Operators](#operator) without any
repartitioning in between. Operators within the same Operator Chain forward records to each other
directly without going through serialization or Flink's network stack.

#### Partition

A partition is an independent subset of the overall data stream or data set. A data stream or
data set is divided into partitions by assigning each [record](#Record) to one or more partitions.
Partitions of data streams or data sets are consumed by [Tasks](#task) during runtime. A
transformation which changes the way a data stream or data set is partitioned is often called
repartitioning.

#### Physical Graph

A physical graph is the result of translating a [Logical Graph](#logical-graph) for execution in a
distributed runtime. The nodes are [Tasks](#task) and the edges indicate input/output-relationships
or [partitions](#partition) of data streams or data sets.

#### Record

Records are the constituent elements of a data set or data stream. [Operators](#operator) and
[Functions](#Function) receive records as input and emit records as output.

#### Flink Session Cluster

A long-running [Flink Cluster](#flink-cluster) which accepts multiple [Flink Jobs](#flink-job) for
execution. The lifetime of this Flink Cluster is not bound to the lifetime of any Flink Job.
Formerly, a Flink Session Cluster was also known as a Flink Cluster in *session mode*. Compare to
[Flink Application Cluster](#flink-application-cluster).

#### State Backend

For stream processing programs, the State Backend of a [Flink Job](#flink-job) determines how its
[state](#managed-state) is stored on each TaskManager (Java Heap of TaskManager or (embedded)
RocksDB) as well as where it is written upon a checkpoint (Java Heap of
[Flink Master](#flink-master) or Filesystem).

#### Sub-Task

A Sub-Task is a [Task](#task) responsible for processing a [partition](#partition) of
the data stream. The term "Sub-Task" emphasizes that there are multiple parallel Tasks for the same
[Operator](#operator) or [Operator Chain](#operator-chain).

#### Task

Node of a [Physical Graph](#physical-graph). A task is the basic unit of work, which is executed by
Flink's runtime. Tasks encapsulate exactly one parallel instance of an
[Operator](#operator) or [Operator Chain](#operator-chain).

#### Flink TaskManager

TaskManagers are the worker processes of a [Flink Cluster](#flink-cluster). [Tasks](#task) are
scheduled to TaskManagers for execution. They communicate with each other to exchange data between
subsequent Tasks.

#### Transformation

A Transformation is applied on one or more data streams or data sets and results in one or more
output data streams or data sets. A transformation might change a data stream or data set on a
per-record basis, but might also only change its partitioning or perform an aggregation. While
[Operators](#operator) and [Functions](#function) are the "physical" parts of Flink's API,
Transformations are only an API concept. Specifically, most transformations are
implemented by certain [Operators](#operator).
