---
title: 词汇表
nav-pos: 3
nav-title: 词汇表
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

Flink Application Cluster 是一个专用的 [Flink Cluster](#flink-cluster)，它仅用于执行单个 [Flink Job](#flink-job)。[Flink Cluster](#flink-cluster)的生命周期与 [Flink Job](#flink-job)的生命周期绑定在一起。以前，Flink Application Cluster 也称为*job mode*的 Flink Cluster。和 [Flink Session Cluster](#flink-session-cluster) 作对比。

#### Flink Cluster

一般情况下，Flink 集群是由一个 [Flink Master](#flink-master) 和一个或多个 [Flink TaskManager](#flink-taskmanager) 进程组成的分布式系统。

#### Event

Event 是对应用程序建模的域的状态更改的声明。它可以同时为流或批处理应用程序的 input 和 output，也可以单独是 input 或者 output 中的一种。Event 是特殊类型的 [Record](#record)。

#### ExecutionGraph

见 [Physical Graph](#physical-graph)。

#### Function

Function 是由用户实现的，并封装了 Flink 程序的应用程序逻辑。大多数 Function 都由相应的 [Operator](#operator) 封装。

#### Instance

Instance 常用于描述运行时的特定类型(通常是 [Operator](#operator) 或者 [Function](#function))的一个具体实例。由于 Apache Flink 主要是用 Java 编写的，所以，这与 Java 中的 *Instance* 或 *Object* 的定义相对应。在 Apache Flink 的上下文中，*parallel instance* 也常用于强调同一 [Operator](#operator) 或者 [Function](#function) 的多个 instance 以并行的方式运行。

#### Flink Job

Flink Job 代表运行时的 Flink 程序。Flink Job 可以提交到长时间运行的 [Flink Session Cluster](#flink-session-cluster)，也可以作为独立的 [Flink Application Cluster](#flink-application-cluster) 启动。

#### JobGraph

见 [Logical Graph](#logical-graph)。

#### Flink JobManager

JobManager 是在 [Flink Master](#flink-master) 运行中的组件之一。JobManager 负责监督单个作业 [Task](#task) 的执行。以前，整个 [Flink Master](#flink-master) 都叫做 JobManager。

#### Logical Graph

Logical Graph 是一种描述流处理程序的高阶逻辑有向图。节点是[Operator](#operator)，边代表输入/输出关系、数据流和数据集中的之一。

#### Managed State

Managed State 描述了已在框架中注册的应用程序的托管状态。对于托管状态，Apache Flink 会负责持久化和重伸缩等事宜。

#### Flink Master

Flink Master 是 [Flink Cluster](#flink-cluster) 的主节点。它包含三个不同的组件：Flink Resource Manager、Flink Dispatcher、运行每个 [Flink Job](#flink-job) 的 [Flink JobManager](#flink-jobmanager)。

#### Operator

[Logical Graph](#logical-graph) 的节点。算子执行某种操作，该操作通常由 [Function](#function) 执行。Source 和 Sink 是数据输入和数据输出的特殊算子。

#### Operator Chain

算子链由两个或多个连续的 [Operator](#operator) 组成，两者之间没有任何的重新分区。同一算子链内的算子可以彼此直接传递 record，而无需通过序列化或 Flink 的网络栈。

#### Partition

分区是整个数据流或数据集的独立子集。通过将每个 [Record](#record) 分配给一个或多个分区，来把数据流或数据集划分为多个分区。在运行期间，[Task](#task) 会消费数据流或数据集的分区。改变数据流或数据集分区方式的转换通常称为重分区。

#### Physical Graph

Physical graph 是一个在分布式运行时，把 [Logical Graph](#logical-graph) 转换为可执行的结果。节点是 [Task](#task)，边表示数据流或数据集的输入/输出关系或 [partition](#partition)。

#### Record

Record 是数据集或数据流的组成元素。[Operator](#operator) 和 [Function](#Function)接收 record 作为输入，并将 record 作为输出发出。

#### Flink Session Cluster

长时间运行的 [Flink Cluster](#flink-cluster)，它可以接受多个 [Flink Job](#flink-job) 的执行。此 [Flink Cluster](#flink-cluster) 的生命周期不受任何 [Flink Job](#flink-job) 生命周期的约束限制。以前，Flink Session Cluster 也称为 *session mode* 的 [Flink Cluster](#flink-cluster)，和 [Flink Application Cluster](#flink-application-cluster) 相对应。

#### State Backend

对于流处理程序，[Flink Job](#flink-job) 的 State Backend 决定了其 [state](#managed-state) 是如何存储在每个 TaskManager 上的（ TaskManager 的 Java 堆栈或嵌入式 RocksDB），以及它在 checkpoint 时的写入位置（ [Flink Master](#flink-master) 的 Java 堆或者 Filesystem）。

#### Sub-Task

Sub-Task 是负责处理数据流 [Partition](#partition) 的 [Task](#task)。"Sub-Task"强调的是同一个 [Operator](#operator) 或者 [Operator Chain](#operator-chain) 具有多个并行的 Task 。

#### Task

Task 是 [Physical Graph](#physical-graph) 的节点。它是基本的工作单元，由 Flink 的 runtime 来执行。Task 正好封装了一个 [Operator](#operator) 或者 [Operator Chain](#operator-chain) 的 *parallel instance*。 

#### Flink TaskManager

TaskManager 是 [Flink Cluster](#flink-cluster) 的工作进程。[Task](#task) 被调度到 TaskManager 上执行。TaskManager 相互通信，只为在后续的 Task 之间交换数据。

#### Transformation

Transformation 应用于一个或多个数据流或数据集，并产生一个或多个输出数据流或数据集。Transformation 可能会在每个记录的基础上更改数据流或数据集，但也可以只更改其分区或执行聚合。虽然 [Operator](#operator) 和 [Function](#function) 是 Flink API 的“物理”部分，但 Transformation 只是一个 API 概念。具体来说，大多数（但不是全部）Transformation 是由某些 [Operator](#operator) 实现的。
