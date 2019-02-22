---
title: 分布式运行时环境
nav-pos: 2
nav-title: 分布式运行时
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

* This will be replaced by the TOC
{:toc}

## Tasks 和算子链 (Operator Chains)

分布式计算中，Flink 将算子的 subtasks 串联(chains)成 tasks，每个 task 由一个线程执行。把算子串联为 tasks 是一个非常好的优化：它减少了线程之间的通信和缓冲，而且还能增加吞吐量降低延迟。链化操作的配置详情可参考 [串联文档](../dev/stream/operators/#task-chaining-and-resource-groups)。

下图的数据流中有 5 个 subtasks，因此有 5 个线程并发进行处理。

<img src="../fig/tasks_chains.svg" alt="Operator chaining into Tasks" class="offset" width="80%" />

{% top %}

## Job Managers, Task Managers, Clients

Flink 运行时包含两类进程：

  - **JobManagers** （也称为 *masters*）用来协调分布式计算。负责进行任务调度、协调检查点 (checkpoints)、协调错误恢复等等。
    
    至少需要一个 JobManager。高可用部署下会有多个 JobManagers，其中一个作为 leader，其余处于 standby 状态。

  - TaskManagers（也称为 workers）负责执行 dataflow 中的 tasks（更准确的描述是 subtasks），并且对  streams 进行缓存和交换。
    
    至少需要一个 TaskManager。

有多种方式可以启动 JobManagers 和 TaskManagers：作为[独立集群](../ops/deployment/cluster_setup.html)直接在计算机上启动，在容器中或者由资源管理器 [YARN](../ops/deployment/yarn_setup.html) 或 [Mesos](../ops/deployment/mesos.html) 启动。TaskManagers 连接到 JobManagers 后，会通知 JobManagers 自己已可用，接着被分配工作。

**client** 不作为运行时和程序执行的一部分，只是用于准备和发送 dataflow 作业给 JobManager。 因此客户端可以断开连接，或者保持连接以接收进度报告。客户端可以作为触发执行的Java/Scala 程序的一部分或者运行在命令行进程中 `./bin/flink run ...`。

<img src="../fig/processes.svg" alt="The processes involved in executing a Flink dataflow" class="offset" width="80%" />

{% top %}

## Task 槽和资源

每个 worker(TaskManager)都是一个 **JVM 进程**，并且可以在不同的线程中执行一个或多个 subtasks。每个 worker 用  task 槽(至少有一个)来控制可以接收多少个 tasks。

每个 task 槽代表 TaskManager 中一个固定的资源子集。例如，有 3 个槽位的 TaskManager 会将它的内存资源划分成 3 份分配给每个槽。划分资源意味着 subtask 不会和来自其他作业的 subtasks 竞争资源，但是也意味着它只拥有固定的内存资源。注意划分资源不进行 CPU 隔离，只划分内存资源给不同的 tasks。

通过调整 task 槽的个数进而可以调整 subtasks 之间的隔离方式。当每个 TaskManager 只有一个槽位时，意味着每个 task group 运行在不同的 JVM 中（例如：可能在不同的 container 中）。当每个 TaskManager 有多个槽时，意味着多个 subtasks 可以共享同一个 JVM。同一个 JVM 中的 tasks 共享 TCP 连接（通过多路复用技术）和心跳消息。可能还会共享数据集和数据结构，从而减少每个 task 的开销。

<img src="../fig/tasks_slots.svg" alt="A TaskManager with Task Slots and Tasks" class="offset" width="80%" />

默认情况下，只要 subtasks 是来自同一个作业，Flink 允许不同 tasks 的 subtasks 共享槽位。因此，一个槽可能会负责作业的整个 pipeline。允许槽位共享有两个好处：

  - Flink 集群需要的槽的数量和 job 的最高并发度相同，不需要计算一个作业总共包含多少个 tasks（具有不同并行度）。

  - 更易获取更好的资源利用率。没有槽位共享，非集中型subtasks（source/map()）将会占用和集中型 subtasks（window）一样多的资源。在我们的示例中，允许槽位共享，可以将示例作业的并发度从 2 增加到 6，从而可以充分利用资源，同时保证负载很重的 subtasks 可以在 TaskManagers 中平均分配。

<img src="../fig/slot_sharing.svg" alt="TaskManagers with shared Task Slots" class="offset" width="80%" />

APIs 还包含了一种 *[资源组](../dev/stream/operators/#task-chaining-and-resource-groups)* 机制，用来防止不必要的槽位共享。

经验来讲，task 槽的默认值应该与CPU核数一致。在使用超线程下，一个槽将会占用2个或更多的硬件资源。

{% top %}

## 状态后端（State backends）

key/values 索引存储的准确数据结构取决于选择的状态后端。状态后端可以将数据存储在内存哈希表中，也可以使用 RocksDB 作为 key/value 的存储。 除了定义存储状态的数据结构，状态后端还实现了获取 key/value 状态的时间点快照的逻辑，并将该快照存储为检查点的一部分。

<img src="../fig/checkpoints.svg" alt="checkpoints and snapshots" class="offset" width="60%" />

{% top %}

## 保存点（Savepoints）
使用 Data Stream API 编写的程序可以从一个**保存点**恢复执行。保存点允许在不丢失任何状态的情况下修改程序和 Flink 集群。

[保存点](../ops/state/savepoints.html)是**手动触发的检查点**，它依赖常规的检查点机制，生成程序快照并将其写入到状态后端。在运行期间，worker 节点周期性的生成程序快照并产生检查点。在恢复重启时只会使用最后成功的检查点。并且只要有一个新的检查点生成时，旧的检查点将会被安全地丢弃。

保存点与周期性触发的检查点很类似，但是其式**由用户触发**的，且当更新的检查点完成时，老的检查点**不会自动失效**。可以通过[命令行](../ops/cli.html#savepoints)或者在取消一个作业时调用 [REST API](../monitoring/rest_api.html#cancel-job-with-savepoint) 的方式创建保存点。

{% top %}
