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

## 任务和算子链

分布式计算中，Flink 将算子（operator）的 subtask *链接（chain）*成 task。每个 task 由一个线程执行。把算子链接成 tasks 能够减少线程间切换和缓冲的开销，在降低延迟的同时提高了整体吞吐量。链接操作的配置详情可参考：[chaining docs](../dev/stream/operators/#task-chaining-and-resource-groups)

下图的 dataflow 由五个 subtasks 执行，因此具有五个并行线程。

<img src="{{ site.baseurl }}/fig/tasks_chains.svg" alt="Operator chaining into Tasks" class="offset" width="80%" />

{% top %}

## Job Managers、Task Managers、客户端（Clients）

Flink 运行时包含两类进程：

  - **JobManagers** （也称为 *masters*）协调分布式计算。它们负责调度任务、协调 checkpoints、协调故障恢复等。

    每个 Job 至少会有一个 JobManager。高可用部署下会有多个 JobManagers，其中一个作为 *leader*，其余处于 *standby* 状态。

  - **TaskManagers**（也称为 *workers*）执行 dataflow 中的 *tasks*（准确来说是 subtasks ），并且缓存和交换数据 *streams*。

    每个 Job 至少会有一个 TaskManager。

JobManagers 和 TaskManagers 有多种启动方式：直接在机器上启动（该集群称为 [standalone cluster](../ops/deployment/cluster_setup.html)），在容器或资源管理框架，如 [YARN](../ops/deployment/yarn_setup.html) 或 [Mesos](../ops/deployment/mesos.html)，中启动。TaskManagers 连接到 JobManagers，通知后者自己可用，然后开始接手被分配的工作。

**客户端**虽然不是运行时（runtime）和作业执行时的一部分，但它是被用作准备和提交 dataflow 到 JobManager 的。提交完成之后，客户端可以断开连接，也可以保持连接来接收进度报告。客户端既可以作为触发执行的 Java / Scala 程序的一部分，也可以在命令行进程中运行`./bin/flink run ...`。

<img src="{{ site.baseurl }}/fig/processes.svg" alt="The processes involved in executing a Flink dataflow" class="offset" width="80%" />

{% top %}

## Task Slots 和资源

每个 worker（TaskManager）都是一个 *JVM 进程*，并且可以在不同的线程中执行一个或多个 subtasks。为了控制 worker 接收 task 的数量，worker 拥有所谓的  **task slots** （至少一个）。

每个 *task slots* 代表 TaskManager 的一份固定资源子集。例如，具有三个 slots 的 TaskManager 会将其管理的内存资源分成三等份给每个 slot。 划分资源意味着 subtask 之间不会竞争资源，但是也意味着它们只拥有固定的资源。注意这里并没有 CPU 隔离，当前 slots 之间只是划分任务的内存资源。

通过调整 slot 的数量，用户可以决定 subtasks 的隔离方式。每个 TaskManager 有一个 slot 意味着每组 task 在一个单独的 JVM 中运行（例如，在一个单独的容器中启动）。拥有多个 slots 意味着多个 subtasks 共享同一个 JVM。 Tasks 在同一个 JVM 中共享 TCP 连接（通过多路复用技术）和心跳信息（heartbeat messages）。它们还可能共享数据集和数据结构，从而降低每个 task 的开销。

<img src="{{ site.baseurl }}/fig/tasks_slots.svg" alt="A TaskManager with Task Slots and Tasks" class="offset" width="80%" />

默认情况下，Flink 允许 subtasks 共享 slots，即使它们是不同 tasks 的 subtasks，只要它们来自同一个 job。因此，一个 slot 可能会负责这个 job 的整个管道（pipeline）。允许 *slot sharing* 有两个好处：

  - Flink 集群需要与 job 中使用的最高并行度一样多的 slots。这样不需要计算作业总共包含多少个 tasks（具有不同并行度）。

  - 更好的资源利用率。在没有 slot sharing 的情况下，简单的 subtasks（*source/map()*）将会占用和复杂的 subtasks （*window*）一样多的资源。通过 slot sharing，将示例中的并行度从 2 增加到 6 可以充分利用 slot 的资源，同时确保繁重的 subtask 在 TaskManagers 之间公平地获取资源。

<img src="{{ site.baseurl }}/fig/slot_sharing.svg" alt="TaskManagers with shared Task Slots" class="offset" width="80%" />

APIs 还包含了 *[resource group](../dev/stream/operators/#task-chaining-and-resource-groups)* 机制，它可以用来防止不必要的 slot sharing。

根据经验，合理的 slots 数量应该和 CPU 核数相同。在使用超线程（hyper-threading）时，每个 slot 将会占用 2 个或更多的硬件线程上下文（hardware thread contexts）。

{% top %}

## State Backends

key/values 索引存储的数据结构取决于 [state backend](../ops/state/state_backends.html) 的选择。一类 state backend 将数据存储在内存的哈希映射中，另一类 state backend 使用  [RocksDB](http://rocksdb.org) 作为键/值存储。除了定义保存状态（state）的数据结构之外， state backend 还实现了获取键/值状态的时间点快照的逻辑，并将该快照存储为 checkpoint 的一部分。

<img src="{{ site.baseurl }}/fig/checkpoints.svg" alt="checkpoints and snapshots" class="offset" width="60%" />

{% top %}

## Savepoints

用 Data Stream API 编写的程序可以从 **savepoint** 继续执行。Savepoints 允许在不丢失任何状态的情况下升级程序和 Flink 集群。

[Savepoints](../ops/state/savepoints.html) 是**手动触发的 checkpoints**，它依靠常规的 checkpoint 机制获取程序的快照并将其写入 state backend。在执行期间，程序会定期在 worker 节点上创建快照并生成 checkpoints。对于恢复，Flink 仅需要最后完成的 checkpoint，而一旦完成了新的 checkpoint，旧的就可以被丢弃。

Savepoints 类似于这些定期的 checkpoints，除了它们是**由用户触发**并且在新的 checkpoint 完成时**不会自动过期**。你可以通过[命令行](../ops/cli.html#savepoints) 或在取消一个 job 时通过  [REST API](../monitoring/rest_api.html#cancel-job-with-savepoint) 来创建 Savepoints。

{% top %}
