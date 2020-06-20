---
title: 通过状态快照实现容错处理
nav-id: fault-tolerance
nav-pos: 6
nav-title: 容错处理
nav-parent_id: learn-flink
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

## State Backends

由 Flink 管理的 keyed state 是一种分片的键/值存储，并且 keyed state 的每一项的工作副本都保存在负责该键的 taskmanager 本地的某个地方。
Operator state 对于需要它的机器节点来说也是本地的。Flink 定期获取所有状态的连续快照，并将这些快照复制到持久化的地方，例如分布式文件系统。

如果发生故障，Flink 可以恢复应用程序的完整状态并恢复处理，就如同没有出现过异常一样。

Flink 管理的状态存储在 _state backend_。
state backends 的两种实现 -- 一种是基于 RocksDB 内嵌 key/value 存储将其工作状态保存在磁盘上的，另一种基于堆的 state backend，将其工作状态保存在 Java 的 堆内存中。
这种基于堆的 state backend 有两种方式：保存其状态快照到分布式文件系统的 FsStateBackend，以及使用 JobManager 堆的 MemoryStateBackend。

<table class="table table-bordered">
  <thead>
    <tr class="alert alert-info">
      <th class="text-left">名称</th>
      <th class="text-left">Working State</th>
      <th class="text-left">状态备份</th>
      <th class="text-left">快照</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th class="text-left">RocksDBStateBackend</th>
      <td class="text-left">本地磁盘（tmp dir）</td>
      <td class="text-left">分布式文件系统</td>
      <td class="text-left">全量 / 增量</td>
    </tr>
    <tr>
      <td colspan="4" class="text-left">
        <ul>
          <li>支持大于内存大小的状态</li>
          <li>经验法则：比基于堆的后端慢10倍</li>
        </ul>
      </td>
    </tr>
    <tr>
      <th class="text-left">FsStateBackend</th>
      <td class="text-left">JVM Heap</td>
      <td class="text-left">分布式文件系统</td>
      <td class="text-left">全量</td>
    </tr>
    <tr>
      <td colspan="4" class="text-left">
        <ul>
          <li>快速，需要大的堆内存</li>
          <li>受限制于 GC</li>
        </ul>
      </td>
    </tr>
    <tr>
      <th class="text-left">MemoryStateBackend</th>
      <td class="text-left">JVM Heap</td>
      <td class="text-left">JobManager JVM Heap</td>
      <td class="text-left">全量</td>
    </tr>
    <tr>
      <td colspan="4" class="text-left">
        <ul>
          <li>适用于小状态（本地）的测试和实验</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

当使用基于堆的 state backend 保存状态时，访问和更新涉及在堆上读写对象。
但是对于保存在 `RocksDBStateBackend` 中的对象，访问和更新涉及序列化和反序列化，所以会有更加巨大的开销。
但 RocksDB 的状态量仅受本地磁盘大小的限制。
还要注意，只有 `RocksDBStateBackend` 能够进行增量快照，这对于具有大量变化缓慢状态的应用程序来说是大有裨益的。

所有这些 state backends 都能够异步执行快照，这意味着它们可以在不妨碍正在进行的流处理的情况下执行快照。

{% top %}

## 状态快照

### 定义

* _快照_ -- 通用术语，指的是 Flink 作业状态的全局一致镜像。
快照包括指向每个数据源的指针（例如，到文件或 Kafka 分区的偏移量）以及每个作业的有状态运算符的状态副本，该状态副本包含了处理所有事件直至 sources 中的那些位置。
  
* _Checkpoint_ -- 一种由 Flink 自动执行的快照，其目的是能够从故障中恢复。
Checkpoints 可以是增量的，并为快速恢复进行了优化。

* _外部化的 Checkpoint_ -- 通常 checkpoints 不会被用户操纵。
Flink 只保留作业运行时的最近的 _n_ 个 checkpoints（_n_ 可配置），并在作业取消时删除它们。
但你可以将它们配置为保留，在这种情况下，你可以手动从中恢复。

* _Savepoint_ -- 用户出于某种操作目的（例如有状态的重新部署/升级/缩放操作）手动（或 API 调用）触发的快照。Savepoints 始终是完整的，并且已针对操作灵活性进行了优化。

### 状态快照如何工作？

Flink 使用 [Chandy-Lamport algorithm](https://en.wikipedia.org/wiki/Chandy-Lamport_algorithm) 算法的一种变体，称为异步屏障快照（_asynchronous barrier snapshotting_）。

当 checkpoint coordinator（job manager 的一部分）指示 task manager 开始 checkpoint 时，它会让所有 sources 记录它们的偏移量，并将编号的 _checkpoint barriers_ 插入到它们的流中。这些屏障（barriers）流经作业图（job graph），标注每个 checkpoint 前后的流部分。

<img src="{{ site.baseurl }}/fig/stream_barriers.svg" alt="Checkpoint barriers are inserted into the streams" class="center" width="80%" />

Checkpoint _n_ 将包含每个 operator 的 state，这些 state 产生于消费 **在 checkpoint barrier _n_ 之前的所有事件，并且不会包含任何在此之后的事件**。

当 job graph 中的每个 operator 接收到这些 barriers 其中之一时，它就会记录下其状态。
拥有两个输入流的 Operators（例如 `CoProcessFunction`）会执行 _barrier 对齐（barrier alignment）_ 以便快照将反应由于消费所有输入流 events 直到（但不超过）所有 barriers 而产生的状态。

<img src="{{ site.baseurl }}/fig/stream_aligning.svg" alt="Barrier alignment" class="center" width="100%" />

Flink 的 state backends 使用写时复制（copy-on-write）机制允许流处理在异步快照状态的旧版本时不受阻碍地继续。
只有当快照被持久保存时，这些旧版本的状态才会被垃圾回收。

### 精确一次保证

当流处理应用程序发生错误的时候，可能产生缺失，或者重复冗余的结果。
使用 Flink，根据你为应用程序和运行的集群所做的选择，可能会出现以下任何结果：

- Flink 不会努力从故障中恢复（_at most once_）
- 没有任何丢失，但是你可能会得到重复冗余的结果（_at least once_）
- 没有丢失或冗余重复（_exactly once_）

假设 Flink 通过倒回和重新发送 source 数据流从故障中恢复，当理想情况被描述为 **精确一次（exactly once）** 时，这并*不*意味着每个事件都将被精确一次处理。
相反，这意味着 _每一个事件都会影响 Flink 管理的状态精确一次_。

Barrier 只需要在提供精确一次的语义保证时需要对齐（Barrier alignment）。
如果不需要这种语义，可以通过将 Flink 配置为使用 `CheckpointingMode.AT_LEAST_ONCE` 来获得一些性能，这具有关闭屏障对齐（Barrier alignment）的效果。

### 端到端精确一次

为了实现端到端的精确一次，以便 sources 中的每个事件都仅精确一次对 sinks 生效，必须满足以下条件：

1. 你的 sources 必须是可重播的，并且
2. 你的 sinks 必须是事务性的（或幂等的）

{% top %}

## 实践练习

[Flink Operations Playground]({% link
try-flink/flink-operations-playground.zh.md %}) 包含 
[Observing Failure & Recovery]({% link
try-flink/flink-operations-playground.zh.md %}#observing-failure--recovery)
 部分。

{% top %}

## 延伸阅读

- [Stateful Stream Processing]({% link concepts/stateful-stream-processing.zh.md %})
- [State Backends]({% link ops/state/state_backends.zh.md %})
- [Data Sources 和 Sinks 的容错保证]({% link dev/connectors/guarantees.zh.md %})
- [开启和配置 Checkpointing]({% link dev/stream/state/checkpointing.zh.md %})
- [Checkpoints]({% link ops/state/checkpoints.zh.md %})
- [Savepoints]({% link ops/state/savepoints.zh.md %})
- [大状态与 Checkpoint 调优]({% link ops/state/large_state_tuning.zh.md %})
- [监控 Checkpoint]({% link monitoring/checkpoint_monitoring.zh.md %})
- [Task 故障恢复]({% link dev/task_failure_recovery.zh.md %})

{% top %}
