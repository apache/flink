---
title: 有状态流处理
weight: 2
type: docs
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

# 有状态流处理

<a name="what-is-state"></a>

## 状态是什么?

虽然数据流中的许多操作一次只查看一个单独的*事件*（例如事件解析器），但是有些操作会记住跨越多个事件的信息（例如窗口算子）。这些操作称为**有状态的**。

有状态操作的一些示例：

  - 当应用程序搜索某些事件模式时，状态将存储到目前为止遇到的事件序列。 
  - 以分钟/小时/天为单位聚合事件时，状态将保存待聚合的历史事件。
  - 在数据点的流上训练机器学习模型时，状态将保存模型参数的当前版本。
  - 当需要管理历史数据时，状态允许完整访问历史上已发生的事件。

Flink 需要知道状态以便使用 [checkpoints]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) 及 
[savepoints]({{< ref "docs/ops/state/savepoints" >}})。

在重新调整 Flink 应用程序的规模时也需要有关状态的信息，这是指 Flink 负责在并行实例之间重新分配状态。

[Queryable state]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) 带来了在运行时从 Flink 外部访问状态的可能性。

在使用状态时，阅读 [Flink 的 state backends]({{< ref "docs/ops/state/state_backends" >}}) 可能也很有用。Flink 提供了不同的 state backends 来指定状态的存储方式和位置。

{{< top >}}

## Keyed State

可以被认为，keyed state 以内嵌的键/值存储方式进行维护。
状态紧密的与有状态算子所读取的流一起被分区和分发。
因此，只有在 *keyed streams* 上（即，在交换完按键分割/分区数据之后）才能访问键/值的状态，
并且仅限于与当前事件的键所关联的值。
对齐流和状态的键可以确保所有的状态更新都是本地操作，并且可以在没有事务开销的情况下保证其一致性。
这种对齐还允许 Flink 重新分配状态并且透明地调整流分区。

{{< img src="/fig/state_partitioning.svg" alt="State and Partitioning" class="offset" width="50%" >}}

Keyed State被进一步组织成所谓的 *Key Groups*。
Key Groups 是 Flink 可以重新分配 Keyed State 的最小单位；Key Groups 的数量与已定义的最大并行度完全相同。
在执行期间，keyed 算子的每个并行实例都与一个或多个 Key Groups 的键一起工作。

<a name="state-persistence"></a>

## 状态持久性

Flink 使用 **流重播（stream reply）**和 **checkpointing** 的组合来实现容错。
Checkpoint 会在每个输入流及每个算子相应的状态中标记一个特定的点。
流式数据流可以恢复到 checkpoint 并维持其一致性 *(精确一次处理语义)*，
这是通过从 checkpoint 所标记的点处复原算子的状态并重播记录实现的。

Checkpoint 间隔是一种权衡执行期间容错开销和恢复时间（需要重放的记录数）的手段。

容错机制不断绘制分布式流数据流的快照（snapshot）。
对于状态较小的流式应用，这些快照非常轻量，可以被频繁绘制，对性能影响不大。
流式应用程序的状态存储在可配置的位置，通常是在分布式文件系统中。

如果出现程序故障（由于机器、网络或软件故障），Flink 会停止分布式流式数据流。
随后，系统重新启动算子并将其重置到最新的被成功保存的 checkpoint。
输入流同时被重置到状态快照点。
作为重启的并行数据流一部分而被处理的任何记录都保证不会影响先前的 checkpoint 状态。

{{< hint warning >}}
默认情况下，checkpointing 是被禁用的。有关如何启动和配置 checkpointing 的详情，请参阅 [Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})。
{{< /hint >}}

{{< hint info >}}
为了使该机制实现其全部保证，数据流 Source（如消息队列或消息代理）需要能够将流倒回到一个新近定义的点上。
[Apache Kafka](http://kafka.apache.org) 具有这种能力，而 Flink 的 Kafka 连接器则利用了这种能力。
有关 Flink 连接器所提供保证的详情，请参阅 [Data Source 和 Sink 的容错保证]({{< ref "docs/connectors/datastream/guarantees" >}})。
{{< /hint >}}

{{< hint info >}} 
因为 Flink 的 checkpoint 是通过分布式快照实现的，所以可以互换使用 *快照* 和 *checkpoint* 这两个词。 
通常我们也使用术语 *快照* 来表示 *checkpoint* 或 *savepoint*。
{{< /hint >}}

### Checkpointing

Flink 容错机制的核心部分是绘制分布式数据流和算子状态的一致性快照。
这些快照充当一致性的 checkpoints，在发生故障时系统可以回退到这些 checkpoints。
Flink 绘制这些快照的机制在《[Lightweight Asynchronous Snapshots for Distributed Dataflows](http://arxiv.org/abs/1506.08603)》中有描述。
它受到分布式快照的标准 [Chandy-Lamport 算法](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf) 的启发，并专门针对 Flink 的执行模型量身定制。

请记住，与 checkpointing 有关的所有事情都可以异步完成。
Checkpoint 分隔符（barriers）不会在锁定步骤中移动，操作可以异步快照它们的状态。

从 Flink 1.11 开始，checkpoints 既可以是对齐的（aligned）也可以是非对齐的（unaligned）。 
在本节中，我们首先描述对齐的 checkpoints。

<a name="barriers"></a>

#### 分隔符（Barriers）

Flink 分布式快照的一个核心元素是*流分隔符（steam barriers）*。
这些分隔符被插入到数据流中，并作为数据流的一部分与原本的记录一起流动。 
分隔符永远不会超过记录，它们严格按照线性流动。
分隔符将数据流中的记录分成进入当前快照的记录集和进入下一个快照的记录。
每个分隔符都带有快照的 ID，在它之前的记录都属于该快照。 
分隔符不会打断数据的流动，因此非常轻巧。
数据流中可以同时存在来自不同快照的多个分隔符，这意味着各种快照可能同时发生。

<div style="text-align: center">
  {{< img src="/fig/stream_barriers.svg" alt="Checkpoint barriers in data streams" width="60%" >}}
</div>

数据流的分隔符在流的 source 处被插入到并行数据流中。
代表着快照 *n* 的分隔符所插入的位置（可称为 <i>S<sub>n</sub></i>）是源数据流中此快照覆盖范围的终点。
例如，在 Apache Kafka 中，此位置是分区中最后一条记录的偏移量。
此位置信息将被报告给 *checkpoint 协调器（coordinator）* （Flink 的 JobManager）。

分隔符逐渐向下游流动。
当一个中间算子从它的所有输入流中接收到快照 *n* 的分隔符时，它会向它的所有输出流发出同样的分隔符。
一旦 Sink 算子（流式 DAG 的末尾）从其所有输入流中接收到分隔符 *n*，它就会向 checkpoint 协调器确认快照 *n*。
在所有 Sink 都确认快照后，它被认为已完成。

一旦快照 *n* 完成，该 Job 将不再向 Sink 询问 <i>S<sub>n</sub></i> 之前的记录，
因为到那时这些记录（及其后继记录） 已经通过了整个数据流拓扑。

<div style="text-align: center">
  {{< img src="/fig/stream_aligning.svg" alt="Aligning data streams at operators with multiple inputs" width="60%" >}}
</div>

接受多于一个输入流的算子需要将这些输入流*对齐*到快照分隔符上。上图说明了这一点：

  - 一旦算子从传入流接收到快照分隔符 *n*，那么在它从其他输入流中接收到分隔符 *n* 之前，
该算子都无法进一步处理来自该流的任何记录。因为不这样做的话，算子将会混合属于快照 *n* 和属于 *n+1* 的记录。
  - 一旦最后一个流接收到分隔符 *n*，算子会发出所有待处理的传出记录，并且算子自己会在随后发出快照 *n* 的分隔符。
  - 它对状态进行快照并继续处理来自所有输入流的记录，在处理这些来自流的记录之前会处理来自输入缓冲区的记录。
  - 最后，算子将状态异步写入 state backend。

请注意，所有具有多个输入的算子以及 shuffle 后在消耗多个上游子任务的输出流时的算子都需要对齐。

<a name="snapshotting-operator-state"></a>

#### 拍摄算子状态快照

当算子含有任何形式的*状态*时，该状态也必须是快照的一部分。

当算子已经从其输入流中接收到了所有的快照分隔符，并且在它将分隔符发送到输出流之前的时间点上，
算子会拍摄其状态的快照。
此时，对来自分隔符之前记录的所有状态更新都已完成，并且不会执行对于依赖来自分隔符之后记录的更新。
因为快照的状态可能很大，所以它储存在一个可配置的 *[state backend]({{< ref "docs/ops/state/state_backends" >}})* 中。
默认情况下，它们储存在 JobManager 的内存中，但对于生产用途，应当配置分布式可靠存储（如HDFS）。
状态储存完毕后，算子确认 checkpoint，并向输出流发出快照分隔符，然后继续。

生成的快照现在包括：

  - 对于每个并行流数据源，快照开始时流中的偏移量/位置
  - 对于每个算子，一个指向作为快照一部分储存的状态的指针

<div style="text-align: center">
  {{< img src="/fig/checkpointing.svg" alt="Illustration of the Checkpointing Mechanism" width="75%" >}}
</div>

<a name="recovery"></a>

#### 从故障中恢复

这种机制下的恢复很简单：一旦发生故障，Flink 会选择最近记录的 checkpoint *k*。
然后系统重新部署整个分布式数据流，并为每个算子提供作为 checkpoint *k* 一部分的快照的状态。
设置 source 从其位置 <i>S<sub>k</sub></i> 开始读取流。
例如在 Apache Kafka 中，这意味着告诉消费者从偏移量 <i>S<sub>k</sub></i> 开始获取。

如果状态是增量快照，则算子从最新的一个完整快照的状态开始，然后将一系列增量快照更新应用于该状态。

更详细信息请参阅[重启策略]({{< ref "docs/ops/state/task_failure_recovery" >}}#restart-strategies)。

<a name="unaligned-checkpointing"></a>

### 非对齐的 Checkpointing

Checkpointing 也可以在未对齐的情况下执行。
其基本思想是，只要正在处理中（in-flight）的数据成为算子状态的一部分，checkpoints 就可以跨过所有处理中的数据。

请注意，这种方法实际上更接近 [Chandy-Lamport 算法](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf)，但 Flink 仍然会将分隔符插入源中以避免 checkpoint 协调器过载。

{{< img src="/fig/stream_unaligning.svg" alt="Unaligned checkpointing" >}}

该图描述了算子如何处理未对齐的 checkpoint 分隔符：

- 算子对存储在其输入缓冲区中的第一个分隔符做出反应。
- 它通过将分隔符添加到输出缓冲区的末尾的方式立即将分隔符转发给下游算子。
- 算子将所有被超越的记录标记为异步存储，并创建其自身状态的快照。

因此，算子只需短暂停止输入处理以标记缓冲区、转发分隔符并创建其他状态的快照。
  
非对齐的 checkpointing 可确保分隔符尽快到达 Sink。
它特别适用于具有至少一个缓慢移动的数据路径的应用程序，这种程序的对齐时间可能长达数小时。
但是，由于它增加了额外的 I/O 压力，因此对于由 state backend 的 I/O 所造成的瓶颈毫无办法。
对于其他限制的更深入讨论请参阅 [ops]({{< ref "docs/ops/state/checkpoints" >}}#unaligned-checkpoints)。

请注意，savepoints 总是对齐的。

<a name="unaligned-recovery"></a>

#### 非对齐的恢复

在开始处理任何来自上游算子的数据之前，算子首先在未对齐的 checkpointing 中恢复处理中的数据。
除此之外，它执行与 [恢复对齐的 checkpoints](#recovery) 期间相同的步骤。

### State Backends

存储键/值索引的确切数据结构取决于所选的 [state backend]({{< ref "docs/ops/state/state_backends" >}})。
一种 state backend 将数据存储在内存中的哈希散列中，另一种则使用 [RocksDB](http://rocksdb.org) 作为键/值存储。 
除了定义保存状态的数据结构外，state backend 还实现了获取键/值状态的任意时间点快照并将此快照存储为 checkpoint 一部分的逻辑。
可以在不更改应用程序逻辑的情况下配置 state backend。

{{< img src="/fig/checkpoints.svg" alt="checkpoints and snapshots" class="offset" width="60%" >}}


{{< top >}}

### Savepoints

所有使用 checkpointing 的程序都可以从 ** savepoint** 恢复执行。 
Savepoints 允许在不丢失任何状态的情况下更新你的程序和 Flink 集群。

[Savepoints]({{< ref "docs/ops/state/savepoints" >}}) 是**手动触发的 checkpoints**，
它拍摄程序的快照并将其写入 state backend。 
为此，它们依靠常规的 checkpointing 机制。

Savepoints 类似于 checkpoints，不同之处在于前者**由用户触发**并且**不会在较新的 checkpoints 完成时自动过期**。


{{< top >}}

<a name="exactly-once-vs-at-least-once"></a>

### 精确一次 vs. 至少一次

对齐的操作可能会增加流式程序的延迟。
通常，这种额外的延迟大约为几个毫秒，但在一些情况中，我们也已经看到某些异常值的延迟有显著的增加。
对于那些需要对所有记录都维持超低延迟（几毫秒）的应用程序，Flink 提供了一个可以在 checkpoint 期间跳过流对齐的开关。
但是只要算子在输入中看到 checkpoint 分隔符，那么它们仍然会绘制 checkpoint 快照。

当对齐被跳过时，即便 checkpoint *n* 的一些 checkpoint 分隔符到达，算子仍将继续处理所有的输入。
因此，在拍摄 checkpoint *n* 的状态快照之前，算子也会处理属于 checkpoint *n+1* 的元素。
在恢复时，这些记录将作为重复数据出现，因为它们既被包涵在 checkpoint *n* 的状态快照中，
又将作为 checkpoint *n* 后续数据的一部分被重播。

{{< hint info >}}
对齐的概念仅出现于具有多个前序（连接）的算子以及具有多个发送者（在流重分区/ shuffle 之后）的算子。
正因如此，即使在*至少一次*模式下，只存在易并行（embarrassingly parallel）流式操作（`map()`、`flatMap()`、`filter()`、...）的数据流实际上提供了*精确一次*的保证。
{{< /hint >}}

{{< top >}}

<a name="state-and-fault-tolerance-in-batch-programs"></a>

## 批处理程序中的状态和容错

Flink 将[批处理程序]({{< ref "docs/dev/dataset/overview" >}})作为流式程序的一种特殊情况执行，其特殊之处在于流是有界的（元素数量有限）。
在内部，*DataSet* 被视为是一种数据流。
因此，上述关于流式程序的概念以同样的方式适用于批处理程序，但是其中也有一些细微的差别：


  - [批处理程序的容错]({{< ref "docs/ops/state/task_failure_recovery" >}})不使用 checkpointing。
而是通过完全重播流来进行恢复。因为输入是有界的，所以完全可以做到这一点。这种方式虽然将成本更多地推向了恢复，但却降低了常规处理的开销（因为它避免了 checkpoints）。

  - DataSet API 中的有状态操作使用了简化的内存/核外（out-of-core）数据结构，而不是键/值索引。

  - DataSet API 引入了特殊的同步（基于superstep）迭代，这仅在有界流上才有可能。有关详细信息，请查看[迭代文档]({{< ref "docs/dev/dataset/iterations" >}})。

{{< top >}}
