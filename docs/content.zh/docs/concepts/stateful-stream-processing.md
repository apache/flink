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

## 什么是状态？

虽然数据流中的很多操作一次只着眼于一个单独的事件（例如事件解析器），但有些操作会记住多个事件的信息（例如窗口算子）。
这些操作称为**有状态的**（stateful）。

一些有状态操作的示例：

  - 当应用程序搜索某些事件模式时，状态将保存目前为止遇到事件的顺序。
  - 当聚合每分钟/每小时/每天事件时，状态会持有待处理的聚合。
  - 当在数据点的流上训练一个机器学习模型时，状态会保存当前模型参数的版本。
  - 当需要管理历史数据时，状态允许有效访问过去发生的事件。

Flink 需要使用
[checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})
和 [savepoint]({{< ref "docs/ops/state/savepoints" >}}) 来感知状态并进行容错。

我们知道状态是支持 Flink 程序的动态伸缩的，这意味着 Flink 会跨多个并行实例重新分配状态。

[可查询的状态]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}})允许你在 Flink 运行时从外部访问状态。

在使用状态时，参考 [Flink 的状态后端]({{< ref "docs/ops/state/state_backends" >}})也会有一些帮助。
Flink 提供了不同的状态后端，用于指定状态存储的方式和位置。

{{< top >}}

## Keyed State

Keyed state 被维护在一个键/值存储的地方。状态和有状态算子读取的流一起被严格地分区和分布。
因此，只能在 *keyed streams* 上访问键/值状态，即在有键的/分区的数据交换后，
并且仅限于和当前事件的键相关联的值。对齐流和状态的键确保所有状态更新都是本地操作，保证一致性而没有事务开销。
这个对齐也允许 Flink 重新分布状态并透明地调整流的分区。

{{< img src="/fig/state_partitioning.svg" alt="状态和分区" class="offset" width="50%" >}}

Keyed State 被进一步组织成*键组*（Key Groups）。键组是 Flink 可以重新分布 Keyed State 的原子单元，键组的个数和定义的最大并行度保持一致。
在执行期间，每个 keyed operator 的并行实例和一个或多个键组的键一起使用。

## 状态持久化

Flink 使用**流重放**（stream replay）和**检查点**（checkpointing）的组合来实现容错。
检查点标记了每一个输入流的特定点和每个算子的相应状态。 流式数据流可以从检查点恢复，
同时通过重新恢复算子状态并从检查点重放记录来保证一致性 *（精确一次处理语义）*。

检查点间隔是一种权衡执行期间容错开销与恢复时间（需要重放的记录数）的方法。

容错机制不断地绘制分布式数据流快照。对于小状态的流式应用，它们的快照非常轻量，可以被频繁绘制，对性能没有太大影响。
流式应用的状态存储在一个可配置的地方，通常是在分布式文件系统中。

如果程序出现故障（由于机器、网络或者软件故障），Flink 会停止分布式数据流。之后系统会重启算子并且将它们重置到最近的成功检查点。
输入流被重置到状态快照的点。作为重新启动的并行数据流的一部分被处理的任何记录都保证不会影响之前检查点的状态。

{{< hint warning >}}
默认情况下，检查点是被禁用的。关于如何启用和配置检查点的详细信息，请参阅[检查点]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}})。
{{< /hint >}}

{{< hint info >}}
为了让该机制完整实现一致性保证，数据流的源（例如消息队列或 broker）需要能够将流回滚到定义的最近点。
[Apache Kafka](http://kafka.apache.org) 具有这个能力，Flink 的 Kafka 
连接器利用了这一点。关于 Flink 连接器提供的保证的更多信息请参阅[数据源和 sink 的容错保证]({{< ref "docs/connectors/datastream/guarantees" >}})
{{< /hint >}}

{{< hint info >}} 
由于 Flink 的检查点是通过分布式快照实现的，所以我们交替使用*快照*（snapshot）和*检查点*（checkpoint）两个词。
通常我们也使用术语*快照*来表示*检查点*或*保存点*（savepoint）。
{{< /hint >}}

### 检查点

Flink 容错机制的核心部分是绘制分布式数据流和算子状态的一致快照。 这些快照充当一致性检查点，
系统可以在发生故障时可以回退到这些检查点。[分布式数据流的轻量级异步快照](http://arxiv.org/abs/1506.08603)中描述了 Flink 绘制这些快照的机制。
它受到标准 [Chandy-Lamport 算法](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf)的启发，
专门针对 Flink 的执行模型量身定制。

请记住，与检查点有关的一切都可以异步完成。检查点屏障（checkpoint barrier）不会同步移动，操作可以异步快照它们的状态。

从 Flink 1.11 开始，检查点可以在对齐或不对齐的情况下进行。在本节中，我们先描述对齐的检查点。

#### 屏障（barrier）

Flink 分布式快照的一个核心元素是*流屏障*（stream barriers）。
这些屏障被注入到数据流中，并作为数据流的一部分与记录一起流动。屏障永远不会超过记录，它们严格按照顺序流动。
屏障将数据流中的记录分为进入当前快照的记录集和进入下一个快照的记录。每个屏障都带有它推送到它前面的记录的快照的 ID。
屏障不会中断数据流因此非常轻量。来自不同快照的多个屏障可以同时在流中，这意味着各种快照可能会同时发生。

<div style="text-align: center">
  {{< img src="/fig/stream_barriers.svg" alt="数据流中的检查点屏障" width="60%" >}}
</div>

流屏障被注入到数据源的并行数据流中。快照 *n* 的屏障被注入到的位置（我们称之为 <i>S<sub>n</sub></i>
）是数据源中快照覆盖数据的位置。例如，在 Apache Kafka 中，这个位置就是分区最后一条记录的 offset。 
这个位置 <i>S<sub>n</sub></i> 会上报给*检查点协调器*（checkpoint coordinator）（Flink 的 JobManager）。

然后屏障向下游流动。当中间的算子已经从它所有的输入流中接收到了快照屏障 *n*，它会将快照 *n* 的屏障发送到它所有的输出流中。 
一旦 sink 算子（DAG 流结束）从其所有的输入流中接收到了屏障 *n*，它就会向检查点协调器确认快照 *n*。 
等到所有的 sink 都已经确认了这个快照，它就被认为完成了。

一旦快照 *n* 完成了，作业将不再向源询问 <i>S<sub>n</sub></i> 之前的记录，
因此这时这些记录（和它们的后代记录）将通过整个数据流拓扑。

<div style="text-align: center">
  {{< img src="/fig/stream_aligning.svg" alt="将算子处数据流与多个输入对齐" width="60%" >}}
</div>

接收多个输入流的算子需要在快照屏障上和输入流*对齐*。上图说明了这一点：

  - 一旦算子从一个输入流里接收到了快照屏障 *n*，它就无法处理该流中的任何进一步记录，直到它也从其他输入流中接收到屏障 *n*。
    否则，它将混合属于快照 *n* 的记录和属于快照 *n+1* 的记录。
  - 一旦最后一个流接收到了屏障 *n*，算子将发出所有待输出的记录，然后自己发出快照 *n* 的屏障。
  - 它对状态进行快照并恢复处理来自所有输入流的记录，在处理来自流的记录之前处理来自输入缓冲区的记录。
  - 最后，算子将状态异步写入状态后端（state backend）。
  
请注意，所有具有多个输入的算子以及在使用多个上游子任务的输出流时经过 shuffle 后的算子都需要对齐。

#### 对算子状态进行快照

当算子包含任何形式的*状态*（state）时，该状态也必须是快照的一部分。

算子在收到来自输入流的所有快照屏障时以及将屏障发送到其输出流之前对其状态进行快照。
在这个时候，所有的来自屏障之前的记录状态更新已经完成，并且没有依赖于应用屏障后的记录的更新。
由于快照状态会比较大，所以它存储在一个可配置的 *[状态后端]({{< ref "docs/ops/state/state_backends" >}})* 中。 
默认地，它是 JobManager 的内存，但是对于生产用途，应该配置分布式可靠存储（例如 HDFS）。
在状态被存储之后，算子确认检查点，将快照屏障发送到输出流中，然后继续。

生成的快照现在包含：

  - 对于每个并行流数据源，快照开始时在流中的偏移量/位置
  - 对于每个算子，指向作为快照一部分存储的状态的指针

<div style="text-align: center">
  {{< img src="/fig/checkpointing.svg" alt="检查点机制插图" width="75%" >}}
</div>

<a name="recovery"></a>

#### 恢复

这种机制下的恢复很简单：发生故障时，Flink 选择最近完成的检查点 *k*。系统接着会重新部署整个分布式数据流（dataflow），
并为每个算子提供作为检查点 *k* 一部分的快照状态。源被设置为从位置 <i>S<sub>k</sub></i> 开始读流。 
例如在 Apache Kafka 中，这意味着告诉消费者（consumer）从 offset <i>S<sub>k</sub></i> 开始获取。 

如果状态被增量快照，算子就从最近全量快照的状态开始，接着将一系列的增量快照（incremental snapshot）更新应用到这个状态上。

有关更多信息，请查阅[重启策略]({{< ref "docs/ops/state/task_failure_recovery" >}}#restart-strategies)。

### 未对齐的检查点

检查点（checkpointing）也可以不对齐地执行。
基本思想是，检查点可以超过所有正在处理的（in-flight）数据，只要正在处理的数据成为算子状态的一部分。

请注意，这种方法实际上更接近于 [Chandy-Lamport 算法](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf) ，
但是 Flink 仍然在源中插入屏障来避免检查点协调器过载。

{{< img src="/fig/stream_unaligning.svg" alt="未对齐的检查点" >}}

该图描绘了一个算子如何处理未对齐的检查点屏障：

- 算子对存储在其输入缓冲区中的第一个屏障做出反应。
- 它通过将屏障添加到输出缓冲区的末尾的方式来立即将屏障转发给下游算子。
- 算子将所有被超过的记录标记为异步存储并创建其自身状态的快照。
 
因此，算子只简单地停止输入处理以标记缓冲区，转发屏障，并且创建另一个状态的快照。
  
未对齐的检查点确保屏障会尽可能快地达到 sink 。这非常适合那些具有至少一个缓慢移动数据路径的应用程序，其中对齐时间可以达到数小时。
然而，由于它增加了额外的 I/O 压力，当到状态后端的 I/O 成为瓶颈时，它将无济于事。 有关其他限制，
请参阅 [ops]({{< ref "docs/ops/state/checkpoints" >}}#unaligned-checkpoints)中更深入的讨论。

注意，保存点将总是对齐。

#### 未对齐的恢复

在开始处理来自未对齐检查点的上游算子的任何数据之前，算子首先恢复运行中（in-flight）的数据。除此之外，它执行和在[对齐的检查点的恢复](#recovery)期间相同的步骤。

### 状态后端

存储键/值索引的确切数据结构取决于所选的[状态后端]({{< ref "docs/ops/state/state_backends" >}})。
一个状态后端将数据存储在内存哈希映射中，另外一个状态后端使用 [RocksDB](http://rocksdb.org) 作为键/值存储。
除了定义保存状态的数据结构之外，状态后端还实现了获取键/值状态的时间点（point-in-time）的快照并将该快照作为检查点的一部分存储的逻辑。
可以在不更改应用程序逻辑的情况下配置状态后端。

{{< img src="/fig/checkpoints.svg" alt="检查点和快照" class="offset" width="60%" >}}

{{< top >}}

### 保存点

所有使用检查点的程序都可以从**保存点**恢复运行。保存点允许在不丢失任何状态的情况下更新你的程序和 Flink 集群。

[保存点]({{< ref "docs/ops/state/savepoints" >}})是**手动触发检查点**的，它拍摄程序的快照并将其写出到状态后端。
为此，它们依赖于定期的检查点机制。

保存点和检查点类似，不同之处在于保存点**由用户触发**并且**不会在新的检查点完成时自动过期**。

{{< top >}}

### 精确一次 vs 最少一次

对齐的步骤可能会给流处理程序增加延迟。 通常，这额外的延迟大约是几毫秒，但是我们已经看到一些异常值的延迟显著增加的情况。
对于要求所有记录始终保持超低延迟（几毫秒）的应用程序，Flink 有一个开关可以在检查点期间跳过流对齐。
一旦算子接收到任何一个输入的检查点屏障，检查点快照就仍然会被绘制。

如果跳过对齐，算子继续处理所有输入，即使在检查点 *n* 的一些检查点屏障到达之后也是如此。
这样，算子还会在获取检查点 *n* 的状态快照之前处理属于检查点 *n+1* 的元素。
在恢复时，这些记录将会重复，因为它们都包含在检查点 *n* 的状态快照中，并且将在检查点 *n* 之后重放。

{{< hint info >}}
对齐只发生在有多个前驱（连接）的算子和有多个发送者（在流重新分区/shuffle 之后）的算子。
因此，即使在*至少一次*（at least once）模式下，只包含高度并行流操作（`map()`、`flatMap()`、`filter()` 等）的数据流也可以提供*精确一次*（exactly once）保证。
{{< /hint >}}

{{< top >}}

## 批处理中的状态和容错

Flink 执行[批处理程序]({{< ref "docs/dev/dataset/overview" >}})作为流处理程序的一个特例，其中流是有界的（元素数量有限）。
*DataSet* 在内部被视为数据流。因此，上述概念以同样的方式适用于批处理程序以及流处理程序，除了少数例外：

  - [批处理程序的容错]({{< ref "docs/ops/state/task_failure_recovery" >}})
    并不使用检查点。恢复是通过完全重放流来实现的。这是可能的，因为输入是有界的。
    这将导致恢复成本更高，但是使得定期的处理开销更小，因为它避免了生成检查点。

  - DataSet API 中有状态的操作使用简化的内存/核外（out-of-core）数据结构，而不是键/值索引。

  - DataSet API 引入了特殊的同步（基于 superstep）迭代，这只能在有界流上使用。
    有关详细信息，请查看[迭代文档]({{< ref "docs/dev/dataset/iterations" >}})。

{{< top >}}
