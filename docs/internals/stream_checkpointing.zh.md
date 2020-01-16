---
title:  "数据流容错"
nav-title: 数据流容错
nav-parent_id: internals
nav-pos: 3
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

本文将介绍 Flink 流数据处理的容错机制（fault tolerance mechanism）。

* This will be replaced by the TOC
{:toc}


## 介绍

Apache Flink 提供了一种容错机制，可以一致地恢复流处理应用程序的状态。
这种机制确保了即使发生错误，程序最终也将实现数据流中记录的**精确一次**（exactly once）投送。注意，这里可以将语义*降低到至少一次（at least once）*。（下面会提到）

容错机制会不断生成分布式数据流的快照（snapshot）。对于状态量较小的流处理程序，这些快照会非常轻巧，并且生成时几乎不会影响性能。这些流处理程序的状态会存储的位置是可以配置的（例如 master 节点或者 HDFS 上）。

如果发生了程序崩溃（由于机器、网络或者软件原因崩溃），Flink 会终止分布式流处理程序。然后系统会重启算子并将它们重置到最近一的个有效的 ckeckpoint。输入流会被重置到状态快照的点。重启的并行数据流中的任何记录都能确保不属于前一个 checkpoint 状态。

*注释：* 默认情况下，checkpoint 不启用。参见 [Checkpointing]({{ site.baseurl }}/zh/dev/stream/state/checkpointing.html) 以获取如何启用和配置 checkpoint 的详细信息。

*注释：* 为了实现这种机制全部功能，流数据源（例如消息队列或代理）需要能够将数据流回退到定义的最近点。[Apache Kafka](http://kafka.apache.org) 就有这种能力并且 Flink 与 Kafka 的连接器可以发挥出这种能力。参阅 [数据源和 sink 的容错保证]({{ site.baseurl }}/zh/dev/connectors/guarantees.html) 以获取更多关于 FLink 连接器提供的保证的信息。

*注释：* 因为 Flink 的 checkpoint 是通过分布式快照技术实现的，所以我们可以可以互换使用*快照*和 *checkpoint*。


## Checkpointing

FLink 容错机制的核心是是不断生成算子和数据流状态的快照。这些快照作为一致性 checkpoint，在系统发生故障时可以回退至这些 checkpoint。Flink 生成这些快照的机制已经在“[分布式数据流的轻量级异步快照](http://arxiv.org/abs/1506.08603)”中论述了。它是受到用于分布式快照的标准 [Chandy-Lamport 算法](http://research.microsoft.com/en-us/um/people/lamport/pubs/chandy.pdf) 的启发并针对 Flink 的执行模型量身定制的。



### 屏障（Barriers）

Flink 分布式快照技术的核心元素是*数据流屏障*（stream barriers）。这些屏障会被插入到数据流中，并与记录一起作为数据流的一部分继续流动。屏障永远不会超过记录，它们严格按照顺序流动。屏障将数据流中的记录分成计入当前快照的记录集和计入下一个快照的记录集。每个屏障都持有一个快照的 ID，快照记录着屏障之前的推送。屏障不会中断数据的流动，因此是十分轻巧的。来自不同快照的屏障可以同时存在于同一个数据流中，这就是说多种快照可能同时进行。

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/stream_barriers.svg" alt="Checkpoint barriers in data streams" style="width:60%; padding-top:10px; padding-bottom:10px;" />
</div>

流屏障在流数据源处被插入到并行的数据流中。快照 *n* 的屏障插入点（我们就称它为 <i>S<sub>n</sub></i>）是流数据源中快照覆盖数据的位置。例如，在 Apache Kafka 中，这个位置将是分区中最后一条记录的偏移量。这个位置 <i>S<sub>n</sub></i> 会汇报给*checkpoint 协调器*（checkpoint coordinator）（Flink 的任务管理器（JobManager））。

然后这些屏障向下流动。当中间算子从其所有输入流中收到快照 *n* 的屏障时，它会向它所有的输出流发出快照 *n* 的屏障。一旦 sink 算子（有向无环图的终点）从其所有的输入流中收到了快照 *n* 的屏障，它向 checkpoint 协调器确认快照 *n* 的完成。直到所有的 sink 都确认了快照，快照 *n* 才被视作完成。

一旦快照 *n* 完成，作业不会再向数据源请求 <i>S<sub>n</sub></i> 之前的记录，因为此时这些记录（及其子记录）将通过整个数据流拓扑。

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/stream_aligning.svg" alt="Aligning data streams at operators with multiple inputs" style="width:100%; padding-top:10px; padding-bottom:10px;" />
</div>

收到多个数据流的算子需要在快照屏障上*对齐*输入流。上图说明了这一点：

  - 当算子从一个输入数据流中收到快照屏障 *n* 时，它将无法继续处理该数据流到来的记录，直至它也收到来自其它输入数据流的快照屏障。否则它会混合属于快照 *n* 和 *n+1* 的记录。
  - 将屏障 *n* 送达的数据流会被搁置。后续从该数据流到达的记录不会处理，但会将它们放入输入缓冲区。
  - 一旦最后一个数据流送达屏障 *n*，算子会发出所有挂起的输出记录，然后发出快照 *n* 屏障本身。
  - 完成后，算子会继续处理来自输入流的记录，对于缓冲区中的记录处理的优先级高于数据流中的。


### 状态（State）

如果算子包含了任何类型的*状态*，那么这个状态也必须包含在快照中。算子状态有多种类型：

  - *用户自定义状态*: 这种状态是直接由转换函数（transformation functions）（例如 `map()` 或者 `filter()`）产生和修改的。详情参见[流处理程序中的状态]({{ site.baseurl }}/zh/dev/stream/state/index.html)。
  - *系统状态*: 这种状态记录着算子计算过程中的部分缓存区。这种状态的典型用例是 *窗口缓存区*，在窗口中，系统收集（和聚合）窗口中的记录，直至窗口结算并销毁。

算子会在它们从输入流中收到所有快照屏障之后，向输出流发出屏障之前将自身状态生成快照。届时，屏障之前的记录对状态进行的所有更新都会完成，并且依赖于屏障后记录的更新操作不会进行。因为快照的状态可能非常庞大，所以用于存储它的 *[state backend]({{ site.baseurl }}/zh/ops/state/state_backends.html)* 是可配置的。默认存放在 JobManager 的内存中，但在实际生产中应当配置可靠的分布式存储（例如 HDFS）。状态存储后，算子会通知 checkpoint、向输出流中发出快照屏障并继续工作。

生成的快照目前包括：

  - 对于每个并行的流数据源，快照启动时流中的偏移量/位置
  - 对每个算子，指向作为快照一部分存储的状态的指针

<div style="text-align: center">
  <img src="{{ site.baseurl }}/fig/checkpointing.svg" alt="Illustration of the Checkpointing Mechanism" style="width:100%; padding-top:10px; padding-bottom:10px;" />
</div>


### 精确一次和至少一次

对齐过程可能会给流式处理程序增加延迟。通常，这种额外的延迟大约是几毫秒，但是我们已经看到过一些异常值的延迟明显增加的情况。对于要求所有记录始终具有超低延迟（几毫秒）的应用程序，Flink有一个配置项，可以在 checkpoint 期间跳过流对齐过程。当算子从每个输入中看到 checkpoint 屏障时，仍会生成 checkpoint 快照。

当跳过对齐过程时，即使在 checkpoint *n* 的一些 checkpoint 屏障到达之后，算子仍会继续处理所有输入。如此，在 checkpoint *n* 的状态快照生成之前，算子也会处理属于 checkpoint *n+1* 的元素。
在还原时，这些记录将作为副本出现，因为它们都包含在 checkpoint *n* 的状态快照中，并且将在 checkpoint *n* 之后作为数据的一部分重新处理。

*注释*： 对齐过程仅针对具有多个前置任务（joins）的算子以及有多个输出（在流重新分区/shuffle 之后）的算子。正因为如此，令人尴尬的是实际上只有并行流操作（`map()`、`flatMap()`、`filter()`、……）的数据流提供了*精确一次*的保证，即使是在*至少一次*语义下。


### 异步状态快照（Asynchronous State Snapshots）

注意，上述机制意味着算子在 *state backend* 中存储状态快照时会停止处理输入记录。每次生成快照时，*异步*状态快照都会引入延迟。

可以让算子在存储状态快照的同时继续处理，从而有效地让状态快照在后台以*异步地*方式进行。为了做到这一点，算子必须能够生成状态对象，状态对象的存储方式应该使得对状态的进一步修改不会影响该状态对象。例如，*即写即拷*的数据结构（如在 RockDB 中使用的）有这样的行为特性。

在算子从它的输入中收到 checkpoint 屏障之后，算子启动其状态的异步快照复制。它会立即向输出流中发出屏障并继续进行常规的流处理。后台复制过程完成后，它将向 checkpoint 协调器（JobManager）确认 checkpoint。checkpoint 现在只有在所有 sink 都接收到屏障并且所有有状态算子都已确认其已完成备份（可能是在屏障到达 sink 之后）之后才完成。

关于状态快照的详情请参考 [State Backends]({{ site.baseurl }}/zh/ops/state/state_backends.html)。


## 恢复（Recovery）

这种机制下的恢复是简单易懂的：一旦失败，FLink 选择最新的完成的 checkpoint *k*。然后，系统重新部署整个分布式数据流，并为每个算子提供作为 checkpoint *k* 快照的一部分的状态。将数据源的设置为从流的位置 <i>S<sub>k</sub></i> 开始读取。例如在 Apache Kafka 中，上面的方法就是告诉 consumer 从偏移量（offset） <i>S<sub>k</sub></i> 处开始拉数据。

如果状态是以增量方式快照的，则算子从最新的完整的快照状态开始，然后对该状态进行一系列增量快照更新。

详情参见[重启策略]({{ site.baseurl }}/zh/dev/restart_strategies.html)。

## 算子快照的实现

当算子快照生成，它有两个部分：**同步**的部分和**异步**的部分。

算子和 state backends 将它们的快照作为 Java `FutureTask` 提供。这个任务包括*同步*部分完成而*异步*部分挂起的状态。异步部分随后由该 checkpoint 的后台线程执行。

Checkpoint 后的算子完全同步的返回一个已完成的 `FutureTask`。
如果需要执行异步操作，则在该 `FutureTask` 的 `run（）` 方法中执行。

这种任务是可以取消的，因此可以释放数据流和其它消耗资源的程序。

{% top %}
