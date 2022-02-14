---
title: "网络缓冲调优"
weight: 100
type: docs
aliases:
  - /zh/deployment/memory/network_mem_tuning.html
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

# 网络内存调优指南

## 概述

Flink 中每条消息都会被放到*网络缓冲（network buffer）* 中，并以此为最小单位发送到下一个 subtask。
为了维持连续的高吞吐，Flink 在传输过程的输入端和输出端使用了*网络缓冲队列*。

每个 subtask 都有一个输入队列来接收数据和一个输出队列来发送数据到下一个 subtask。
在 pipeline 场景，拥有更多的中间缓存数据可以使 Flink 提供更高、更富有弹性的吞吐量，但是也会增加快照时间。

只有所有的 subtask 都收到了全部注入的 checkpoint barrier 才能完成快照。
在[对齐的 checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#checkpointing) 中，checkpoint barrier 会跟着网络缓冲数据在 job graph 中流动。
缓冲数据越多，checkpoint barrier 流动的时间就越长。在[非对齐的 checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#unaligned-checkpointing) 中，缓冲数据越多，checkpoint 就会越大，因为这些数据都会被持久化到 checkpoint 中。

## 缓冲消胀机制（Buffer Debloating）

之前，配置缓冲数据量的唯一方法是指定缓冲区的数量和大小。然而，因为每次部署的不同很难配置一组完美的参数。
Flink 1.14 新引入的缓冲消胀机制尝试通过自动调整缓冲数据量到一个合理值来解决这个问题。

缓冲消胀功能计算 subtask 可能达到的最大吞吐（始终保持繁忙状态时）并且通过调整缓冲数据量来使得数据的消费时间达到配置值。

可以通过设置 `taskmanager.network.memory.buffer-debloat.enabled` 为 `true` 来开启缓冲消胀机制。
通过设置 `taskmanager.network.memory.buffer-debloat.target` 为 `duration` 类型的值来指定消费缓冲数据的目标时间。
默认值应该能满足大多数场景。

这个功能使用过去的吞吐数据来预测消费剩余缓冲数据的时间。如果预测不准，缓冲消胀机制会导致以下问题：
* 没有足够的缓存数据来提供全量吞吐。
* 有太多缓冲数据对 checkpoint barrier 推进或者非对齐的 checkpoint 的大小造成不良影响。

如果您的作业负载经常变化（即，突如其来的数据尖峰，定期的窗口聚合触发或者 join ），您可能需要调整以下设置：

* `taskmanager.network.memory.buffer-debloat.period`：这是缓冲区大小重算的最小时间周期。周期越小，缓冲消胀机制的反应时间就越快，但是必要的计算会消耗更多的CPU。

* `taskmanager.network.memory.buffer-debloat.samples`：调整用于计算平均吞吐量的采样数。采集样本的频率可以通过 `taskmanager.network.memory.buffer-debloat.period` 来设置。样本数越少，缓冲消胀机制的反应时间就越快，但是当吞吐量突然飙升或者下降时，缓冲消胀机制计算的最佳缓冲数据量会更容易出错。

* `taskmanager.network.memory.buffer-debloat.threshold-percentages`：防止缓冲区大小频繁改变的优化（比如，新的大小跟旧的大小相差不大）。

更多详细和额外的参数配置，请参考[配置参数]({{< ref "docs/deployment/config" >}}#full-taskmanageroptions)。

您可以使用以下[指标]({{< ref "docs/ops/metrics" >}}#io)来监控当前的缓冲区大小：
* `estimatedTimeToConsumeBuffersMs`：消费所有输入通道（input channel）中数据的总时间。
* `debloatedBufferSize`：当前的缓冲区大小。

### 限制

当前，有一些场景还没有自动地被缓冲消胀机制处理。

#### 大消息

如果您的消息超过了[最小内存段 (memory segment) 长度]({{< ref "docs/deployment/config" >}}#taskmanager-memory-min-segment-size)，缓冲消胀可能会极大减少单个缓冲区大小，从而导致网络栈需要更多的缓冲区去传输一条消息。在实际上没有减少缓冲数据量的情况下，这可能对吞吐产生不利影响。

#### 多个输入和合并

当前，吞吐计算和缓冲消胀发生在 subtask 层面。

如果您的 subtask 有很多不同的输入或者有一个合并的输入，缓冲消胀可能会导致低吞吐的输入有太多缓冲数据，而高吞吐输入的缓冲区数量可能太少而不够维持当前吞吐。当不同的输入吞吐差别比较大时，这种现象会更加的明显。我们推荐您在测试这个功能时重点关注这种 subtask。

#### 缓冲区的尺寸和个数

当前，缓冲消胀仅在使用的缓冲区大小上设置上限。实际的缓冲区大小和个数保持不变。这意味着缓冲消胀机制不会减少作业的内存使用。您应该手动减少缓冲区的大小或者个数。

此外，如果您想减少缓冲数据量使其低于缓冲消胀当前允许的量，您可能需要手动的设置缓冲区的个数。

## 网络缓冲生命周期
 
Flink 有多个本地缓冲区池 —— 每个输出和输入流对应一个。
每个缓冲区池的大小被限制为

`#channels * taskmanager.network.memory.buffers-per-channel + taskmanager.network.memory.floating-buffers-per-gate`

缓冲区的大小可以通过 `taskmanager.memory.segment-size` 来设置。

### 输入网络缓冲

输入通道中的缓冲区被分为独占缓冲区（exclusive buffer）和流动缓冲区（floating buffer）。每个独占缓冲区只能被一个特定的通道使用。
一个通道可以从输入流的共享缓冲区池中申请额外的流动缓冲区。剩余的流动缓冲区是可选的并且只有资源足够的时候才能获取。

在初始阶段：
- Flink 会为每一个输入通道获取配置数量的独占缓冲区。
- 所有的独占缓冲区都必须被满足，否则作业会抛异常失败。
- Flink 至少要有一个流动缓冲区才能运行。

### 输出网络缓冲

不像输入缓冲区池，输出缓冲区池只有一种类型的缓冲区被所有的 subpartitions 共享。

为了避免过多的数据倾斜，每个 subpartition 的缓冲区数量可以通过 `taskmanager.network.memory.max-buffers-per-channel` 来限制。

不同于输入缓冲区池，这里配置的独占缓冲区和流动缓冲区只被当作推荐值。如果没有足够的缓冲区，每个输出 subpartition 可以只使用一个独占缓冲区而没有流动缓冲区。

## 缓冲区的数量

独占缓冲区和流动缓冲区的默认配置应该足以应对最大吞吐。如果想要最小化缓冲数据量，那么可以将独占缓冲区设置为 `0`，同时减小内存段的大小。

### 选择缓冲区的大小

在往下游 subtask 发送数据部分时，缓冲区通过汇集 record 来优化网络开销。下游 subtask 应该在接收到完整的 record 后才开始处理它。

If the buffer size is too small, or the buffers are flushed too frequently (`execution.buffer-timeout` configuration parameter), this can lead to decreased throughput
since the per-buffer overhead are significantly higher then per-record overheads in the Flink's runtime.

As a rule of thumb, we don't recommend thinking about increasing the buffer size, or the buffer timeout unless you can observe a network bottleneck in your real life workload
(downstream operator idling, upstream backpressured, output buffer queue is full, downstream input queue is empty).

如果缓冲区太大，会导致：
- 内存使用高
- 大量的 checkpoint 数据量（针对非对齐的 checkpoints）
- 漫长的 checkpoint 时间（针对对齐的 checkpoints）
- `execution.buffer-timeout` 较小时内存分配使用率会比较低，因为缓冲区还没被塞满数据就被发送下去了。

### 选择缓冲区的数量

缓冲区的数量是通过 `taskmanager.network.memory.buffers-per-channel` 和 `taskmanager.network.memory.floating-buffers-per-gate` 来配置的。

为了最好的吞吐率，我们建议使用独占缓冲区和流动缓冲区的默认值。如果缓冲数据量存在问题，更建议打开[缓冲消胀]({{< ref "docs/deployment/memory/network_mem_tuning" >}}#the-buffer-debloating-mechanism)。

您可以人工地调整网络缓冲区的个数，但是需要注意：

1. 您应该根据期待的吞吐量（单位 `bytes/second`）来调整缓冲区的数量。协调数据传输量（大约两个节点之间的两个往返消息）。延迟也取决于您的网络。

使用 buffer 往返时间（大概 `1ms` 在正常的本地网络中），[缓冲区大小]({{< ref "docs/deployment/config" >}}#taskmanager-memory-segment-size)和期待的吞吐，您可以通过下面的公式计算维持吞吐所需要的缓冲区数量：
```
number_of_buffers = expected_throughput * buffer_roundtrip / buffer_size
```
比如，期待吞吐为 `320MB/s`，往返延迟为 `1ms`，内存段为默认大小，为了维持吞吐需要使用10个活跃的缓冲区：
```
number_of_buffers = 320MB/s * 1ms / 32KB = 10
```
2. 流动缓冲区的目的是为了处理数据倾斜。理想情况下，流动缓冲区的数量（默认8个）和每个通道独占缓冲区的数量（默认2个）能够使网络吞吐量饱和。但这并不总是可行和必要的。所有 subtask 中只有一个通道被使用也是非常罕见的。

3. 独占缓冲区的目的是提供一个流畅的吞吐量。当一个缓冲区在传输数据时，另一个缓冲区被填充。当吞吐量比较高时，独占缓冲区的数量是决定 Flink 中缓冲数据的主要因素。

当低吞吐量下出现反压时，您应该考虑减少[独占缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel)。

## 总结

可以通过开启缓冲消胀机制来简化 Flink 网络的内存配置调整。您也可能需要调整它。

如果这不起作用，您可以关闭缓冲消胀机制并且人工地配置内存段的大小和缓冲区个数。针对第二种场景，我们推荐：
- 使用默认值以获得最大吞吐
- 减少内存段大小、独占缓冲区的数量来加快 checkpoint 并减少网络栈消耗的内存量

{{< top >}}
