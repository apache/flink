---
title: 自适应批处理
weight: 5
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

# 自适应批处理
本文档描述了自适应批处理的背景，使用方法，以及限制条件。

## 背景

传统的 Flink 批作业执行流程，作业的执行计划是在提交之前确定的。如果想要对执行计划进行调优，就需要用户和 Flink 静态执行计划优化器在运行作业前，了解作业逻辑并能够准确判断作业运行过程，包括各个节点所处理的数据特性和连接边的数据分发方式。

然而在现实情况中，这些数据特性在作业运行前是无法被预判的。虽然在输入数据有丰富的统计信息的前提下，用户和 Flink 静态执行计划优化器可以将这些统计信息，与执行计划中的各个算子特性结合起来，进行一些适度的推理优化。
然而实际生产中，输入数据的统计信息往往不全或者不准确，而且 Flink 作业的中间节点的输入难以估算。依据静态的信息来优化的作业执行计划并不能很好的在这些场景下进行作业执行计划的优化。

为了解决这个问题，Flink 引入了 **AdaptiveBatchScheduler** 调度器，该调度器是一种可以**自动调整执行计划**的批作业调度器。
它会随着作业运行逐步确定作业执行计划，并根据确定下来的执行计划来增量式生成 JobGraph。未确定下来的执行计划将允许 Flink 根据具体的优化策略和中间运行结果的特点，来进行运行时的执行计划动态调整。
目前，该调度器支持的优化策略有：
- [自动推导算子并行度](#自动推导并行度)
- [自动均衡数据分发](#自动均衡数据分发)
- [自适应 Broadcast Join](#自适应-broadcast-join)
- [自适应 Skewed Join Optimization](#自适应-skewed-join-优化)

## 自动推导并行度

Adaptive Batch Scheduler 支持自动推导算子并行度，如果算子未设置并行度，调度器将根据其消费的数据量的大小来推导其并行度。这可以带来诸多好处：
- 批作业用户可以从并行度调优中解脱出来
- 根据数据量自动推导并行度可以更好地适应每天变化的数据量
- SQL作业中的算子也可以分配不同的并行度

### 用法

使用 Adaptive Batch Scheduler 自动推导算子的并行度，需要：
- 启用自动并行度推导：

  Adaptive Batch Scheduler 默认启用了自动并行度推导，你可以通过配置 [`execution.batch.adaptive.auto-parallelism.enabled`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-enabled) 来开关此功能。
  除此之外，你也可以根据作业的情况调整以下配置:
    - [`execution.batch.adaptive.auto-parallelism.min-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-min-parallelism): 允许自动设置的并行度最小值。
    - [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism): 允许自动设置的并行度最大值，如果该配置项没有配置将使用通过 [`parallelism.default`]({{< ref "docs/deployment/config" >}}) 或者 `StreamExecutionEnvironment#setParallelism()` 设置的默认并行度作为允许自动设置的并行度最大值。
    - [`execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-avg-data-volume-per-ta): 期望每个任务平均处理的数据量大小。请注意，当出现数据倾斜，或者确定的并行度达到最大并行度（由于数据过多）时，一些任务实际处理的数据可能会远远超过这个值。
    - [`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle): source 算子可动态推导的最大并行度，若该配置项没有配置将优先使用 [`execution-batch-adaptive-auto-parallelism-max-parallelism`]({{< ref "docs/deployment/config" >}})作为允许动态推导的并行度最大值，若该配置项也没有配置，将使用 [`parallelism.default`]({{< ref "docs/deployment/config" >}}) 或者 `StreamExecutionEnvironment#setParallelism()` 设置的默认并行度。
- 不要指定算子的并行度：

  Adaptive Batch Scheduler 只会为用户未指定并行度的算子推导并行度。 所以如果你想算子的并行度被自动推导，需要避免通过算子的 `setParallelism()` 方法来为其指定并行度。

  除此之外，对于 DataSet 作业还需要进行以下配置：
    - 配置 `parallelism.default: -1`
    - 不要通过 `ExecutionEnvironment` 的 `setParallelism()` 方法来指定并行度

### 让 Source 支持动态并行度推导
如果你的作业有用到自定义 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="Source" >}},
你需要让 Source 实现接口 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/DynamicParallelismInference.java" name="DynamicParallelismInference" >}}。
```java
public interface DynamicParallelismInference {
    int inferParallelism(Context context);
}
```
其中 Context 会提供可推导并行度上界、期望每个任务平均处理的数据量大小、动态过滤信息来协助并行度推导。
Adaptive Batch Scheduler 将会在调度 Source 节点之前调用上述接口，需注意实现中应尽量避免高耗时的操作。

若 Source 未实现上述接口，[`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle) 将会作为 Source 节点的并行度。

需注意，Source 动态并行度推导也只会为用户未指定并行度的 Source 算子推导并行度。

### 性能调优

1. 建议使用 [Sort Shuffle](https://flink.apache.org/2021/10/26/sort-shuffle-part1.html) 并且设置 [`taskmanager.network.memory.buffers-per-channel`]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) 为 `0`。 这会解耦并行度与需要的网络内存，对于大规模作业，这样可以降低遇到 "Insufficient number of network buffers" 错误的可能性。
2. 建议将 [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism) 设置为最坏情况下预期需要的并行度。不建议配置太大的值，否则可能会影响性能。这个配置项会影响上游任务产出的 subpartition 的数量，过多的 subpartition 可能会影响 hash shuffle 的性能，或者由于小包影响网络传输的性能。

## 自动均衡数据分发

Adaptive Batch Scheduler 支持自动均衡数据分发。调度器会尝试将数据均匀分配给下游子任务，确保各个下游子任务消耗的数据量大致相同。
该优化无需用户手动配置，点对点连接类型（如 Rescale）和全联接连接类型（如 Hash、Rebalance、Custom）均适用。

### 局限性

- 目前仅支持对[自动推导算子并行度](#自动推导并行度)的节点进行自动均衡数据分发。因此，用户需要开启[自动推导算子并行度](#自动推导并行度)，并避免手动设置节点的并行度，才能享受到自动均衡数据分发的优化。
- 目前自动均衡数据分发无法完全解决单 key 数据热点问题。当单个 key 的数据远远多于其他 key 的数据时，仍然会有热点。然而为了数据的正确性，我们并不能拆分这个 key 的数据，将其分配给不同的子任务处理。不过，在一些特定的情况下，单 key 问题是可以被解决的，见 [自适应 Skewed Join Optimization](#自适应-skewed-join-优化)。

## 自适应 Broadcast Join

在分布式数据处理中，broadcast join 是一个比较常见的优化，其实现原理是在 join 的两个表中，如果有一个超小的表（可以放到单个计算节点的内存中），那对于这个超小表可以不做 shuffle，而是直接将其全量数据 broadcast 到每个处理大表的分布式计算节点上，直接在内存中完成 join 操作。broadcast join 优化能大量减少大表 shuffle 和排序，非常明显的提升作业运行性能。然而静态优化方法对此优化的判断生效条件往往不准，且在生产中应用有限，原因是：
- 源表的统计信息**完整性与准确性**在生产中往往不足 
- 对于**非源表的输入**无法实现很好的判断，因为中间数据的大小往往需要在作业运行过程中才能准确得知。当 join 操作离源表数据比较远时，这种预判基本是不可用的。 
- 如果静态优化方法错误的判断了生效条件，则可能造成比较严重的后果。这是因为错误的将大表判定为小表实际不小并无法放进单节点内存，那么 broadcast join 算子在试图建立内存中的 hash 表时就会因为 **Out of Memory** 而导致任务失败，从而**需要任务重跑**。
   
因此，虽然静态 broadcast join 在正确使用时可以带来较大的性能提升，但实际上优应用有限。而自适应 Broadcast Join 则可以让 Flink 在运行时根据实际的数据输入来自适应的将 Join 算子转为 Broadcast Join。

**为保证 Join 的正确性语义**，自适应 Broadcast Join 会根据 Join 类型来决策输入边是否能够被广播，可广播的情况如下：

| **Join 类型**                                   | **Left 输入**      | **Right 输入**           |
|:----------------------------------------------|:-----------------|:-----------------------|
| Inner                                         | ✅                | ✅                      |
| LeftOuter                                     | ❌                | ✅                      |
| RightOuter                                    | ✅                | ❌                      |
| FullOuter                                     | ❌                | ❌                      |
| Semi                                          | ❌                | ✅                      |
| Anti                                          | ❌                | ✅                      |

### 用法

Adaptive Batch Scheduler 默认**同时启用**编译时静态自适应 Broadcast Join 和运行时动态自适应 Broadcast Join，你可以通过配置 [`table.optimizer.adaptive-broadcast-join.strategy`]({{< ref "docs/dev/table/config" >}}#table-optimizer-adaptive-broadcast-join-strategy) 来控制 Broadcast Join 的时机，比如是否仅启用运行时自适应 Broadcast Join。
除此之外，你也可以根据作业的情况调整以下配置:
- [`table.optimizer.join.broadcast-threshold`]({{< ref "docs/dev/table/config" >}}#table-optimizer-join-broadcast-threshold)：允许被广播的数据量阈值。当 TaskManager 内存较大时，可以适当提高该配置值，反之则可以适当降低该配置值。

### 局限性

- 目前 Adaptive Broadcast Join 不支持对包含在 MultiInput 算子内部的 Join 算子进行优化
- 目前 Adaptive Broadcast Join 还不支持和 [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) 同时启用，因此在启用 [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) 后，Adaptive Broadcast Join 将不会生效。

## 自适应 Skewed Join 优化

在 Join 查询中，当某些键频繁出现时，可能会导致下游每个 Join task 处理的数据量会有很大差异。从而会导致单个处理任务的性能严重降低整个作业的性能。
然而由于 Join 操作符的两个输入边存在关联性，相同的 keyGroup 必须分配到同一个下游子任务，因此仅依靠自动均衡数据分发来解决 Join 类型的数据倾斜问题是不够的。
因此，自适应 Skewed Join 优化将允许 Join 操作符基于运行时的输入统计信息**动态拆分倾斜且可拆分的数据分区**，从而减轻由倾斜数据引起的长尾问题。

**为保证 Join 的正确性语义**，自适应 Skewed Join 优化会根据 Join 类型来决策输入边是否能够被动态拆分，可拆分的情况如下：

| **Join 类型**                                    | **Left 输入**      | **Right 输入**           |
|:-----------------------------------------------|:-----------------|:-----------------------|
| Inner                                          | ✅                | ✅                      |
| LeftOuter                                      | ✅                | ❌                      |
| RightOuter                                     | ❌                | ✅                      |
| FullOuter                                      | ❌                | ❌                      |
| Semi                                           | ✅                | ❌                      |
| Anti                                           | ✅                | ❌                      |

### 用法

Adaptive Batch Scheduler  默认启用 Skewed Join 优化，你可以通过配置 [`table.optimizer.skewed-join-optimization.strategy`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-strategy) 来控制 Skewed Join 的优化策略。该配置项可选值为：
- **none**：禁用 Skewed Join 优化
- **auto**：允许 Skewed Join 优化，但是在一些情况下 Skewed Join 优化需要引入额外的 Shuffle 来保证数据正确性，在这种情况下 Skewed Join 优化将不会生效以规避引入额外 Shuffle 导致的开销。
- **forced**：允许 Skewed Join 优化，并且在引入额外 Shuffle 的场景下 Skewed Join 优化也会生效。

此外，你也可以根据作业的特性调整如下配置：
- [`table.optimizer.skewed-join-optimization.skewed-threshold`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-skewed-threshold)：触发数据倾斜 Join 优化的最小数据量。在 Join 阶段，当子任务处理的最大数据量超过该偏斜阈值时，Flink 能够自动将子任务处理的最大数据量与中位数数据量的比率降低到小于偏斜因子（skewed-factor）。
- [`table.optimizer.skewed-join-optimization.skewed-factor`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-skewed-factor)：倾斜因子。在 Join 阶段，Flink 将自动将最大子任务处理数据量与中位数数据量的比率降低到低于该倾斜因子来实现更均衡的数据分布。

### 局限性

- 由于 Adaptive Skewed Join 优化会影响 Join 节点的并行度，所以目前 Adaptive Skewed Join 优化需要启用[自动推导算子并行度](#自动推导并行度)才能生效
- 目前 Adaptive Skewed Join 优化不支持对包含在 MultiInput 算子内部的 Join 算子进行优化
- 目前 Adaptive Skewed Join 优化还不支持和 [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) 同时启用，因此在启用 [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) 后，Adaptive Skewed Join 优化将不会生效。

## 自适应批处理局限性

- **只支持 AdaptiveBatchScheduler**: 不过由于 Adaptive Batch Scheduler 是 Flink 默认的批作业调度器，无需额外配置。除非用户显式的配置了使用其他调度器，例如 [`jobmanager.scheduler: default`]。
- **只支持所有数据交换都为 BLOCKING 或 HYBRID 模式的作业**: 目前 Adaptive Batch Scheduler 只支持 [shuffle mode]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) 为 ALL_EXCHANGES_BLOCKING 或 ALL_EXCHANGES_HYBRID_FULL 或 ALL_EXCHANGES_HYBRID_SELECTIVE 的作业。请注意，使用 DataSet API 的作业无法识别上述 shuffle 模式，需要将 ExecutionMode 设置为 BATCH_FORCED 才能强制启用 BLOCKING shuffle。
- **不支持 FileInputFormat 类型的 source**: 不支持 FileInputFormat 类型的 source, 包括 `StreamExecutionEnvironment#readFile(...)` 和 `StreamExecutionEnvironment#createInput(FileInputFormat, ...)`。 当使用 Adaptive Batch Scheduler 时，用户应该使用新版的 Source API ([FileSystem DataStream Connector]({{< ref "docs/connectors/datastream/filesystem.md" >}}) 或 [FileSystem SQL Connector]({{< ref "docs/connectors/table/filesystem.md" >}})) 来读取文件.
- **Web UI 上展示的上游输出的数据量和下游收到的数据量可能不一致**: 在使用 Adaptive Batch Scheduler 自动推导并行度时，对于 broadcast 边，上游发送的数据量是基于下游最大并行度估算的结果，与下游算子实际接收的数据量可能会不相等，这在 Web UI 的显示上可能会困扰用户。细节详见 [FLIP-187](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Job+Scheduler)。

{{< top >}}
