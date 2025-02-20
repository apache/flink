---
title: Adaptive Batch
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

# Adaptive Batch Execution
This document describes the background, usage, and limitations of adaptive batch execution.

## Background

In the traditional Flink batch job execution process, the execution plan of a job is determined before submission. To optimize the execution plan, users and Flink's static execution plan optimizer need to understand the job logic and accurately evaluate how the job will execute, including the data characteristics processed by each node and the data distribution of the connecting edges.

However, in real-world scenarios, these data characteristics cannot be predicted before the job is executed. 
Although, if there is rich statistical information about the input data, users and Flink's static execution plan optimizer can combine these statistics with the characteristics of each operator in the execution plan to conduct some moderate inferential optimization. But in actual production environments, the statistical information on input data is often incomplete or inaccurate, making it difficult to estimate the intermediate data in a Flink job.

To address this issue, Flink introduced the **AdaptiveBatchScheduler**, a batch job scheduler that can **automatically adjust execution plan**. 
This scheduler gradually determines the job execution plan as the job executes and incrementally generates the JobGraph based on the determined execution plan. For parts of undecided execution plans, Flink is allowed to dynamically adjust execution plans at runtime based on specific optimization strategies and the characteristics of intermediate data. 
Currently, the optimization strategies supported by the scheduler include:
- [Automatically decide parallelisms for operators](#automatically-decide-parallelisms-for-operators)
- [Automatic load balancing of data distribution](#automatic-balancing-of-data-distribution)
- [Adaptive Broadcast Join](#adaptive-broadcast-join)
- [Adaptive Skewed Join Optimization](#adaptive-skewed-join-optimization)

## Automatically decide parallelisms for operators

The Adaptive Batch Scheduler supports automatically deciding parallelisms of operators for batch jobs. If an operator is not set with a parallelism, 
the scheduler will decide parallelism for it according to the size of its consumed datasets. This can bring many benefits:
- Batch job users can be relieved from parallelism tuning
- Automatically tuned parallelisms can better fit consumed datasets which have a varying volume size every day
- Operators from SQL batch jobs can be assigned with different parallelisms which are automatically tuned

### Usage

To automatically decide parallelisms for operators with Adaptive Batch Scheduler, you need to:
- Toggle the feature on:

  Adaptive Batch Scheduler enables automatic parallelism derivation by default. You can configure [`execution.batch.adaptive.auto-parallelism.enabled`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-enabled) to toggle this feature.
  In addition, there are several related configuration options that may need adjustment when using Adaptive Batch Scheduler to automatically decide parallelisms for operators:
    - [`execution.batch.adaptive.auto-parallelism.min-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-min-parallelism): The lower bound of allowed parallelism to set adaptively.
    - [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism): The upper bound of allowed parallelism to set adaptively. The default parallelism set via [`parallelism.default`]({{< ref "docs/deployment/config" >}}) or `StreamExecutionEnvironment#setParallelism()` will be used as upper bound of allowed parallelism if this configuration is not configured.
    - [`execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-avg-data-volume-per-ta): The average size of data volume to expect each task instance to process. Note that when data skew occurs, or the decided parallelism reaches the max parallelism (due to too much data), the data actually processed by some tasks may far exceed this value.
    - [`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle): The default parallelism of data source or the upper bound of source parallelism to set adaptively. The upper bound of allowed parallelism set via [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism) will be used if this configuration is not configured. If the upper bound of allowed parallelism is also not configured, the default parallelism set via [`parallelism.default`]({{< ref "docs/deployment/config" >}}) or `StreamExecutionEnvironment#setParallelism()` will be used instead.

- Avoid setting the parallelism of operators:

  The Adaptive Batch Scheduler only decides the parallelism for operators which do not have a parallelism set. So if you want the parallelism of an operator to be automatically decided, you need to avoid setting the parallelism for the operator through the 'setParallelism()' method.

#### Enable dynamic parallelism inference support for Sources
New {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="Source" >}}
can implement the interface {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/DynamicParallelismInference.java" name="DynamicParallelismInference" >}} to enable dynamic parallelism inference.
```java
public interface DynamicParallelismInference {
    int inferParallelism(Context context);
}
```
The Context will provide the upper bound for the inferred parallelism, the expected average data size to be processed by each task, and dynamic filtering information to assist with parallelism inference.

The Adaptive Batch Scheduler will invoke the interface before scheduling the source vertices, and it should be noted that implementations should avoid time-consuming operations as much as possible.

If the Source does not implement the interface, the configuration setting [`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle) will be used as the parallelism of the source vertices.

Note that the dynamic source parallelism inference only decides the parallelism for source operators which do not already have a specified parallelism.

#### Performance tuning

1. It's recommended to use [Sort Shuffle](https://flink.apache.org/2021/10/26/sort-shuffle-part1.html) and set [`taskmanager.network.memory.buffers-per-channel`]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) to `0`. This can decouple the required network memory from parallelism, so that for large scale jobs, the "Insufficient number of network buffers" errors are less likely to happen.
2. It's recommended to set [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism) to the parallelism you expect to need in the worst case. Values larger than that are not recommended, because excessive value may affect the performance. This option can affect the number of subpartitions produced by upstream tasks, large number of subpartitions may degrade the performance of hash shuffle and the performance of network transmission due to small packets.

## Automatic Balancing of Data Distribution

The Adaptive Batch Scheduler supports automatic balancing of data distribution. The scheduler will attempt to evenly distribute data to downstream subtasks, ensuring that the amount of data consumed by each downstream subtask is roughly the same. 
This optimization requires no manual configuration by the user and is applicable to various connect edges, including point-wise connections (e.g., Rescale) and all-to-all connections (e.g., Hash, Rebalance, Custom).

### Limitations

- Currently, automatic balancing of data distribution only supports operators with [Automatically decide parallelisms for operators](#automatically-decide-parallelisms-for-operators). Therefore, users need to enable [Automatically decide parallelisms for operators](#automatically-decide-parallelisms-for-operators) and avoid manually setting the parallelism of operators in order to benefit from the optimization of automatic data distribution balancing.
- This optimization does not fully address scenarios involving single-key hotspots. When the data for a single key far exceeds the data for other keys, hotspots may still occur. However, for the correctness of the data, we cannot split the data for this key and assign it to different subtasks for processing. However, in certain specific situations, the single key issue can be addressed, as seen in the [Adaptive Skewed Join Optimization](#adaptive-skewed-join-optimization).

## Adaptive Broadcast Join

In distributed data processing, broadcast join is a common optimization. A broadcast join works on the principle that if one of the two tables is very small—small enough to fit in the memory of a single compute node—then its entire dataset can be broadcast to each distributed compute node. 
This join operation can be performed directly in memory. The broadcast join optimization can significantly reduce shuffling and sorting of the large table. 
However, static optimizers often misjudge the conditions that determine when this optimization is effective, which limits their application in production for the following reasons:
- The **completeness and accuracy** of statistical information about the source tables is often inadequate in production environments.
- It is difficult to make accurate judgments regarding inputs that are **not from the source tables**. This is because the size of intermediate data cannot be accurately determined until the job is running. When the join operation is far from the source table data, such early evaluation is usually not feasible.
- If static optimization incorrectly assesses the effective conditions, it can lead to serious problems. For instance, if a large table is mistakenly classified as small and cannot fit in a single node's memory, the broadcast join operator may fail due to an **Out of Memory** error when trying to create a hash table in memory, resulting in the need to **restart the task**.

Therefore, while broadcast joins can provide significant performance improvements when used correctly, their practical applications are limited. In contrast, adaptive Broadcast Join allows Flink to dynamically convert the Join operator to a Broadcast Join at runtime based on the actual data input. 

**To ensure the correctness of the join semantics**, adaptive Broadcast Join will decide whether the input edge can be broadcast based on the type of join. The conditions under which broadcasting is possible are as follows:

| **Join Type** | **Left Input** | **Right Input** |
|:--------------|:---------------|:----------------|
| Inner         | ✅              | ✅               |
| LeftOuter     | ❌              | ✅               |
| RightOuter    | ✅              | ❌               |
| FullOuter     | ❌              | ❌               |
| Semi          | ❌              | ✅               |
| Anti          | ❌              | ✅               |

### Usage

The Adaptive Batch Scheduler defaults to **simultaneously enabling** both compile-time static Broadcast Join and runtime dynamic adaptive Broadcast Join. You can control the timing of the Broadcast Join by configuring [`table.optimizer.adaptive-broadcast-join.strategy`]({{< ref "docs/dev/table/config" >}}#table-optimizer-adaptive-broadcast-join-strategy). For example, you can set the value to be RUNTIME_ONLY so that the adaptive Broadcast Join is effective only at runtime. 
Additionally, you can adjust the following configuration based on the job's specific requirements:
- [`table.optimizer.join.broadcast-threshold`]({{< ref "docs/dev/table/config" >}}#table-optimizer-join-broadcast-threshold)：The threshold for the amount of data that can be broadcast. When the memory of the TaskManager is large, this value can be increased appropriately; conversely, it can be decreased if the memory is limited.

### Limitations

- Adaptive Broadcast Join does not support optimization of Join operators contained within MultiInput operators.
- Adaptive Broadcast Join does not support being enabled simultaneously with [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}). Therefore, after [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) enabled, Adaptive Broadcast Join will not take effect.

## Adaptive Skewed Join Optimization

In Join queries, when certain keys appear frequently, it may lead to significant variations in the amount of data processed by each Join task. 
This can lead to a significant reduction in the performance of individual processing tasks, thereby degrading the overall performance of the job. 
However, because the two input sides of the Join operator are related, the same keyGroup needs to be processed by the same downstream sub-task. 
Therefore, simply relying on automatic load balancing to solve data skew issues in Join operations is not enough. 
And Adaptive Skewed Join optimization allows the Join operator to **dynamically split skewed and splittable data partitions** based on runtime input statistics, thereby alleviating the tail latency problem caused by skewed data. 

**To ensure the correctness of the Join semantics**, the Adaptive Skewed Join optimization decides whether the input edges can be dynamically split based on the Join type. 
The scenarios where splitting is possible are as follows:

| **Join Type** | **Left Input** | **Right Input** |
|:--------------|:---------------|:----------------|
| Inner         | ✅              | ✅               |
| LeftOuter     | ✅              | ❌               |
| RightOuter    | ❌              | ✅               |
| FullOuter     | ❌              | ❌               |
| Semi          | ✅              | ❌               |
| Anti          | ✅              | ❌               |

### Usage

The Adaptive Batch Scheduler enables Skewed Join optimization by default. You can control the optimization strategy for Skewed Join by configuring [`table.optimizer.skewed-join-optimization.strategy`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-strategy). The possible values for this configuration are:
- **none**：Disable Skewed Join optimization.
- **auto**：Allows Skewed Join optimization; however, in some cases, the Skewed Join optimization may require additional Shuffle to ensure data correctness. In such cases, the Skewed Join optimization will not take effect to avoid incurring the overhead caused by the additional Shuffle.
- **forced**：Allow Skewed Join optimization, and it will take effect even in scenarios where additional Shuffle is introduced.

Additionally, you can adjust the following configurations based on the characteristics of your job:
- [`table.optimizer.skewed-join-optimization.skewed-threshold`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-skewed-threshold)：The minimum amount of data that triggers skewed Join optimization. During the Join stage, when the maximum amount of data processed by the subtasks exceeds this skew threshold, Flink can automatically reduce the ratio of the maximum data processed by the subtasks to the median data amount to be below the skew factor.
- [`table.optimizer.skewed-join-optimization.skewed-factor`]({{< ref "docs/dev/table/config" >}}#table-optimizer-skewed-join-optimization-skewed-factor)：Skew factor. During the Join stage, Flink will automatically reduce the ratio of the maximum data processed by subtasks to the median data amount to be below this skew factor to achieve a more balanced data distribution.

### Limitations

- Due to the Adaptive Skewed Join optimization can affect the parallelism of Join operators, it currently requires enabling [Automatically decide parallelisms for operators](#automatically-decide-parallelisms-for-operators) to take effect.
- Adaptive Skewed Join optimization does not support optimization of Join operators contained within MultiInput operators.
- Adaptive Skewed Join optimization does not support being enabled simultaneously with [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}). Therefore, after [Batch Job Recovery Progress]({{< ref "docs/ops/batch/recovery_from_job_master_failure" >}}) enabled，Adaptive Skewed Join optimization will not take effect.

## Limitations of Adaptive Batch Execution

- **AdaptiveBatchScheduler only**: It only takes effect when using the AdaptiveBatchScheduler. And since the Adaptive Batch Scheduler is the default batch job scheduler in Flink, no additional configuration is required unless the user explicitly configures to use another scheduler, such as [`jobmanager.scheduler: default`].
- **BLOCKING or HYBRID jobs only**: At the moment, Adaptive Batch Scheduler only supports jobs whose [shuffle mode]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) is `ALL_EXCHANGES_BLOCKING / ALL_EXCHANGES_HYBRID_FULL / ALL_EXCHANGES_HYBRID_SELECTIVE`.
- **FileInputFormat sources are not supported**: FileInputFormat sources are not supported, including `StreamExecutionEnvironment#readFile(...)` and `StreamExecutionEnvironment#createInput(FileInputFormat, ...)`. Users should use the new sources([FileSystem DataStream Connector]({{< ref "docs/connectors/datastream/filesystem.md" >}}) or [FileSystem SQL Connector]({{< ref "docs/connectors/table/filesystem.md" >}})) to read files when using the Adaptive Batch Scheduler.
- **Inconsistent broadcast results metrics on WebUI**: When use Adaptive Batch Scheduler to automatically decide parallelisms for operators, for broadcast results, the number of bytes/records sent by the upstream task counted by metric is not equal to the number of bytes/records received by the downstream task, which may confuse users when displayed on the Web UI. See [FLIP-187](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Job+Scheduler) for details.

{{< top >}}
