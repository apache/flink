---
title: "存算分离"
weight: 20
type: docs
aliases:
  - /ops/state/disaggregated_state.html
  - /apis/streaming/disaggregated_state.html
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

# 存算分离（Disaggregated State Management）

<a name="overview"></a>

## 概览

在 Flink 的前 10 年，状态管理是基于 TaskManager 的内存或本地磁盘。该方法适用于大多数用户场景，但存在一些限制：
 * **本地磁盘限制**：状态大小受限于 TaskManager 的内存或者磁盘大小。
 * **资源使用尖刺**：本地状态模型会在 checkpoint 或 SST 文件 compaction 期间周期性地引起 CPU 和网络 I/O 的尖刺。
 * **恢复很慢**：在恢复期间，state backend 需要下载状态。恢复时间与状态大小成比例，大状态的恢复非常慢。

在 Flink 2.0，我们引入了存算分离（disaggregated state management）。存算分离允许用户将状态存储在外部存储系统中，如 S3、HDFS 等。
这在状态非常大时非常有用。存算分离能以更经济的方式存储状态，并且能够更轻量地持久化或恢复状态。它有如下好处：
 * **状态大小无限制**：状态大小仅取决于外部存储系统大小。
 * **资源使用稳定**：状态被存储在外部存储上，因此 checkpoint 可以非常轻量。SST 文件的 compaction 也可以被放在远程（TODO）。
 * **恢复快速**：在恢复期间无需下载状态到本地，恢复时间与状态大小无关。
 * **灵活**：用户可以轻松地选择不同的外部存储系统或 I/O 性能级别，或者根据需求扩展存储资源，而不需要改变他们的硬件。
 * **经济实惠**：外部存储通常比本地磁盘便宜。如果有瓶颈，用户可以灵活、独立地调整计算资源以及存储资源。

存算分离包括三个部分：
 * **ForSt State Backend**：一个将状态存储在外部存储系统中的 state backend，它也可以利用本地磁盘进行缓存和缓冲。
它使用异步 I/O 模型来读取和写入状态。详细信息请参阅 [ForSt State Backend]({{< ref "docs/ops/state/state_backends#the-forststatebackend" >}})。
 * **新的状态 API**：引入了新的状态 API（State V2）用于异步状态读取和写入，异步状态访问是存算分离克服高网络延迟的必要组件。
详细信息请参阅 [New State APIs]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}})。
 * **SQL 支持**：许多 SQL 算子已被重写以支持存算分离和异步状态访问，用户可以轻松地通过配置项启用这些算子。

{{< hint info >}}
对于大状态作业，我们推荐使用存算分离和异步状态访问。如果作业状态很小，本地状态管理和同步状态访问会是更好的选择。
{{< /hint >}}

{{< hint warning >}}
存算分离仍然是实验性功能。我们正在改进此功能的性能和稳定性。相关的 API 和配置可能会在将来的版本中发生变化。
{{< /hint >}}

<a name="quick-start"></a>

## 快速开始

<a name="for-sql-jobs"></a>

### SQL 作业

在 SQL 作业中可以通过以下配置启用存算分离：
```yaml
state.backend.type: forst
table.exec.async-state.enabled: true

# enable checkpoints, checkpoint directory is required
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://your-bucket/flink-checkpoints

# We don't support the mini-batch and two-phase aggregation in asynchronous state access yet.
table.exec.mini-batch.enabled: false
table.optimizer.agg-phase-strategy: ONE_PHASE
```
如此一来，你可以在 SQL 作业中使用存算分离和异步状态访问。目前我们并未实现所有异步状态访问版本的 SQL 算子。
对于还未支持异步状态访问的算子，算子会自动回退到同步状态实现，这种情况下性能可能不是最佳的。
目前支持异步状态访问的有状态算子有：
 - Rank (Top1, Append TopN)
 - Row Time Deduplicate
 - Aggregate (without distinct)
 - Join
 - Window Join
 - Tumble / Hop / Cumulative Window Aggregate


<a name="for-datastream-jobs"></a>

### DataStream 作业

在 DataStream 作业中启用存算分离，首先需要使用 `ForStStateBackend`。在 per-job 模式下可以通过代码配置：
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "forst");
config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://your-bucket/flink-checkpoints");
config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
env.configure(config);
```
或者通过 `config.yaml` 进行配置：
```yaml
state.backend.type: forst

# enable checkpoints, checkpoint directory is required
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://your-bucket/flink-checkpoints
```

然后，你需要使用新的状态 API 来编写 DataStream 作业。详细信息请参阅 [State V2]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}})。

<a name="advanced-tuning-options"></a>

## 高级调优选项

<a name="tuning-forst-state-backend"></a>

### ForSt State Backend 调优

`ForStStateBackend` 有许多配置项用于性能调优。
ForSt 的设计和 RocksDB 非常类似，可配置的选项也几乎相同，
因此你可以参考[大状态调优]({{< ref "docs/ops/state/large_state_tuning#tuning-rocksdb-or-forst" >}})来为 ForSt State Backend 调优。

除此之外，以下章节介绍了一些 ForSt 独有的配置项。

<a name="forst-primary-storage-location"></a>

#### ForSt 主存储位置

默认情况下，ForSt 会将状态存储在 checkpoint 目录中。在这种情况下，ForSt 可以执行轻量级的 checkpoint 和快速恢复。
但用户也可能想将状态存储在一个不同的位置，例如 S3 中一个不同的桶。你可以通过以下配置来指定主存储位置：
```yaml
state.backend.forst.primary-dir: s3://your-bucket/forst-state
```

**注意**：如果设置了此配置，你可能无法利用轻量级的 checkpoint 和快速恢复，因为 ForSt 会在 checkpoint 和恢复过程中，
在主存储位置和 checkpoint 目录之间复制文件。

<a name="forst-local-storage-location"></a>

#### ForSt 本地存储位置

默认情况下，ForSt **仅**在使用异步 API（State V2）时才会进行存算分离。在 DataStream 和 SQL 作业中使用同步状态 API 时，
ForSt 仅作为**本地状态存储**。由于一个作业可能包含混合使用不同 API 的多个 ForSt 实例，同步的本地状态访问与异步的远程状态访问相结合，
有助于实现更好的整体吞吐量。如果你希望使用同步状态 API 的算子也将状态存储在远端，可以使用以下配置：
```yaml
state.backend.forst.sync.enforce-local: false
```
你还可以通过以下配置项来指定本地存储位置：
```yaml
state.backend.forst.local-dir: path-to-local-dir
```

<a name="forst-file-cache"></a>

#### ForSt 文件缓存

ForSt 使用本地磁盘进行缓存和缓冲。缓存粒度是整个文件。默认启用缓存功能，除非将主存储位置设置为本地。
缓存有两种容量限制策略：
 - 基于固定大小（Size-based）：在已使用缓存大小超过限制时淘汰最旧的文件。
 - 基于预留空间（Reserved-based）：在磁盘（缓存目录所在的磁盘）上剩余空间不足时淘汰最旧的文件。
相关配置项如下：
```yaml
state.backend.forst.cache.size-based-limit: 1GB
state.backend.forst.cache.reserve-size: 10GB
```
以上两个配置项可以同时生效。如果同时开启两个选项，在缓存使用量触发以上两个限制之一时，缓存会淘汰最旧的文件。

你还可以通过以下配置项来指定缓存目录：
```yaml
state.backend.forst.cache.dir: /tmp/forst-cache
```

<a name="forst-asynchronous-threads"></a>

#### ForSt 异步线程

ForSt 使用异步 I/O 来读取和写入状态。有 3 种类型的线程：
 - 协调线程（Coordinator thread）：负责协调异步读取和写入。
 - 读线程（Read thread）：负责异步读取状态。
 - 写线程（Write thread）：负责异步写入状态。

异步线程的数量可以配置。通常，你不需要调整这些值，因为默认值已经适合大多数情况。
如果有特殊需要，你可以设置以下配置来指定异步线程的数量：
 - `state.backend.forst.executor.read-io-parallelism`：读线程的数量，默认值是 3。
 - `state.backend.forst.executor.write-io-parallelism`：写线程的数量，默认值是 1。
 - `state.backend.forst.executor.inline-write`：写操作是否在协调线程内执行。
默认值为 true。设置为 false 会提高 CPU 使用率。
 - `state.backend.forst.executor.inline-coordinator`：是否让任务线程作为协调线程。
默认值为 true。设置为 false 会提高 CPU 使用率。

{{< hint info >}}
`ForStStateBackend` 以 [ForSt](https://github.com/ververica/ForSt/) 作为其底层数据库核心。
当前版本的 ForSt 是从 [frocksdb](https://github.com/ververica/frocksdb) fork 而来的，
其架构是专为 Flink 本地状态管理设计的嵌入式数据库核心。

在将数据库向存算分离架构演进的过程中，我们在现有框架内遇到了重大的架构和工程限制。
为了解决这些挑战，社区成员正在开发使用 Rust 编写的下一代云原生 ForSt DB。

<a name="key-advantages-of-the-new-forst-db"></a>

#### 新 ForSt DB 的关键优势：
- 架构简洁：为高扩展性而设计的精简代码库。
- 流原生设计：专门针对大规模流处理的独特需求进行优化。
- 云原生：从头开始设计以支持存算分离。

<a name="roadmap--maintenance"></a>

#### 路线图与维护：
- 发布计划：第一个稳定的开源版本预计于 2026 年晚些时候发布（乐观估计为 8 月）。
- 弃用声明：随着我们将重心转移到新的基于 Rust 的实现，基于 frocksdb 的 ForSt 版本已不再活跃开发，并将在新版本发布后逐步淘汰（弃用）。

{{< /hint >}}

{{< top >}}
