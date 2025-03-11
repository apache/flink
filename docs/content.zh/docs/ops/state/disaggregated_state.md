---
title: "Disaggregated State Management"
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

# 存算分离(Disaggregated State Management)

## 概览

在 Flink 的前 10 年，状态管理是基于 TaskManager 的内存或本地磁盘。该方法适用于大多数用户场景，但存在一些限制：
* **本地磁盘限制**: 状态大小受限于 TaskManager 的内存或者磁盘大小。
* **资源使用尖刺**：本地状态模型会在 checkpoint 时触发 SST 文件的 compaction，引起 CPU 和网络 I/O 的尖刺。
* **恢复繁重**：在恢复期间，需要下载状态。恢复时间与状态大小成比例，大状态的恢复非常慢。

在 Flink 2.0，我们引入了存算分离(disaggregated state management). 存算分离允许用户将状态直接存储在第三方存储系统，如 S3，HDFS 等，
这在状态非常大时很有用。存算分离以一种更有效的方式存储状态，轻量地持久化或恢复状态。存算分离有如下好处：
* **状态大小无限制**：状态大小仅取决于外部存储系统大小。 
* **资源使用稳定**：状态被存储在外部存储上，因此 checkpoint 可以非常轻量。SST 文件的 compaction 也可以被放在远程（TODO）。
* **恢复快速**：在恢复期间无需下载状态到本地，恢复时间与状态大小无关。
* **灵活**：用户可以轻松地选择不同的外部存储系统或I/O性能级别，或者根据需求独立地扩展存储资源，而不需要改变他们的硬件。
* **经济实惠**：外部存储通常比本地磁盘便宜。如果有瓶颈，用户可以灵活、独立地调整计算资源以及存储资源。

存算分离包括三个部分：
* **ForSt 状态后端**: 一个以外部存储系统作为主存的状态后端。它还可以利用本地磁盘进行缓存和缓冲。
异步I/O模型用于读取和写入状态。详细信息请参阅[ForSt State Backend]({{< ref "docs/ops/state/state_backends#the-forststatebackend" >}}).
* **新的状态 API**: 引入了新的状态 API (State V2) 用于异步状态读取和写入，异步状态访问是存算分离克服高网络延迟的必要组件。
详细信息请参阅[New State APIs]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}}).
* **SQL 支持**: 许多 SQL 算子都被重写以支持存算分离和异步状态访问。用户可以轻松地通过配置项启用这些功能。

{{< hint info >}}
对大状态作业，推荐使用存算分离和异步状态访问。如果状态很小，本地状态管理和同步状态访问是更好的选择。
{{< /hint >}}

{{< hint warning >}}
存算分离(disaggregated state management)仍然是实验性功能。我们正在改进此功能的性能和稳定性。新的状态 API 和配置可能会在将来的版本中更改。
{{< /hint >}}

## Quick Start

### For SQL Jobs

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
如此一来，您可以在 SQL 作业中使用存算分离和异步状态访问。目前还未全部实现所有 SQL 算子的异步状态访问。
对于还未支持异步状态访问的算子，算子会自动回退到同步状态实现，这种情况下性能可能不是最佳的。
目前支持异步状态访问的有状态算子有：
- Rank (Top1, Append TopN)
- Row Time Deduplicate
- Aggregate (without distinct)
- Join
- Window Join
- Tumble / Hop / Cumulative Window Aggregate

### For DataStream Jobs
在 DataStream 作业启用存算分离，首先需要使用 `ForStStateBackend`. 通过以下代码在作业中配置：
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
然后，您可以使用新的状态 API 来编写您的 datastream 作业。有关新的状态 API，请参阅[状态 V2]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}}).

## 高级调优选项

### ForSt 状态后端调优

`ForStStateBackend` 有许多配置项用于性能调优。ForSt 的设计和 RocksDB 非常类似，因此您可以参考[大状态调优]({{< ref "docs/ops/state/large_state_tuning#tuning-rocksdb-or-forst" >}})
来为 ForSt state backend 调优。
除此之外，以下章节介绍了一些 ForSt 独有的配置项。

#### ForSt 主要存储位置

默认情况下，ForSt 会将状态存储在 checkpoint 目录中。在这种情况下，ForSt 可以执行轻量级的 checkpoint 和快速恢复。
用户也可能想将状态存在一个不同的位置，例如，一个不同的 S3 桶。您可以通过以下配置来指定主要存储位置：
```yaml
state.backend.forst.primary-dir: s3://your-bucket/forst-state
```
**注意**：如果设置了此配置，则您可能无法利用轻量级检查点和快速恢复，因为 ForSt 会在 checkpoint 和恢复过程中将文件
从主要存储位置复制到 checkpoint 目录。

#### ForSt 文件缓存

ForSt 使用本地磁盘进行缓存和缓冲。缓存粒度是整个文件。默认启用缓存功能，除非将主要存储位置设置为本地。缓存有两种容量限制策略：
- Size-based: 在缓存大小超过限制时淘汰最旧的文件。
- Reserved-based: 在磁盘上（缓存目录所在的磁盘）剩余空间不足淘汰最旧的文件。
相关配置项如下：
```yaml
state.backend.forst.cache.size-based-limit: 1GB
state.backend.forst.cache.reserve-size: 10GB
```
以上两个配置项可以同时生效。如果同时开启两个选项，在缓存大小超过 size-based 限制或者 reserved-based 限制之一时，缓存会淘汰最老的文件。

可以通过以下配置项来指定缓存目录:
```yaml
state.backend.forst.cache.dir: /tmp/forst-cache
```

#### ForSt 异步线程

ForSt 使用异步 I/O 来读取和写入状态。有 3 种类型的线程：
- 分发线程: 负责分发状态读取和写入请求。
- 读线程: 负责异步读取状态。
- 写线程: 负责异步写入状态。

异步线程的数量可以配置。通常，您不需要调整这些值，因为默认值已经适合大多数情况。
如果有特殊需要，您可以设置以下配置来指定异步线程的数量：
 - `state.backend.forst.executor.read-io-parallelism`: 读线程的数量，默认值是 3。
 - `state.backend.forst.executor.write-io-parallelism`: 写线程的数量，默认值是 1。
 - `state.backend.forst.executor.inline-write`: 写操作否在分发线程内执行。默认值为 true。设置为 false 会提高 CPU 使用率。
 - `state.backend.forst.executor.inline-coordinator`: 是否让任务线程（即主线程）作为分发线程。默认值为 true。设置为 false 会提高 CPU 使用率。

{{< top >}}
