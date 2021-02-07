---
title: "调优指南"
weight: 5
type: docs
aliases:
  - /zh/deployment/memory/mem_tuning.html
  - /zh/ops/memory/mem_tuning.html
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

# 调优指南

本文在的基本的[配置指南]({{< ref "docs/deployment/memory/mem_setup" >}})的基础上，介绍如何根据具体的使用场景调整内存配置，以及在不同使用场景下分别需要重点关注哪些配置参数。

<a name="configure-memory-for-standalone-deployment" />

## 独立部署模式（Standalone Deployment）下的内存配置

[独立部署模式]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})下，我们通常更关注 Flink 应用本身使用的内存大小。
建议配置 [Flink 总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)（[`taskmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size) 或者 [`jobmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-flink-size" >}})）或其组成部分。
此外，如果出现 [Metaspace 不足的问题]({{< ref "docs/deployment/memory/mem_trouble" >}}#outofmemoryerror-metaspace)，可以调整 *JVM Metaspace* 的大小。

这种情况下通常无需配置*进程总内存*，因为不管是 Flink 还是部署环境都不会对 *JVM 开销* 进行限制，它只与机器的物理资源相关。

<a name="configure-memory-for-containers" />

## 容器（Container）的内存配置

在容器化部署模式（Containerized Deployment）下（[Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}})、[Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}) 或 [Mesos]({{< ref "docs/deployment/resource-providers/mesos" >}})），建议配置[进程总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)（[`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size) 或者 [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size)）。
该配置参数用于指定分配给 Flink *JVM 进程*的总内存，也就是需要申请的容器大小。

<span class="label label-info">提示</span>
如果配置了 *Flink 总内存*，Flink 会自动加上 JVM 相关的内存部分，根据推算出的*进程总内存*大小申请容器。

<div class="alert alert-warning">
  <strong>注意：</strong> 如果 Flink 或者用户代码分配超过容器大小的非托管的堆外（本地）内存，部署环境可能会杀掉超用内存的容器，造成作业执行失败。
</div>

请参考[容器内存超用]({{< ref "docs/deployment/memory/mem_trouble" >}}#container-memory-exceeded)中的相关描述。

<a name="configure-memory-for-state-backends" />

## State Backend 的内存配置

本章节内容仅与 TaskManager 相关。

在部署 Flink 流处理应用时，可以根据 [State Backend]({{< ref "docs/ops/state/state_backends" >}}) 的类型对集群的配置进行优化。

### Heap State Backend

执行无状态作业或者使用 Heap State Backend（[MemoryStateBackend]({{< ref "docs/ops/state/state_backends" >}}#memorystatebackend)
或 [FsStateBackend]({{< ref "docs/ops/state/state_backends" >}}#fsstatebackend)）时，建议将[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)设置为 0。
这样能够最大化分配给 JVM 上用户代码的内存。

### RocksDB State Backend

[RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#rocksdbstatebackend) 使用本地内存。
默认情况下，RocksDB 会限制其内存用量不超过用户配置的[*托管内存*]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。
因此，使用这种方式存储状态时，配置足够多的*托管内存*是十分重要的。
如果你关闭了 RocksDB 的内存控制，那么在容器化部署模式下如果 RocksDB 分配的内存超出了申请容器的大小（[进程总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)），可能会造成 TaskExecutor 被部署环境杀掉。
请同时参考[如何调整 RocksDB 内存]({{< ref "docs/ops/state/large_state_tuning" >}}#tuning-rocksdb-memory)以及 [state.backend.rocksdb.memory.managed]({{< ref "docs/deployment/config" >}}#state-backend-rocksdb-memory-managed)。

<a name="configure-memory-for-batch-jobs" />

## 批处理作业的内存配置

Flink 批处理算子使用[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)来提高处理效率。
算子运行时，部分操作可以直接在原始数据上进行，而无需将数据反序列化成 Java 对象。
这意味着[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)对应用的性能具有实质上的影响。
因此 Flink 会在不超过其配置限额的前提下，尽可能分配更多的[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。
Flink 明确知道可以使用的内存大小，因此可以有效避免 `OutOfMemoryError` 的发生。
当[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)不足时，Flink 会优雅地将数据落盘。

## SortMerge数据Shuffle内存配置

对于SortMerge数据Shuffle，每个ResultPartition需要的网络缓冲区（Buffer）数目是由[taskmanager.network.sort-
shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers)这个配置决定的。它的
默认值是64，是比较小的。虽然64个网络Buffer已经可以支持任意规模的并发，但性能可能不是最好的。对于大并发的作业，通
过增大这个配置值，可以提高落盘数据的压缩率并且减少网络小包的数量，从而有利于提高Shuffle性能。为了增大这个配置值，
你可能需要通过调整[taskmanager.memory.network.fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)，
[taskmanager.memory.network.min]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min)和[taskmanager.memory
.network.max]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)这三个参数来增大总的网络内存大小从而避免出现
`insufficient number of network buffers`错误。

除了网络内存，SortMerge数据Shuffle还需要使用一些JVM Direct Memory来进行Shuffle数据的写出与读取。所以，为了使
用SortMerge数据Shuffle你可能还需要通过增大这个配置值[taskmanager.memory.task.off-heap.size
]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)来为其来预留一些JVM Direct Memory。如果在你开启
SortMerge数据Shuffle之后出现了Direct Memory OOM的错误，你只需要继续加大上面的配置值来预留更多的Direct Memory
直到不再发生Direct Memory OOM的错误为止。
