---
title: "调优指南"
nav-parent_id: ops_mem
nav-pos: 4
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

本文在的基本的[配置指南](mem_setup.html)的基础上，介绍如何根据具体的使用场景调整 TaskExecutor 的内存配置，以及在不同使用场景下分别需要重点关注哪些配置参数。

* toc
{:toc}

## 独立部署模式（Standalone Deployment）下的内存配置

[独立部署模式](../deployment/cluster_setup.html)，我们通常更关注 Flink 应用本身使用的内存大小。
建议配置 [Flink 总内存](mem_setup.html#配置总内存)（[`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size)）或者它的[组成部分](mem_detail.html)。
此外，如果出现 [Metaspace 不足的问题](mem_trouble.html#outofmemoryerror-metaspace)，可以调整 *JVM Metaspace* 的大小。

这种情况下通常无需配置*进程总内存*，因为不管是 Flink 还是部署环境都不会对 *JVM 开销* 进行限制，它只与机器的物理资源相关。

## 容器（Container）的内存配置

在容器化部署模式（Containerized Deployment）下（[Kubernetes](../deployment/kubernetes.html)、[Yarn](../deployment/yarn_setup.html) 或 [Mesos](../deployment/mesos.html)），建议配置[进程总内存](mem_setup.html#配置总内存)（[`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size)）。
该配置参数用于指定分配给 Flink *JVM 进程*的总内存，也就是需要申请的容器大小。

<span class="label label-info">提示</span> 如果配置了 *Flink 总内存*，Flink 会自动加上 JVM 相关的内存部分，根据推算出的*进程总内存*大小申请容器。
请参考[内存模型详解](mem_detail.html)。

<div class="alert alert-warning">
  <strong>注意：</strong> 如果 Flink 或者用户代码分配超过容器大小的非托管的堆外（本地）内存，部署环境可能会杀掉超用内存的容器，造成作业执行失败。
</div>
请参考[容器内存超用](mem_trouble.html#容器container内存超用)中的相关描述。

## State Backend 的内存配置

在部署 Flink 流处理应用时，可以根据 [State Backend](../state/state_backends.html) 的类型对集群的配置进行优化。

### Heap State Backend

执行无状态作业或者使用 Heap State Backend（[MemoryStateBackend](../state/state_backends.html#memorystatebackend)
或 [FsStateBackend](../state/state_backends.html#fsstatebackend)）时，建议将[托管内存](mem_setup.html#托管内存)设置为 0。
这样能够最大化分配给 JVM 上用户代码的内存。

### RocksDB State Backend

[RocksDBStateBackend](../state/state_backends.html#rocksdbstatebackend) 使用本地内存。
默认情况下，RocksDB 会限制其内存用量不超过用户配置的[*托管内存*](mem_setup.html#托管内存)。
因此，使用这种方式存储状态时，配置足够多的*托管内存*是十分重要的。
如果你关闭了 RocksDB 的内存控制，那么在容器化部署模式下如果 RocksDB 分配的内存超出了申请容器的大小（[进程总内存](mem_setup.html#配置总内存)），可能会造成 TaskExecutor 被部署环境杀掉。
请同时参考[如何调整 RocksDB 内存](../state/large_state_tuning.html#tuning-rocksdb-memory)以及 [state.backend.rocksdb.memory.managed](../config.html#state-backend-rocksdb-memory-managed)。

## 批处理作业的内存配置

Flink 批处理算子使用[托管内存](../memory/mem_setup.html#托管内存)来提高处理效率。
算子运行时，部分操作可以直接在原始数据上进行，而无需将数据反序列化成 Java 对象。
这意味着[托管内存](../memory/mem_setup.html#托管内存)对应用的性能具有实质上的影响。
因此 Flink 会在不超过其配置限额的前提下，尽可能分配更多的[托管内存](../memory/mem_setup.html#托管内存)。
Flink 明确知道可以使用的内存大小，因此可以有效避免 `OutOfMemoryError` 的发生。
当[托管内存](../memory/mem_setup.html#托管内存)不足时，Flink 会优雅的将数据落盘。
