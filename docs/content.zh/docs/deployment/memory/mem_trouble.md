---
title: "常见问题"
weight: 6
type: docs
aliases:
  - /zh/deployment/memory/mem_trouble.html
  - /zh/ops/memory/mem_trouble.html
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

# 常见问题

## IllegalConfigurationException

如果遇到从 *TaskExecutorProcessUtils* 或 *JobManagerProcessUtils* 抛出的 *IllegalConfigurationException* 异常，这通常说明您的配置参数中存在无效值（例如内存大小为负数、占比大于 1 等）或者配置冲突。
请根据异常信息，确认出错的内存部分的相关文档及[配置信息]({{< ref "docs/deployment/config" >}}#memory-configuration)。

## OutOfMemoryError: Java heap space

该异常说明 JVM 的堆空间过小。
可以通过增大[总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)、TaskManager 的[任务堆内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory)、JobManager 的 [JVM 堆内存]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap)等方法来增大 JVM 堆空间。

<span class="label label-info">提示</span>
也可以增大 TaskManager 的[框架堆内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#framework-memory)。
这是一个进阶配置，只有在确认是 Flink 框架自身需要更多内存时才应该去调整。

## OutOfMemoryError: Direct buffer memory

该异常通常说明 JVM 的*直接内存*限制过小，或者存在*直接内存泄漏（Direct Memory Leak）*。
请确认用户代码及外部依赖中是否使用了 JVM *直接内存*，以及如果使用了直接内存，是否配置了足够的内存空间。
可以通过调整堆外内存来增大直接内存限制。
有关堆外内存的配置方法，请参考 [TaskManager]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#configure-off-heap-memory-direct-or-native)、[JobManager]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-off-heap-memory) 以及 [JVM 参数]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters)的相关文档。

## OutOfMemoryError: Metaspace

该异常说明 [JVM Metaspace 限制]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters)过小。
可以尝试调整 [TaskManager]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-metaspace-size)、[JobManager]({{< ref "docs/deployment/config" >}}#jobmanager-memory-jvm-metaspace-size) 的 JVM Metaspace。

## IOException: Insufficient number of network buffers

该异常仅与 TaskManager 相关。

该异常通常说明[网络内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model)过小。
可以通过调整以下配置参数增大*网络内存*：
* [`taskmanager.memory.network.min`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min)
* [`taskmanager.memory.network.max`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)
* [`taskmanager.memory.network.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)

<a name="container-memory-exceeded" />

## 容器（Container）内存超用

如果 Flink 容器尝试分配超过其申请大小的内存（Yarn、Mesos 或 Kubernetes），这通常说明 Flink 没有预留出足够的本地内存。
可以通过外部监控系统或者容器被部署环境杀掉时的错误信息判断是否存在容器内存超用。

对于 *JobManager* 进程，你还可以尝试启用 *JVM 直接内存限制*（[`jobmanager.memory.enable-jvm-direct-memory-limit`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-enable-jvm-direct-memory-limit)），以排除 *JVM 直接内存泄漏*的可能性。

If [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend) is used：
* and memory controlling is disabled: You can try to increase the TaskManager's [managed memory]({{< ref "docs/deployment/memory/mem_setup" >}}#managed-memory).
* and memory controlling is enabled and non-heap memory increases during savepoint or full checkpoints: This may happen due to the `glibc` memory allocator (see [glibc bug](https://sourceware.org/bugzilla/show_bug.cgi?id=15321)).
  You can try to add the [environment variable]({{< ref "docs/deployment/config" >}}#forwarding-environment-variables) `MALLOC_ARENA_MAX=1` for TaskManagers.

此外，还可以尝试增大 [JVM 开销]({{< ref "docs/deployment/memory/mem_setup" >}}#capped-fractionated-components)。

请参考[如何配置容器内存]({{< ref "docs/deployment/memory/mem_tuning" >}}#configure-memory-for-containers)。
