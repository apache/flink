---
title: "常见问题"
nav-parent_id: ops_mem
nav-pos: 5
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

* toc
{:toc}

## IllegalConfigurationException

如果遇到从 *TaskExecutorProcessUtils* 抛出的 *IllegalConfigurationException* 异常，这通常说明您的配置参数中存在无效值（例如内存大小为负数、占比大于 1 等）或者配置冲突。
请根据异常信息，确认[内存模型详解](../config.html#memory-configuration)中与出错的内存部分对应章节的内容。

## OutOfMemoryError: Java heap space

该异常说明 JVM 的堆空间过小。
可以通过增大[总内存](mem_setup.html#配置总内存)或[任务堆内存](mem_setup.html#任务算子堆内存)的方法来增大 JVM 堆空间。

<span class="label label-info">提示</span> 也可以增大[框架堆内存](mem_setup_tm.html#框架内存)。这是一个进阶配置，只有在确认是 Flink 框架自身需要更多内存时才应该去调整。

## OutOfMemoryError: Direct buffer memory

该异常通常说明 JVM 的*直接内存*限制过小，或者存在*直接内存泄漏（Direct Memory Leak）*。
请确认用户代码及外部依赖中是否使用了 JVM *直接内存*，以及如果使用了直接内存，是否配置了足够的内存空间。
可以通过调整[堆外内存](mem_detail.html)来增大直接内存限制。
请同时参考[如何配置堆外内存](mem_setup.html#配置堆外内存直接内存或本地内存))以及 Flink 设置的 [JVM 参数](mem_detail.html#jvm-参数)。

## OutOfMemoryError: Metaspace

该异常说明 [JVM Metaspace 限制](mem_detail.html#jvm-参数)过小。
可以尝试调整 [JVM Metaspace 参数](../config.html#taskmanager-memory-jvm-metaspace-size)。

## IOException: Insufficient number of network buffers

该异常通常说明[网络内存](mem_detail.html)过小。
可以通过调整以下配置参数增大*网络内存*：
* [`taskmanager.memory.network.min`](../config.html#taskmanager-memory-network-min)
* [`taskmanager.memory.network.max`](../config.html#taskmanager-memory-network-max)
* [`taskmanager.memory.network.fraction`](../config.html#taskmanager-memory-network-fraction)

## 容器（Container）内存超用

如果 TaskExecutor 容器尝试分配超过其申请大小的内存（Yarn、Mesos 或 Kubernetes），这通常说明 Flink 没有预留出足够的本地内存。
可以通过外部监控系统或者容器被部署环境杀掉时的错误信息判断是否存在容器内存超用。

如果使用了 [RocksDBStateBackend](../state/state_backends.html#rocksdbstatebackend) 且没有开启内存控制，也可以尝试增大[托管内存](mem_setup.html#托管内存)。

此外，还可以尝试增大 [JVM 开销](mem_detail.html)。

请参考[如何配置容器内存](mem_tuning.html#容器container的内存配置)。
