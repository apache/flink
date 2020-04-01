---
title: "内存模型详解"
nav-parent_id: ops_mem
nav-pos: 2
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

本文将详细介绍 Flink TaskExecutor 内存模型中的所有组成部分。
关于基本的内存配置方法，请参考[配置指南](mem_setup.html)。

* toc
{:toc}

## 概述

<br />
<center>
  <img src="{{ site.baseurl }}/fig/detailed-mem-model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />
<span class="label label-info">提示</span> 任务堆外内存也包括了用户代码使用的本地内存（非直接内存）。

如上图所示，下表中列出了 Flink TaskExecutor 内存模型的所有组成部分，以影响其大小的相关配置参数。

| &nbsp;&nbsp;**组成部分**&nbsp;&nbsp;                                            | &nbsp;&nbsp;**配置参数**&nbsp;&nbsp;                                                                                                                                                                                                                                                                        | &nbsp;&nbsp;**描述**&nbsp;&nbsp;                                                                                                            |
| :----------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| [框架堆内存（Framework Heap Memory）](#框架内存)                                  | [`taskmanager.memory.framework.heap.size`](../config.html#taskmanager-memory-framework-heap-size)                                                                                                                                                                                                          | 用于 Flink 框架的 JVM 堆内存（进阶配置）。                                                                                                     |
| [任务堆内存（Task Heap Memory）](mem_setup.html#任务算子堆内存)                    | [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size)                                                                                                                                                                                                                    | 用于 Flink 应用的算子及用户代码的 JVM 堆内存。                                                                                                  |
| [托管内存（Managed memory）](mem_setup.html#托管内存)                             | [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size) <br/> [`taskmanager.memory.managed.fraction`](../config.html#taskmanager-memory-managed-fraction)                                                                                                                      | 由 Flink 管理的用于排序、哈希表、缓存中间结果及 RocksDB State Backend 的本地内存。                                                                |
| [框架堆外内存（Framework Off-heap Memory）](#框架内存)                             | [`taskmanager.memory.framework.off-heap.size`](../config.html#taskmanager-memory-framework-off-heap-size)                                                                                                                                                                                                 | 用于 Flink 框架的[堆外内存（直接内存或本地内存）](mem_setup.html#配置堆外内存直接内存或本地内存)（进阶配置）。                                         |
| [任务堆外内存（Task Off-heap Memory）](mem_setup.html#配置堆外内存直接内存或本地内存) | [`taskmanager.memory.task.off-heap.size`](../config.html#taskmanager-memory-task-off-heap-size)                                                                                                                                                                                                           | 用于 Flink 应用的算计及用户代码的[堆外内存（直接内存或本地内存）](mem_setup.html#配置堆外内存直接内存或本地内存)。                                      |
| 网络内存（Network Memory）                                                       | [`taskmanager.memory.network.min`](../config.html#taskmanager-memory-network-min) <br/> [`taskmanager.memory.network.max`](../config.html#taskmanager-memory-network-max) <br/> [`taskmanager.memory.network.fraction`](../config.html#taskmanager-memory-network-fraction)                               | 用于任务之间数据传输的直接内存（例如网络传输缓冲）。该内存部分为基于 [Flink 总内存](mem_setup.html#配置总内存)的[受限的等比内存部分](#受限的等比内存部分)。   |
| [JVM Metaspace](#jvm-参数)                                                      | [`taskmanager.memory.jvm-metaspace.size`](../config.html#taskmanager-memory-jvm-metaspace-size)                                                                                                                                                                                                           | Flink JVM 进程的 Metaspace。                                                                                                                 |
| JVM 开销                                                                        | [`taskmanager.memory.jvm-overhead.min`](../config.html#taskmanager-memory-jvm-overhead-min) <br/> [`taskmanager.memory.jvm-overhead.max`](../config.html#taskmanager-memory-jvm-overhead-max) <br/> [`taskmanager.memory.jvm-overhead.fraction`](../config.html#taskmanager-memory-jvm-overhead-fraction) | 用于其他 JVM 开销的本地内存，例如栈空间、垃圾回收空间等。该内存部分为基于[进程总内存](mem_setup.html#配置总内存)的[受限的等比内存部分](#受限的等比内存部分)。 |
{:.table-bordered}
<br/>

我们可以看到，有些内存部分的大小可以直接通过一个配置参数进行设置，有些则需要根据多个参数进行调整。

## 框架内存

通常情况下，不建议对*框架堆内存*和*框架堆外内存*进行调整。
除非你非常肯定 Flink 的内部数据结构及操作需要更多的内存。
这可能与具体的部署环境及作业结构有关，例如非常高的并发度。
此外，Flink 的部分依赖（例如 Hadoop）在某些特定的情况下也可能会需要更多的直接内存或本地内存。

<span class="label label-info">提示</span> 不管是堆内存还是堆外内存，Flink 中的框架内存和任务内存之间目前是没有隔离的。
对框架和任务内存的区分，主要是为了在后续版本中做进一步优化。

## 受限的等比内存部分

本节介绍下列内存部分的配置方法，它们都可以通过指定在[总内存](mem_setup.html#配置总内存)中所占比例的方式进行配置。
* *网络内存*：可以配置占用 *Flink 总内存*的固定比例
* *JVM 开销*：可以配置占用*进程总内存*的固定比例

请同时参考[概述部分](#概述)。

这些内存部分的大小必须在相应的最大值、最小值范围内，否则 Flink 将无法启动。
最大值、最小值具有默认值，也可以通过相应的配置参数进行设置。
例如，如果仅配置下列参数：
- Flink 总内存 = 1000Mb
- 网络内存最小值 = 64Mb
- 网络内存最大值 = 128Mb
- 网络内存占比 = 0.1

那么网络内存的实际大小将会是 1000Mb x 0.1 = 100Mb，在 64-128Mb 的范围内。

如果将最大值、最小值设置成相同大小，那相当于明确指定了该内存部分的大小。

如果没有明确指定内存部分的大小，Flink 会根据总内存和占比计算出该内存部分的大小。
计算得到的内存大小将受限于相应的最大值、最小。
例如，如果仅配置下列参数：
- Flink 总内存 = 1000Mb
- 网络内存最小值 = 128Mb
- 网络内存最大值 = 256Mb
- 网络内存占比 = 0.1

那么网络内存的实际大小将会是 128Mb，因为根据总内存和占比计算得到的内存大小 100Mb 小于最小值。

如果配置了总内存和其他内存部分的大小，那么 Flink 也有可能会忽略给定的占比。
这种情况下，受限的等比内存部分的实际大小是总内存减去其他所有内存部分后剩余的部分。
这样推导得出的内存大小必须符合最大值、最小值范围，否则 Flink 将无法启动。
例如，如果仅配置下列参数：
- Flink 总内存 = 1000Mb,
- 任务堆内存 = 100Mb,
- 网络内存最小值 = 64Mb
- 网络内存最大值 = 256Mb
- 网络内存占比 = 0.1

Flink 总内存中所有其他内存部分均有默认大小（包括托管内存的默认占比）。
因此，网络内存的实际大小不是根据占比算出的大小（1000Mb x 0.1 = 100Mb），而是 Flink 总内存中剩余的部分。
这个剩余部分的大小必须在 64-256Mb 的范围内，否则将会启动失败。

## JVM 参数

Flink 启动 TaskExecutor 进程时，会根据配置的和自动推导出的各内存部分大小，显式地设置以下 JVM 参数：

| &nbsp;&nbsp;**JVM 参数**&nbsp;&nbsp; | &nbsp;&nbsp;**值**&nbsp;&nbsp;   |
| :---------------------------------- | :------------------------------- |
| *-Xmx* 和 *-Xms*                    | 框架堆内存 + 任务堆内存              |
| *-XX:MaxDirectMemorySize*           | 框架堆外内存 + 任务堆外内存 + 网络内存 |
| *-XX:MaxMetaspaceSize*              | JVM Metaspace                     |
{:.table-bordered}
<br/>

请同时参考[概述部分](#概述)。

## 本地执行
如果你是将 Flink 作为一个单独的 Java 程序运行在你的电脑本地而非创建一个集群（例如在 IDE 中），那么只有下列配置会生效，其他配置参数则不会起到任何效果：

| &nbsp;&nbsp;**组成部分**&nbsp;&nbsp; | &nbsp;&nbsp;**配置参数**&nbsp;&nbsp;                                                                                                                                        | &nbsp;&nbsp;**本地执行时的默认值**&nbsp;&nbsp; |
| :---------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------ |
| 任务堆内存                           | [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size)                                                                                    | 无穷大                                       |
| 任务堆外内存                          | [`taskmanager.memory.task.off-heap.size`](../config.html#taskmanager-memory-task-off-heap-size)                                                                            | 无穷大                                      |
| 托管内存                             | [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size)                                                                                        | 128Mb                                       |
| 网络内存                             | [`taskmanager.memory.network.min`](../config.html#taskmanager-memory-network-min) <br /> [`taskmanager.memory.network.max`](../config.html#taskmanager-memory-network-max) | 64Mb                                        |
{:.table-bordered}
<br/>

本地执行模式下，上面列出的所有内存部分均可以但不是必须进行配置。
如果未配置，则会采用默认值。
其中，[*任务堆内存*](mem_setup.html#任务算子堆内存)和*任务堆外内存*的默认值无穷大（*Long.MAX_VALUE*字节），以及[托管内存](mem_setup.html#托管内存)的默认值 128Mb 均只针对本地执行模式。

<span class="label label-info">提示</span> 这种情况下，任务堆内存的大小与实际的堆空间大小无关。
该配置参数可能与后续版本中的进一步优化相关。
本地执行模式下，JVM 堆空间的实际大小不受 Flink 掌控，而是取决于本地执行进程是如何启动的。
如果希望控制 JVM 的堆空间大小，可以在启动进程时明确地指定相关的 JVM 参数，即 *-Xmx* 和 *-Xms*.
