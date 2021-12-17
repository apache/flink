---
title: "升级指南"
weight: 7
type: docs
aliases:
  - /zh/deployment/memory/mem_migration.html
  - /zh/ops/memory/mem_migration.html
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

# 升级指南

在 *1.10* 和 *1.11* 版本中，Flink 分别对 [TaskManager]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) 和 [JobManager]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}) 的内存配置方法做出了较大的改变。
部分配置参数被移除了，或是语义上发生了变化。
本篇升级指南将介绍如何将 [*Flink 1.9 及以前版本*](https://nightlies.apache.org/flink/flink-docs-release-1.9/ops/mem_setup.html)的 TaskManager 内存配置升级到 *Flink 1.10 及以后版本*，
以及如何将 *Flink 1.10 及以前版本*的 JobManager 内存配置升级到 *Flink 1.11 及以后版本*。

* toc


{{< hint warning >}}
**注意：** 请仔细阅读本篇升级指南。
使用原本的和新的内存配制方法可能会使内存组成部分具有截然不同的大小。
未经调整直接沿用 Flink 1.10 以前版本的 TaskManager 配置文件或 Flink 1.11 以前版本的 JobManager 配置文件，可能导致应用的行为、性能发生变化，甚至造成应用执行失败。
{{< /hint >}}

<span class="label label-info">提示</span>
在 *1.10/1.11* 版本之前，Flink 不要求用户一定要配置 TaskManager/JobManager 内存相关的参数，因为这些参数都具有默认值。
[新的内存配置]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)要求用户至少指定下列配置参数（或参数组合）的其中之一，否则 Flink 将无法启动。

| &nbsp;&nbsp;**TaskManager:**&nbsp;&nbsp;                                                                                                                                        | &nbsp;&nbsp;**JobManager:**&nbsp;&nbsp;                                      |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------- |
| [`taskmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size)                                                                                                       | [`jobmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-flink-size)     |
| [`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)                                                                                                   | [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size) |
| [`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size) 和 <br/> [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size) | [`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size)       |

<br/>

Flink 自带的[默认 flink-conf.yaml](#default-configuration-in-flink-confyaml) 文件指定了 [`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)（*>= 1.10*）和 [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size) (*>= 1.11*)，以便与此前的行为保持一致。

可以使用这张[电子表格](https://docs.google.com/spreadsheets/d/1mJaMkMPfDJJ-w6nMXALYmTc4XxiV30P5U7DzgwLkSoE)来估算和比较原本的和新的内存配置下的计算结果。

<a name="migrate-task-manager-memory-configuration" />

## 升级 TaskManager 内存配置

<a name="changes-in-configuration-options" />

### 配置参数变化

本节简要列出了 *Flink 1.10* 引入的配置参数变化，并援引其他章节中关于如何升级到新配置参数的相关描述。

下列配置参数已被彻底移除，配置它们将不会产生任何效果。

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">移除的配置参数</th>
            <th class="text-left">备注</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>taskmanager.memory.fraction</h5></td>
            <td>
                请参考新配置参数 <a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction">taskmanager.memory.managed.fraction</a> 的相关描述。
                新的配置参数与被移除的配置参数在语义上有所差别，因此其配置值通常也需要做出适当调整。
                请参考<a href="#managed-memory">如何升级托管内存</a>。
            </td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.off-heap</h5></td>
             <td>Flink 不再支持堆上的（On-Heap）<i>托管内存</i>。请参考<a href="#managed-memory">如何升级托管内存</a>。</td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.preallocate</h5></td>
             <td>Flink 不再支持内存预分配，今后<i>托管内存</i>将都是惰性分配的。请参考<a href="#managed-memory">如何升级托管内存</a>。</td>
        </tr>
    </tbody>
</table>

下列配置参数将被弃用，出于向后兼容性考虑，配置它们将被解读成对应的新配置参数。

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">弃用的配置参数</th>
            <th class="text-left">对应的新配置参数</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>taskmanager.heap.size</h5></td>
            <td>
                <ul>
                  <li><a href="{{< ref "docs/deployment/resource-providers/standalone/overview" >}}">独立部署模式（Standalone Deployment）</a>下：<a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size">taskmanager.memory.flink.size</a></li>
                  <li>容器化部署模式（Containerized Deployment）下：<a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size">taskmanager.memory.process.size</a></li>
                </ul>
                请参考<a href="#total-memory-previously-heap-memory">如何升级总内存</a>。
            </td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.size</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size">taskmanager.memory.managed.size</a>。请参考<a href="#managed-memory">如何升级托管内存</a>。</td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.min</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min">taskmanager.memory.network.min</a></td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.max</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max">taskmanager.memory.network.max</a></td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.fraction</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction">taskmanager.memory.network.fraction</a></td>
        </tr>
    </tbody>
</table>

尽管网络内存的配置参数没有发生太多变化，我们仍建议您检查其配置结果。
网络内存的大小可能会受到其他内存部分大小变化的影响，例如总内存变化时，根据占比计算出的网络内存也可能发生变化。
请参考[内存模型详解]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model)。

容器切除（Cut-Off）内存相关的配置参数（`containerized.heap-cutoff-ratio` 和 `containerized.heap-cutoff-min`）将不再对 TaskManager 进程生效。
请参考[如何升级容器切除内存](#container-cut-off-memory)。

<a name="total-memory-previously-heap-memory" />

### 总内存（原堆内存）

在原本的内存配置方法中，用于指定用于 Flink 的总内存的配置参数是 `taskmanager.heap.size` 或 `taskmanager.heap.mb`。
尽管这两个参数以“堆（Heap）”命名，实际上它们指定的内存既包含了 JVM 堆内存，也包含了其他堆外内存部分。
这两个配置参数目前已被弃用。

如果配置了上述弃用的参数，同时又没有配置与之对应的新配置参数，那它们将按如下规则对应到新的配置参数。
* 独立部署模式（Standalone Deployment）下：Flink 总内存（[`taskmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size)）
* 容器化部署模式（Containerized Deployment）下（Yarn）：进程总内存（[`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)）

建议您尽早使用新的配置参数取代启用的配置参数，它们在今后的版本中可能会被彻底移除。

请参考[如何配置总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory).

<a name="jvm-heap-memory" />

### JVM 堆内存

此前，JVM 堆空间由托管内存（仅在配置为堆上时）及 Flink 用到的所有其他堆内存组成。
这里的其他堆内存是由总内存减去所有其他非堆内存得到的。
请参考[如何升级托管内存](#managed-memory)。

现在，如果仅配置了*Flink总内存*或*进程总内存*，JVM 的堆空间依然是根据总内存减去所有其他非堆内存得到的。
请参考[如何配置总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)。

此外，你现在可以更直接地控制用于任务和算子的 JVM 的堆内存（[`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)），详见[任务堆内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory)。
如果流处理作业选择使用 Heap State Backend（[MemoryStateBackend]({{< ref "docs/ops/state/state_backends" >}}#memorystatebackend)
或 [FsStateBackend]({{< ref "docs/ops/state/state_backends" >}}#fsstatebackend)），那么它同样需要使用 JVM 堆内存。

Flink 现在总是会预留一部分 JVM 堆内存供框架使用（[`taskmanager.memory.framework.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-heap-size)）。
请参考[框架内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#framework-memory)。

<a name="managed-memory" />

### 托管内存

请参考[如何配置托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。

<a name="explicit-size" />

#### 明确的大小

原本用于指定明确的托管内存大小的配置参数（`taskmanager.memory.size`）已被弃用，与它具有相同语义的新配置参数为 [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size)。
建议使用新的配置参数，原本的配置参数在今后的版本中可能会被彻底移除。

<a name="fraction" />

#### 占比

此前，如果不指定明确的大小，也可以将托管内存配置为占用总内存减去网络内存和容器切除内存（仅在 [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}})）之后剩余部分的固定比例（`taskmanager.memory.fraction`）。
该配置参数已经被彻底移除，配置它不会产生任何效果。
请使用新的配置参数 [`taskmanager.memory.managed.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction)。
在未通过 [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size) 指定明确大小的情况下，新的配置参数将指定[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)在 [Flink 总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)中的所占比例。

<a name="rocksdb-state" />

#### RocksDB State Backend

流处理作业如果选择使用 [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#rocksdbstatebackend)，它使用的本地内存现在也被归为[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。
默认情况下，RocksDB 将限制其内存用量不超过[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)大小，以避免在 [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}) 上容器被杀。你也可以通过设置 [state.backend.rocksdb.memory.managed]({{< ref "docs/deployment/config" >}}#state-backend-rocksdb-memory-managed) 来关闭 RocksDB 的内存控制。
请参考[如何升级容器切除内存](#container-cut-off-memory)。

<a name="other-changes" />

#### 其他变化

此外，Flink 1.10 对托管内存还引入了下列变化：
* [托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)现在总是在堆外。配置参数 `taskmanager.memory.off-heap` 已被彻底移除，配置它不会产生任何效果。
* [托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)现在使用本地内存而非直接内存。这意味着托管内存将不在 JVM 直接内存限制的范围内。
* [托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)现在总是惰性分配的。配置参数 `taskmanager.memory.preallocate` 已被彻底移除，配置它不会产生任何效果。

<a name="migrate-job-manager-memory-configuration" />

## 升级 JobManager 内存配置

在原本的内存配置方法中，用于指定 *JVM 堆内存* 的配置参数是:
* `jobmanager.heap.size`
* `jobmanager.heap.mb`

尽管这两个参数以“堆（Heap）”命名，在此之前它们实际上只有在[独立部署模式]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})才完全对应于 *JVM 堆内存*。
在容器化部署模式下（[Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) 和 [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}})），它们指定的内存还包含了其他堆外内存部分。
*JVM 堆空间*的实际大小，是参数指定的大小减去容器切除（Cut-Off）内存后剩余的部分。
容器切除内存在 *1.11* 及以上版本中已被彻底移除。

从 *1.11* 版本开始，Flink 将采用与[独立部署模式]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})相同的方式设置这些参数。

这两个配置参数目前已被弃用。
如果配置了上述弃用的参数，同时又没有配置与之对应的新配置参数，那它们将按如下规则对应到新的配置参数。
* 独立部署模式（Standalone Deployment）：JVM 堆内存（[`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size)）
* 容器化部署模式（Containerized Deployment）下（Kubernetes、Yarn）：进程总内存（[`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size)）

建议您尽早使用新的配置参数取代启用的配置参数，它们在今后的版本中可能会被彻底移除。

如果仅配置了 *Flink 总内存*或*进程总内存*，那么 [JVM 堆内存]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap)将是总内存减去其他内存部分后剩余的部分。
请参考[如何配置总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)。
此外，也可以通过配置 [`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size) 的方式直接指定 [JVM 堆内存]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap)。

<a name="flink-jvm-process-memory-limits" />

## Flink JVM 进程内存限制

从 *1.10* 版本开始，Flink 通过设置相应的 JVM 参数，对 TaskManager 进程使用的 *JVM Metaspace* 和 *JVM 直接内存*进行限制。
从 *1.11* 版本开始，Flink 同样对 JobManager 进程使用的 *JVM Metaspace* 进行限制。
此外，还可以通过设置 [`jobmanager.memory.enable-jvm-direct-memory-limit`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-enable-jvm-direct-memory-limit) 对 JobManager 进程的 *JVM 直接内存*进行限制。
请参考 [JVM 参数]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters)。

Flink 通过设置上述 JVM 内存限制降低内存泄漏问题的排查难度，以避免出现[容器内存溢出]({{< ref "docs/deployment/memory/mem_trouble" >}}#container-memory-exceeded)等问题。
请参考常见问题中关于 [JVM Metaspace]({{< ref "docs/deployment/memory/mem_trouble" >}}#outofmemoryerror-metaspace) 和 [JVM 直接内存]({{< ref "docs/deployment/memory/mem_trouble" >}}#outofmemoryerror-direct-buffer-memory) *OutOfMemoryError* 异常的描述。

<a name="container-cut-off-memory" />

## 容器切除（Cut-Off）内存

在容器化部署模式（Containerized Deployment）下，此前你可以指定切除内存。
这部分内存将预留给所有未被 Flink 计算在内的内存开销。
其主要来源是不受 Flink 直接管理的依赖使用的内存，例如 RocksDB、JVM 内部开销等。
相应的配置参数（`containerized.heap-cutoff-ratio` 和 `containerized.heap-cutoff-min`）不再生效。
新的内存配置方法引入了新的内存组成部分来具体描述这些内存用量。

<a name="for-taskmanagers" />

### TaskManager

流处理作业如果使用了 [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend)，RocksDB 使用的本地内存现在将被归为[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。
默认情况下，RocksDB 将限制其内存用量不超过[托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)大小。
请同时参考[如何升级托管内存](#managed-memory)以及[如何配置托管内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)。

其他堆外（直接或本地）内存开销，现在可以通过下列配置参数进行设置：
* 任务堆外内存（[`taskmanager.memory.task.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)）
* 框架堆外内存（[`taskmanager.memory.framework.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)）
* JVM Metaspace（[`taskmanager.memory.jvm-metaspace.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-metaspace-size)）
* [JVM 开销]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model)

<a name="for-jobmanagers" />

### JobManager

可以通过下列配置参数设置堆外（直接或本地）内存开销：
* 堆外内存 ([`jobmanager.memory.off-heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-off-heap-size))
* JVM Metaspace ([`jobmanager.memory.jvm-metaspace.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-jvm-metaspace-size))
* [JVM 开销]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#detailed-configuration)

<a name="default-configuration-in-flink-confyaml" />

## flink-conf.yaml 中的默认配置

本节描述 Flink 自带的默认 `flink-conf.yaml` 文件中的变化。

原本的 TaskManager 总内存（`taskmanager.heap.size`）被新的配置项 [`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size) 所取代。
默认值从 1024Mb 增加到了 1728Mb。

原本的 JobManager 总内存（`jobmanager.heap.size`）被新的配置项 [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size) 所取代。
默认值从 1024Mb 增加到了 1600Mb。

请参考[如何配置总内存]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)。

{{< hint warning >}}
**注意：** 使用新的默认 `flink-conf.yaml` 可能会造成各内存部分的大小发生变化，从而产生性能变化。
{{< /hint >}}
