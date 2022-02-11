---
title: "Checkpoints"
weight: 8
type: docs
aliases:
  - /zh/ops/state/checkpoints.html
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

# Checkpoints

## 概述

Checkpoint 使得 Flink 状态具有良好的容错性，通过 checkpoint 机制，Flink 可以对作业的状态和计算位置进行恢复，
从而为应用程序提供与无故障执行时相同的语义。

参考 [Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) 查看如何在 Flink 程序中开启和配置 checkpoint。

## Checkpoint Storage（存储）

启用检查点后，状态会被持久化，以确保在发生故障时进行一致性恢复。checkpoint 生成的状态的持久化位置取决于所选的 **Checkpoint Storage**。

## 可用的 Checkpoint Storage 配置项

开箱即用，Flink 集成的 Checkpoint Storage 类型：

 - *JobManagerCheckpointStorage*
 - *FileSystemCheckpointStorage*

{{< hint info >}}
如果配置了 checkpoint 存储目录，将使用 `FileSystemCheckpointStorage`，否则系统将使用 `JobManagerCheckpointStorage`。
{{< /hint >}}

### JobManagerCheckpointStorage

*JobManagerCheckpointStorage* 将 checkpoint 快照信息存储在 JobManager 的堆内存中。

为避免 JobManager 发生 `OutOfMemoryError` 现象，可以在 `JobManagerCheckpointStorage` 对象中设置最大内存大小，当 checkpoint 所使用的内存超过所配置的最大内存便会触发 checkpoint 失败：

```java
new JobManagerCheckpointStorage(MAX_MEM_STATE_SIZE);
```

`JobManagerCheckpointStorage` 的局限：

  - 每个单独状态的大小默认限制为 5 MB。这个值可以在 `JobManagerCheckpointStorage` 的构造函数中增加。
  - 无论状态配置的最大大小如何，状态都不能大于 Akka frame 大小（请参考 [配置]({{< ref "docs/deployment/config" >}})）。
  - checkpoint 状态必须保证不超过 JobManager 所能提供的内存大小。

`JobManagerCheckpointStorage` 的适用场景：

  - 本地开发、调试。
  - 状态很小的作业，例如同一个时刻只需要处理单条记录（Map、FlatMap、Filter 等）的作业。 如 Kafka 消费者只会产生很小的状态。

### FileSystemCheckpointStorage

使用 *FileSystemCheckpointStorage* 需要配置文件系统的 URL（类型、地址、路径），例如 "hdfs://namenode:40010/flink/checkpoints" 或 "file:///data/flink/checkpoints"。

checkpoint 完成后，它将状态快照数据写入到配置的文件系统和目录中。checkpoint 元数据存储在 JobManager 的内存中（或者，在高可用性模式下，会存储在 checkpoint 元数据文件中）。

如果指定了 checkpoint 目录，`FileSystemCheckpointStorage` 将在此存储 checkpoint 快照数据。

`FileSystemCheckpointStorage` 的适用场景：

  - 所有要求高可用的应用。

建议设置 [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) 的大小为 0。
这将确保分配 JVM 上最大的内存用于用户代码的执行。

## 保留 Checkpoints

Checkpoint 在默认的情况下仅用于作业的异常恢复，并不保留，当程序取消时 checkpoint 就会被删除。
当然，用户可以通过修改配置来保留周期性生成的 checkpoint，这些 checkpoint 在作业失败或取消时不会被清除。
因此用户就可以使用这些 checkpoint 来恢复失败的作业。

```java
CheckpointConfig config = env.getCheckpointConfig();
config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

`ExternalizedCheckpointCleanup` 配置项定义了当作业取消时，对作业 checkpoint 的操作：
- **`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION`**：当作业取消时，保留作业的 checkpoint。注意，这种情况下，需要手动清除该作业保留的 checkpoint。
- **`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`**：当作业取消时，删除作业的 checkpoint。仅当作业失败时，作业的 checkpoint 才会被保留。

### 目录结构

与 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 相似，checkpoint 由元数据文件、数据文件（与 state backend 相关）组成。可通过配置文件中 "state.checkpoints.dir" 配置项来指定元数据文件和数据文件的存储路径，另外也可以在代码中针对单个作业特别指定该配置项。

当前的 checkpoint 目录结构（由 [FLINK-8531](https://issues.apache.org/jira/browse/FLINK-8531) 引入）如下所示:

```yaml
/user-defined-checkpoint-dir
    /{job-id}
        |
        + --shared/
        + --taskowned/
        + --chk-1/
        + --chk-2/
        + --chk-3/
        ...
```

其中 **SHARED** 目录保存了可能被多个 checkpoint 引用的文件，**TASKOWNED** 保存了不会被 JobManager 删除的文件，**EXCLUSIVE** 则保存那些仅被单个 checkpoint 引用的文件。

{{< hint warning >}}
**注意:** Checkpoint 目录不是公共 API 的一部分，因此可能在未来的 Release 中进行改变。
{{< /hint >}}

#### 通过配置文件全局配置

```yaml
state.checkpoints.dir: hdfs:///checkpoints/
```

#### 通过 checkpoint configuration 对单个作业进行配置

```java
env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
```

#### 通过 checkpoint storage 实例进行配置

或者，可以通过初始化预期的 checkpoint storage 实例来配置，这允许设置更细粒度的参数，例如写入缓冲区的大小。

```java
env.getCheckpointConfig().setCheckpointStorage(
  new FileSystemCheckpointStorage("hdfs:///checkpoints-data/", FILE_SIZE_THESHOLD));
```

### Checkpoint 与 Savepoint 的区别

Checkpoint 与 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 有一些区别，体现在 checkpoint 用法上：
- 使用 state backend 特定的数据格式，可能以增量方式存储。
- 不支持 Flink 的特定功能，比如扩缩容。

### 从保留的 checkpoint 中恢复状态

与 savepoint 一样，作业可以从 checkpoint 的元数据文件恢复运行（[savepoint恢复指南]({{< ref "docs/ops/state/savepoints" >}}#resuming-from-savepoints)）。注意，如果元数据文件中信息不充分，那么 jobmanager 就需要使用相关的数据文件来恢复作业(参考[目录结构](#directory-structure))。

```shell
$ bin/flink run -s :checkpointMetaDataPath [:runArgs]
```

{{< top >}}
