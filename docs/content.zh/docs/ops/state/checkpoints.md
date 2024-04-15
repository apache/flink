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
Checkpoint 使 Flink 的状态具有良好的容错性，通过 checkpoint 机制，Flink 可以对作业的状态和计算位置进行恢复。

参考 [Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) 查看如何在 Flink 程序中开启和配置 checkpoint。

要了解 checkpoints 和 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 之间的区别，请参阅 [checkpoints 与 savepoints]({{< ref "docs/ops/state/checkpoints_vs_savepoints" >}})。

## Checkpoint 存储

启用 Checkpoint 后，managed State 将被持久化，来确保故障后 Flink job 恢复的一致性。
Checkpoint 期间 State 存储的位置取决于所选的 **Checkpoint 存储**。

## 可选的 Checkpoint 存储

Flink 开箱即用地提供了两种 Checkpoint 存储类型：

- *JobManagerCheckpointStorage*
- *FileSystemCheckpointStorage*

{{< hint info >}}
如果配置了 Checkpoint 目录，将使用 `FileSystemCheckpointStorage`，否则系统将使用 `JobManagerCheckpointStorage`。
{{< /hint >}}

### JobManagerCheckpointStorage

*JobManagerCheckpointStorage* 将 Checkpoint 快照存储在 JobManager 的堆内存中。

可以将其配置为在超过一定大小时使 Checkpoint 失败，以避免 JobManager 出现 `OutOfMemoryError`。 
要设置此功能，用户可以实例化具有相应最大大小的 `JobManagerCheckpointStorage`：

```java
new JobManagerCheckpointStorage(MAX_MEM_STATE_SIZE);
```

`JobManagerCheckpointStorage` 的限制:

- 默认情况下，每个 State 的大小限制为 5 MB。 可以在 `JobManagerCheckpointStorage` 的构造函数中修改大小。
- 无论配置的最大 State 大小如何，状态都不能大于 Pekka 框架的大小（请参阅 [配置参数]({{< ref "docs/deployment/config" >}})）。
- 聚合后总的状态大小必须小于 JobManager 的内存上限。

鼓励在以下场景使用 JobManagerCheckpointStorage：

- 本地开发和调试
- 使用很少状态的作业，例如仅包含每次仅存储一条记录（Map、FlatMap、Filter...）的作业。 Kafka 消费者需要很少的 State。

### FileSystemCheckpointStorage

*FileSystemCheckpointStorage* 配置中包含文件系统 URL（类型、地址、路径），
例如 "hdfs://namenode:40010/flink/checkpoints" 或 "file:///data/flink/checkpoints"。

Checkpoint 时， Flink 会将 State 快照写到配置的文件系统和目录的文件中。
最少的元数据存储在 JobManager 的内存中（或者，高可用性模式下存储在 Checkpoint 的元数据中）。

如果指定了 Checkpoint 目录，`FileSystemCheckpointStorage` 将用于保存 Checkpoint 快照。

鼓励使用 `FileSystemCheckpointStorage` 的场景：

- 所有高可用的场景。

## 保留 Checkpoint

Checkpoint 在默认的情况下仅用于恢复失败的作业，并不保留，当程序取消时 checkpoint 就会被删除。当然，你可以通过配置来保留 checkpoint，这些被保留的 checkpoint 在作业失败或取消时不会被清除。这样，你就可以使用该 checkpoint 来恢复失败的作业。

```java
CheckpointConfig config = env.getCheckpointConfig();
config.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
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

#### 创建 state backend 对单个作业进行配置

```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs:///checkpoints-data/");
env.configure(config);
```

### 从保留的 checkpoint 中恢复状态

与 savepoint 一样，作业可以从 checkpoint 的元数据文件恢复运行（[savepoint恢复指南]({{< ref "docs/ops/state/savepoints" >}}#resuming-from-savepoints)）。注意，如果元数据文件中信息不充分，那么 jobmanager 就需要使用相关的数据文件来恢复作业(参考[目录结构](#directory-structure))。

```shell
$ bin/flink run -s :checkpointMetaDataPath [:runArgs]
```

{{< top >}}
