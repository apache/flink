---
title: "Checkpointing"
weight: 4
type: docs
aliases:
  - /zh/dev/stream/state/checkpointing.html
  - /zh/apis/streaming/fault_tolerance.html
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

# Checkpointing

Flink 中的每个方法或算子都能够是**有状态的**（阅读 [working with state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}) 了解更多）。
状态化的方法在处理单个 元素/事件 的时候存储数据，让状态成为使各个类型的算子更加精细的重要部分。
为了让状态容错，Flink 需要为状态添加 **checkpoint（检查点）**。Checkpoint 使得 Flink 能够恢复状态和在流中的位置，从而向应用提供和无故障执行时一样的语义。

[容错文档]({{< ref "docs/learn-flink/fault_tolerance" >}}) 中介绍了 Flink 流计算容错机制内部的技术原理。


## 前提条件

Flink 的 checkpoint 机制会和持久化存储进行交互，读写流与状态。一般需要：

  - 一个能够回放一段时间内数据的持久化数据源，例如持久化消息队列（例如 Apache Kafka、RabbitMQ、 Amazon Kinesis、 Google PubSub 等）或文件系统（例如 HDFS、 S3、 GFS、 NFS、 Ceph 等）。
  - 存放状态的持久化存储，通常为分布式文件系统（比如 HDFS、 S3、 GFS、 NFS、 Ceph 等）。

## 开启与配置 Checkpoint

默认情况下 checkpoint 是禁用的。通过调用 `StreamExecutionEnvironment` 的 `enableCheckpointing(n)` 来启用 checkpoint，其中 *n* 表示 [checkpoint 时间间隔]({{< ref "docs/ops/production_ready#choose-the-right-checkpoint-interval" >}})，单位毫秒。

Checkpoint 其他的属性包括：

  - *checkpoint 存储（storage）*：用户可以设置 checkpoint 快照持久化的位置。 默认情况下，Flink 会存储在 JobManager 的堆中。在生产环境中，建议改用持久化的文件系统。 请参考 [checkpoint 存储]({{< ref "docs/ops/state/checkpoints#checkpoint-storage" >}}) 以获取有关作业级别和集群级别的可用配置的更多详细信息。

  - *精确一次（exactly-once）对比至少一次（at-least-once）*：你可以选择向 `enableCheckpointing(n)` 方法中传入一个模式来选择使用两种保证等级中的哪一种。
    对于大多数应用来说，精确一次是较好的选择。至少一次可能与某些延迟超低（始终只有几毫秒）的应用的关联较大。

  - *checkpoint 超时*：如果 checkpoint 执行的时间超过了该配置的阈值，还在进行中的 checkpoint 操作就会被取消。

  - *checkpoints 之间的最小时间*：该属性定义在 checkpoint 之间需要多久的时间，以确保流应用在 checkpoint 之间有足够的进展。如果值设置为了 *5000*，
    无论 checkpoint 持续时间与间隔是多久，在前一个 checkpoint 完成至少五秒后才会开始下一个 checkpoint。

    往往使用“checkpoints 之间的最小时间”来配置应用会比 checkpoint 间隔容易很多，因为 "checkpoints 之间的最小时间" 在 checkpoint 的执行时间超过平均值时不会受到影响（例如如果目标的存储系统忽然变得很慢）。

    注意这个值也意味着并发 checkpoint 的数目是*一*。

  - *checkpoint 可容忍连续失败次数*：该属性定义可容忍多少次连续的 checkpoint 失败。超过这个阈值之后会触发作业错误 fail over。
    默认次数为“0”，这意味着不容忍 checkpoint 失败，作业将在第一次 checkpoint 失败时 fail over。

  - *并发 checkpoint 的数目*：默认情况下，在上一个 checkpoint 未完成（失败或者成功）的情况下，系统不会触发另一个 checkpoint。这确保了拓扑不会在 checkpoint 上花费太多时间，从而影响正常的处理流程。
    不过允许多个 checkpoint 并行进行是可行的，对于有确定的处理延迟（例如某方法所调用比较耗时的外部服务），但是仍然想进行频繁的 checkpoint 去最小化故障后重跑的 pipelines 来说，是有意义的。

    该选项不能和 "checkpoints 之间的最小时间"同时使用。

  - *externalized checkpoints*：你可以配置周期存储 checkpoint 到外部系统中。Externalized checkpoints 将他们的元数据写到持久化存储上并且在 job 失败的时候*不会*被自动删除。
    这种方式下，如果你的 job 失败，你将会有一个现有的 checkpoint 去恢复。更多的细节请看 [Externalized checkpoints 的部署文档]({{< ref "docs/ops/state/checkpoints" >}}#externalized-checkpoints)。

  - *非对齐（unaligned）checkpoints*：用户可以启用 [unaligned checkpoints]({{< ref "docs/ops/state/checkpointing_under_backpressure" >}}) 以显著减少反压下的 checkpoint 执行时间。这仅适用于 exactly-once 且并发 checkpoint 数为 1 的情况。

  - *有界流 Checkpoint*：该特性支持 Flink 应用的 DAG 中部分任务已经完成后继续执行 checkpoints。在启用该特性之前，请阅读[重要注意事项](#checkpointing-with-parts-of-the-graph-finished)。

{{< tabs "4b9c6a74-8a45-4ad2-9e80-52fe44a85991" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 每 1000ms 开始一次 checkpoint
env.enableCheckpointing(1000);

// 高级选项：

// 设置模式为精确一次 (这是默认值)
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 确认 checkpoints 之间的时间会进行 500 ms
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// Checkpoint 必须在一分钟内完成，否则就会被抛弃
env.getCheckpointConfig().setCheckpointTimeout(60000);

// 最多允许两个连续的 checkpoint 失败
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

// 同一时间只允许一个 checkpoint 处于执行中状态
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// 启用 externalized checkpoints，从而在作业取消后仍会保留 checkpoint 数据
env.getCheckpointConfig().enableExternalizedCheckpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 启用非对齐 checkpoint
env.getCheckpointConfig().enableUnalignedCheckpoints();

// 设置保存 checkpoint 快照数据的 checkpoint storage
env.getCheckpointConfig().setCheckpointStorage("hdfs:///my/checkpoint/dir")

// 启用有界流 checkpoint 机制
Configuration config = new Configuration();
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
env.configure(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// 每 1000ms 开始一次 checkpoint
env.enableCheckpointing(1000)

// 高级选项：

// 设置模式为精确一次 (这是默认值)
env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// 确认 checkpoints 之间的时间会进行 500 ms
env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

// Checkpoint 必须在一分钟内完成，否则就会被抛弃
env.getCheckpointConfig.setCheckpointTimeout(60000)

// 最多允许两个连续的 checkpoint 失败
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2)

// 同一时间只允许一个 checkpoint 处于执行中状态
env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

// 启用 externalized checkpoints，从而在作业取消后仍会保留 checkpoint 数据
env.getCheckpointConfig().enableExternalizedCheckpoints(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

// 启用非对齐 checkpoint
env.getCheckpointConfig.enableUnalignedCheckpoints()

// 设置保存 checkpoint 快照数据的 checkpoint storage
env.getCheckpointConfig.setCheckpointStorage("hdfs:///my/checkpoint/dir")

// 启用有界流 checkpoint 机制
val config = new Configuration()
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true)
env.configure(config)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

# 每 1000ms 开始一次 checkpoint
env.enable_checkpointing(1000)

# 高级选项：

# 设置模式为精确一次 (这是默认值)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# 确认 checkpoints 之间的时间会进行 500 ms
env.get_checkpoint_config().set_min_pause_between_checkpoints(500)

# Checkpoint 必须在一分钟内完成，否则就会被抛弃
env.get_checkpoint_config().set_checkpoint_timeout(60000)

# 最多允许两个连续的 checkpoint 失败
env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)

# 同一时间只允许一个 checkpoint 处于执行中状态
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

# 启用 externalized checkpoints，从而在作业取消后仍会保留 checkpoint 数据
env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

# 启用非对齐 checkpoint
env.get_checkpoint_config().enable_unaligned_checkpoints()
```
{{< /tab >}}
{{< /tabs >}}

### 相关的配置选项

更多的属性与默认值能在 `conf/flink-conf.yaml` 中设置（完整教程请阅读 [配置]({{< ref "docs/deployment/config" >}})）。

{{< generated/checkpointing_configuration >}}

{{< top >}}


## 选择 Checkpoint Storage

Flink 的 [checkpointing 机制]({{< ref "docs/learn-flink/fault_tolerance" >}}) 会将 timer 以及 stateful 的 operator 进行快照，然后存储下来，
包括连接器（connectors），窗口（windows）以及任何用户[自定义的状态]({{< ref "docs/dev/datastream/fault-tolerance/state" >}})。
Checkpoint 存储在哪里取决于所配置的 **State Backend**（比如 JobManager memory、 file system、 database）。

默认情况下，checkpoint 保存在 JobManager 的内存中。为了合适地持久化大体量状态，
Flink支持各种各样的途径去存储 checkpoint 状态到其他的位置。通过 `StreamExecutionEnvironment.getCheckpointConfig().setCheckpointStorage(…)` 来配置所选的 checkpoint storage。
强烈建议在生产环境下将 checkpoint 存储在高可用的文件系统中。

参考 [checkpoint storage]({{< ref "docs/ops/state/checkpoints#checkpoint-storage" >}}) 以获取有关作业级别和集群级别的可用配置的更多详细信息。

## 迭代作业中的状态和 checkpoint

Flink 现在为没有迭代（iterations）的作业提供一致性的处理保证。在迭代作业上开启 checkpoint 会导致异常。为了在迭代程序中强制进行 checkpoint，用户需要在开启 checkpoint 时设置一个特殊的标志： `env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force = true)`。

请注意在环形边上游走的记录（以及与之相关的状态变化）在故障时会丢失。

## 部分任务结束后的 Checkpoint

从版本 1.14 开始 Flink 支持在部分任务结束后继续进行Checkpoint。
如果一部分数据源是有限数据集，那么就可以出现这种情况。
从版本 1.15 开始，这一特性被默认打开。如果想要关闭这一功能，可以执行：

```java
Configuration config = new Configuration();
config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```

在这种情况下，结束的任务不会参与 Checkpoint 的过程。在实现自定义的算子或者 UDF （用户自定义函数）时需要考虑这一点。

为了支持部分任务结束后的 Checkpoint 操作，我们调整了 [任务的生命周期]({{<ref "docs/internals/task_lifecycle" >}}) 并且引入了
{{< javadoc file="org/apache/flink/streaming/api/operators/StreamOperator.html#finish--" name="StreamOperator#finish" >}} 方法。
在这一方法中，用户需要写出所有缓冲区中的数据。在 finish 方法调用后的 checkpoint 中，这一任务一定不能再有缓冲区中的数据，因为在 `finish()` 后没有办法来输出这些数据。
在大部分情况下，`finish()` 后这一任务的状态为空，唯一的例外是如果其中某些算子中包含外部系统事务的句柄（例如为了实现恰好一次语义），
在这种情况下，在 `finish()` 后进行的 checkpoint 操作应该保留这些句柄，并且在结束 checkpoint（即任务退出前所等待的 checkpoint）时提交。
一个可以参考的例子是满足恰好一次语义的 sink 接口与 `TwoPhaseCommitSinkFunction`。

### 对 operator state 的影响

在部分 Task 结束后的checkpoint中，Flink 对 `UnionListState` 进行了特殊的处理。
`UnionListState` 一般用于实现对外部系统读取位置的一个全局视图（例如，用于记录所有 Kafka 分区的读取偏移）。
如果我们在算子的某个并发调用 `close()` 方法后丢弃它的状态，我们就会丢失它所分配的分区的偏移量信息。
为了解决这一问题，对于使用 `UnionListState` 的算子我们只允许在它的并发都在运行或都已结束的时候才能进行 checkpoint 操作。

`ListState` 一般不会用于类似的场景，但是用户仍然需要注意在调用 `close()` 方法后进行的 checkpoint 会丢弃算子的状态并且
这些状态在算子重启后不可用。

任何支持并发修改操作的算子也可以支持部分并发实例结束后的恢复操作。从这种类型的快照中恢复等价于将算子的并发改为正在运行的并发实例数。

### 任务结束前等待最后一次 Checkpoint

为了保证使用两阶段提交的算子可以提交所有的数据，任务会在所有算子都调用 `finish()` 方法后等待下一次 checkpoint 成功后退出。
需要注意的是，这一行为可能会延长任务运行的时间，如果 checkpoint 周期比较大，这一延迟会非常明显。
极端情况下，如果 checkpoint 的周期被设置为 `Long.MAX_VALUE`，那么任务永远不会结束，因为下一次 checkpoint 不会进行。

{{< top >}}

