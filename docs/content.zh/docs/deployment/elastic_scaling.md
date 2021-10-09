---
title: 弹性扩缩容
weight: 5
type: docs

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

# 弹性扩缩容

在 Apache Flink 中，可以通过手动停止 Job，然后从停止时创建的 Savepoint 恢复，最后重新指定并行度的方式来重新扩缩容 Job。

这个文档描述了那些可以使 Flink 自动调整并行度的选项。

## Reactive 模式

{{< hint info >}}
Reactive 模式是一个 MVP （minimum viable product，最小可行产品）特性。目前 Flink 社区正在积极地从邮件列表中获取用户的使用反馈。请注意文中列举的一些局限性。
{{< /hint >}}

在 Reactive 模式下，Job 会使用集群中所有的资源。当增加 TaskManager 时，Job 会自动扩容。当删除时，就会自动缩容。Flink 会管理 Job 的并行度，始终会尽可能地使用最大值。

当发生扩缩容时，Job 会被重启，并且会从最新的 Checkpoint 中恢复。这就意味着不需要花费额外的开销去创建 Savepoint。当然，所需要重新处理的数据量取决于 Checkpoint 的间隔时长，而恢复的时间取决于状态的大小。

借助 Reactive 模式，Flink 用户可以通过一些外部的监控服务产生的指标，例如：消费延迟、CPU 利用率汇总、吞吐量、延迟等，实现一个强大的自动扩缩容机制。当上述的这些指标超出或者低于一定的阈值时，增加或者减少 TaskManager 的数量。在 Kubernetes 中，可以通过改变 Deployment 的[副本数（Replica Factor）](https://kubernetes.io/zh/docs/concepts/workloads/controllers/deployment/#replicas) 实现。而在 AWS 中，可以通过改变 [Auto Scaling 组](https://docs.aws.amazon.com/zh_cn/autoscaling/ec2/userguide/AutoScalingGroup.html) 来实现。这类外部服务只需要负责资源的分配以及回收，而 Flink 则负责在这些资源上运行 Job。

<a name="getting-started"></a>

### 入门

你可以参考下面的步骤试用 Reactive 模式。以下步骤假设你使用的是单台机器部署 Flink。

```bash

# 以下步骤假设你当前目录处于 Flink 发行版的根目录。

# 将 Job 拷贝到 lib/ 目录下
cp ./examples/streaming/TopSpeedWindowing.jar lib/
# 使用 Reactive 模式提交 Job
./bin/standalone-job.sh start -Dscheduler-mode=reactive -Dexecution.checkpointing.interval="10s" -j org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
# 启动第一个 TaskManager
./bin/taskmanager.sh start
```

让我们快速解释下上面每一条执行的命令：
- `./bin/standalone-job.sh start` 使用 [Application 模式]({{< ref "docs/deployment/overview" >}}#application-mode) 部署 Flink。
- `-Dscheduler-mode=reactive` 启动 Reactive 模式。
- `-Dexecution.checkpointing.interval="10s"` 配置 Checkpoint 和重启策略。
- 最后一个参数是 Job 的主函数名。

你现在已经启动了一个 Reactive 模式下的 Flink Job。在[Web 界面](http://localhost:8081)上，你可以看到 Job 运行在一个 TaskManager 上。如果你想要扩容，可以再添加一个 TaskManager，
```bash
# 额外启动一个 TaskManager
./bin/taskmanager.sh start
```

如果想要缩容，可以关掉一个 TaskManager。
```bash
# 关闭 TaskManager
./bin/taskmanager.sh stop
```

### 用法

#### 配置

通过将 `scheduler-mode` 配置成 `reactive`，你可以开启 Reactive 模式。

**每个独立算子的并行度都将由调度器来决定**，而不是由配置决定。当并行度在算子上或者整个 Job 上被显式设置时，这些值被会忽略。

而唯一能影响并行度的方式只有通过设置算子的最大并行度（调度器不会忽略这个值）。
最大并行度 maxParallelism 参数的值最大不能超过 2^15（32768）。如果你们没有给算子或者整个 Job 设置最大并行度，会采用[默认的最大并行度规则]({{< ref "docs/dev/datastream/execution/parallel" >}}#setting-the-maximum-parallelism)。
这个值很有可能会低于它的最大上限。当使用默认的调度模式时，请参考[并行度的最佳实践]({{< ref "docs/ops/production_ready" >}}#set-an-explicit-max-parallelism)。

需要注意的是，过大的并行度会影响 Job 的性能，因为 Flink 为此需要维护更多的[内部结构](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html)。

当开启 Reactive 模式时，[`jobmanager.adaptive-scheduler.resource-wait-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-wait-timeout) 配置的默认值是 `-1`。这意味着，JobManager 会一直等待，直到拥有足够的资源。
如果你想要 JobManager 在没有拿到足够的 TaskManager 的一段时间后关闭，可以配置这个参数。

当开启 Reactive 模式时，[`jobmanager.adaptive-scheduler.resource-stabilization-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-stabilization-timeout) 配置的默认值是 `0`：Flink 只要有足够的资源，就会启动 Job。
在 TaskManager 一个一个而不是同时启动的情况下，会造成 Job 在每一个 TaskManager 启动时重启一次。当你希望等待资源稳定后再启动 Job，那么可以增加这个配置的值。
另外，你还可以配置 [`jobmanager.adaptive-scheduler.min-parallelism-increase`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-min-parallelism-increase)：这个配置能够指定在扩容前需要满足的最小额外增加的并行总数。例如，你的 Job 由并行度为 2 的 Source 和并行度为 2 的 Sink组成，并行总数为 4。这个配置的默认值是 `1`，所以任意并行总数的增加都会导致重启。

#### 建议

- **为有状态的 Job 配置周期性的 Checkpoint**：Reactive 模式在扩缩容时通过最新完成的 Checkpoint 恢复。如果没有配置周期性的 Checkpoint，你的程序会丢失状态。Checkpoint 同时还配置了**重启策略**，Reactive会使用配置的重启策略：如果没有设置，Reactive 模式会让 Job 失败而不是运行扩缩容。

- 在 Ractive 模式下缩容可能会导致长时间的停顿，因为 Flink 需要等待 JobManager 和已经停止的 TaskManager 间心跳超时。当你降低 Job 并行度时，你会发现 Job 会停顿大约 50 秒左右。
  
  这是由于默认的心跳超时时间是 50 秒。在你的基础设施允许的情况下，可以降低 [`heartbeat.timeout`]({{< ref "docs/deployment/config">}}#heartbeat-timeout) 的值。但是降低超时时间，会导致比如在网络拥堵或者 GC Pause 的时候，TaskManager 无法响应心跳。需要注意的是，[`heartbeat.interval`]({{< ref "docs/deployment/config">}}#heartbeat-interval) 配置需要低于超时时间。

### 局限性

由于 Reactive 模式是一个新的实验特性，并不是所有在默认调度器下的功能都能支持（也包括 Adaptive 调度器）。Flink 社区正在解决这些局限性。

- **仅支持 Standalone 部署模式**。其他主动的部署模式实现（例如：原生的 Kubernetes 以及 YARN）都明确不支持。Session 模式也同样不支持。仅支持单 Job 的部署。

  仅支持如下的部署方式：[Application 模式下的 Standalone 部署]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}#application-mode)（可以参考[上文](#getting-started)）、[Application 模式下的 Docker 部署]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#application-mode-on-docker) 以及 [Standalone 的 Kubernetes Application 集群模式]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#deploy-application-cluster)。

[Adaptive 调度器的局限性](#limitations-1) 同样也适用于 Reactive 模式.

## Adaptive 调度器

{{< hint warning >}}
只推荐高级用户直接使用 Adaptive 调度器（而不是通过 Reactive 模式使用），因为在一个 Session 集群中对于多个 Job 的 Slot 的分配行为是不确定的。
{{< /hint >}}

Adaptive 调度器可以基于现有的 Slot 调整 Job 的并行度。它会在 Slot 数目不足时，自动减少并行度。这种情况包括在提交时资源不够，或者在 Job 运行时 TaskManager 不可用。当有新的 Slot 加入时，Job 将会自动扩容至配置的并行度。
在 Reactive 模式下（详见上文），并行度配置会被忽略，即无限大，使得 Job 尽可能地使用资源。
你也可以不使用 Reactive 模式而仅使用 Adaptive 调度器，但这种情况会有如下的局限性：
- 如果你在 Session 集群上使用 Adaptive 调度器，在这个集群中运行的多个 Job，他们间 Slot 的分布是无法保证的。

相比默认的调度器，Adaptive 调度器其中一个优势在于，它能够优雅地处理 TaskManager 丢失所造成的问题，因为对它来说就仅仅是缩容。

### 用法

需要设置如下的配置参数：

- `jobmanager.scheduler: adaptive`：将默认的调度器换成 Adaptive。
- `cluster.declarative-resource-management.enabled`：声明式资源管理必须开启（默认开启）。

Adaptive 调度器可以通过[所有在名字包含 `adaptive-scheduler` 的配置]({{< ref "docs/deployment/config">}}#advanced-scheduling-options)修改其行为。

<a name="limitations-1"></a>

### 局限性

- **只支持流式 Job**：Adaptive 调度器的第一个版本仅支持流式 Job。当提交的是一个批处理 Job 时，我们会自动换回默认调度器。
- **不支持[本地恢复]({{< ref "docs/ops/state/large_state_tuning">}}#task-local-recovery)**：本地恢复是将 Task 调度到状态尽可能的被重用的机器上的功能。不支持这个功能意味着 Adaptive 调度器需要每次从 Checkpoint 的存储中下载整个 State。
- **不支持部分故障恢复**: 部分故障恢复意味着调度器可以只重启失败 Job 其中某一部分（在 Flink 的内部结构中被称之为 Region）而不是重启整个 Job。这个限制只会影响那些独立并行（Embarrassingly Parallel）Job的恢复时长，默认的调度器可以重启失败的部分，然而 Adaptive 将需要重启整个 Job。
- **与 Flink Web UI 的集成受限**: Adaptive 调度器会在 Job 的生命周期中改变它的并行度。Web UI 上只显示 Job 当前的并行度。
- **Job 的指标受限**: 除了 `numRestarts` 外，`Job` 作用域下所有的 [可用性]({{< ref "docs/ops/metrics" >}}#availability) 和 [Checkpoint]({{< ref "docs/ops/metrics" >}}#checkpointing) 指标都不准确。
- **空闲 Slot**: 如果 Slot 共享组的最大并行度不相等，提供给 Adaptive 调度器所使用的的 Slot 可能不会被使用。
- 扩缩容事件会触发 Job 和 Task 重启，Task 重试的次数也会增加。


{{< top >}}
