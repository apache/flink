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

Historically, the parallelism of a job has been static throughout its lifecycle and defined once during its submission. Batch jobs couldn't be rescaled at all, while Streaming jobs could have been stopped with a savepoint and restarted with a different parallelism.

This page describes a new class of schedulers that allow Flink to adjust job's parallelism at runtime, which pushes Flink one step closer to a truly cloud-native stream processor. The new schedulers are [Adaptive Scheduler](#adaptive-scheduler) (streaming) and [Adaptive Batch Scheduler](#adaptive-batch-scheduler) (batch).

## Adaptive 调度器

The Adaptive Scheduler can adjust the parallelism of a job based on available slots. It will automatically reduce the parallelism if not enough slots are available to run the job with the originally configured parallelism; be it due to not enough resources being available at the time of submission, or TaskManager outages during the job execution. If new slots become available the job will be scaled up again, up to the configured parallelism.

In Reactive Mode (see below) the configured parallelism is ignored and treated as if it was set to infinity, letting the job always use as many resources as possible.

One benefit of the Adaptive Scheduler over the default scheduler is that it can handle TaskManager losses gracefully, since it would just scale down in these cases.

{{< img src="/fig/adaptive_scheduler.png" >}}

Adaptive Scheduler builds on top of a feature called [Declarative Resource Management](https://cwiki.apache.org/confluence/display/FLINK/FLIP-138%3A+Declarative+Resource+management). As you can see, instead of asking for the exact number of slots, JobMaster declares its desired resources (for reactive mode the maximum is set to infinity) to the ResourceManager, which then tries to fulfill those resources.

{{< img src="/fig/adaptive_scheduler_rescale.png" >}}

当JobMaster在运行时获得更多资源时，它将使用最新的可用保存点（Savepoint）自动重新调整作业的运行规模，从而无需外部进行干预和编排这些作业。

从**Flink 1.18.x**开始， 您可以使用[外部声明式资源管理](#外部声明式资源管理), 重新定义正在运行的作业的资源需求，否则自适应调度器（Adaptive Scheduler）将无法处理因输入速率变化或工作负载性能变化而需要重新调整作业规模的情况。

### 外部声明式资源管理

{{< hint warning >}}
外部化声明性资源管理是一项 MVP（“最小可行产品”）功能。 Flink 社区正在通过我们的邮件列表积极寻求用户的反馈，在使用此功能前请检查本页功能描述相关的限制。
{{< /hint >}}

{{< hint info >}}
您可以将此功能与[Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.6/docs/custom-resource/autoscaler/#flink-118-and-in-place-scaling-support) 集成使用，以获得更佳的作业自动伸缩能力。
{{< /hint >}}

外部化的声明式资源管理旨在解决两种部署场景：
1. 在会话集群上（Session Cluster）的自适应调度器，其中多个作业可以争夺资源，您需要更细粒度地控制作业之间资源的分配。
2. 在应用集群上（Application Cluster）(例如[原生 Kubernetes]({{<ref"docs/deployment/resource-providers/native_kubernetes" >}}))的自适应调度器上（Active Resource Manager），您依赖于Flink来“贪婪”地生成新的TaskManagers去满足你的资源需求，但是您仍然希望像Reactive 模式[Reactive 模式](#Reactive 模式).那样利用重新集群规模的能力。

通过引入一个新的 [REST API 入口]({{< ref "docs/ops/rest_api" >}}#jobs-jobid-resource-requirements-1), 它允许您重新声明正在运行的作业的并行度资源需求，通过设置每个顶点的并行度的最大值和最小值边界。

```
PUT /jobs/<job-id>/resource-requirements
 
REQUEST BODY:
{
    "<first-vertex-id>": {
        "parallelism": {
            "lowerBound": 3,
            "upperBound": 5
        }
    },
    "<second-vertex-id>": {
        "parallelism": {
            "lowerBound": 2,
            "upperBound": 3
        }
    }
}
```

在某种程度上，上述端点可以被视为“重新调整作业并行度的入口”，它引入了为Flink构建自动扩缩体验的一个重要构建模块。

您可以手动尝试此功能，通过在Flink UI中的作业概览，并在任务列表中使用扩容/缩容按钮调整作业并行度。

### Usage

{{< hint info >}}
如果您在[session cluster]({{< ref "docs/deployment/overview" >}}/#session-mode)上使用自适应调度器, 当集群中没有足够的资源时，同一Session中运行的多个作业之间的插槽分配没有保证。[外部声明式资源管理](#外部声明式资源管理) 可以部分地缓解这个问题，但仍然建议在[application cluster]({{< ref "docs/deployment/overview" >}}/#application-mode)上使用自适应调度器.
{{< /hint >}}

**jobmanager.scheduler** 配置需要在集群级别上设置为自适应调度器（Adaptive Scheduler），以代替默认调度器。

```yaml
jobmanager.scheduler: adaptive
```

自适应调度器的行为由名称中带有前缀为[`jobmanager.adaptive-scheduler`]({{< ref "docs/deployment/config">}}#advanced-scheduling-options) 进行配置。

### 使用限制

- **仅仅支持流式作业**: 自适应调度程序仅与流作业一起运行。当提交批处理作业时，Flink 将使用批处理作业的默认调度程序，即 [Adaptive Batch Scheduler](#adaptive-batch-scheduler)
- **不支持部分故障转移**: 部分故障转移意味着调度器能够重新启动失败作业的部分（在Flink的内部称为“区域”），而不是整个作业。这个限制只影响那些简单并行作业的恢复时间：Flink的默认调度器可以重新启动失败的部分，而自适应调度器会重新启动整个作业。
扩展事件会触发作业和任务的重新启动，这将增加任务尝试的次数。

## Reactive 模式

Reactive Mode is a special mode for Adaptive Scheduler, that assumes a single job per-cluster (enforced by the [Application Mode]({{< ref "docs/deployment/overview" >}}#application-mode)). Reactive Mode configures a job so that it always uses all resources available in the cluster. Adding a TaskManager will scale up your job, removing resources will scale it down. Flink will manage the parallelism of the job, always setting it to the highest possible values.

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

## Adaptive Batch Scheduler

Adaptive Batch Scheduler 是一种可以自动调整执行计划的批作业调度器。它目前支持自动推导算子并行度，如果算子未设置并行度，调度器将根据其消费的数据量的大小来推导其并行度。这可以带来诸多好处：
- 批作业用户可以从并行度调优中解脱出来
- 根据数据量自动推导并行度可以更好地适应每天变化的数据量
- SQL作业中的算子也可以分配不同的并行度

当前 Adaptive Batch Scheduler 是 Flink 默认的批作业调度器，无需额外配置。除非用户显式的配置了使用其他调度器，例如 `jobmanager.scheduler: default`。需要注意的是，由于 ["只支持所有数据交换都为 BLOCKING 或 HYBRID 模式的作业"](#局限性-2), 需要将 [`execution.batch-shuffle-mode`]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) 配置为 `ALL_EXCHANGES_BLOCKING`(默认值) 或 `ALL_EXCHANGES_HYBRID_FULL` 或 `ALL_EXCHANGES_HYBRID_SELECTIVE`。

### 自动推导并发度

#### 用法

使用 Adaptive Batch Scheduler 自动推导算子的并行度，需要：
- 启用自动并行度推导：

  Adaptive Batch Scheduler 默认启用了自动并行度推导，你可以通过配置 [`execution.batch.adaptive.auto-parallelism.enabled`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-enabled) 来开关此功能。
  除此之外，你也可以根据作业的情况调整以下配置:
  - [`execution.batch.adaptive.auto-parallelism.min-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-min-parallelism): 允许自动设置的并行度最小值。
  - [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism): 允许自动设置的并行度最大值，如果该配置项没有配置将使用通过 [`parallelism.default`]({{< ref "docs/deployment/config" >}}) 或者 `StreamExecutionEnvironment#setParallelism()` 设置的默认并行度作为允许自动设置的并行度最大值。
  - [`execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-avg-data-volume-per-ta): 期望每个任务平均处理的数据量大小。请注意，当出现数据倾斜，或者确定的并行度达到最大并行度（由于数据过多）时，一些任务实际处理的数据可能会远远超过这个值。
  - [`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle): source 算子可动态推导的最大并行度，若该配置项没有配置将优先使用 [`execution-batch-adaptive-auto-parallelism-max-parallelism`]({{< ref "docs/deployment/config" >}})作为允许动态推导的并行度最大值，若该配置项也没有配置，将使用 [`parallelism.default`]({{< ref "docs/deployment/config" >}}) 或者 `StreamExecutionEnvironment#setParallelism()` 设置的默认并行度。
- 不要指定算子的并行度：

    Adaptive Batch Scheduler 只会为用户未指定并行度的算子推导并行度。 所以如果你想算子的并行度被自动推导，需要避免通过算子的 `setParallelism()` 方法来为其指定并行度。

    除此之外，对于 DataSet 作业还需要进行以下配置：
  - 配置 `parallelism.default: -1`
  - 不要通过 `ExecutionEnvironment` 的 `setParallelism()` 方法来指定并行度

#### 让 Source 支持动态并行度推导
如果你的作业有用到自定义 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="Source" >}},
你需要让 Source 实现接口 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/DynamicParallelismInference.java" name="DynamicParallelismInference" >}}。
```java
public interface DynamicParallelismInference {
    int inferParallelism(Context context);
}
```
其中 Context 会提供可推导并行度上界、期望每个任务平均处理的数据量大小、动态过滤信息来协助并行度推导。
Adaptive Batch Scheduler 将会在调度 Source 节点之前调用上述接口，需注意实现中应尽量避免高耗时的操作。

若 Source 未实现上述接口，[`execution.batch.adaptive.auto-parallelism.default-source-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-default-source-paralle) 将会作为 Source 节点的并行度。

需注意，Source 动态并行度推导也只会为用户未指定并行度的 Source 算子推导并行度。

#### 性能调优

1. 建议使用 [Sort Shuffle](https://flink.apache.org/2021/10/26/sort-shuffle-part1.html) 并且设置 [`taskmanager.network.memory.buffers-per-channel`]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) 为 `0`。 这会解耦并行度与需要的网络内存，对于大规模作业，这样可以降低遇到 "Insufficient number of network buffers" 错误的可能性。
2. 建议将 [`execution.batch.adaptive.auto-parallelism.max-parallelism`]({{< ref "docs/deployment/config" >}}#execution-batch-adaptive-auto-parallelism-max-parallelism) 设置为最坏情况下预期需要的并行度。不建议配置太大的值，否则可能会影响性能。这个配置项会影响上游任务产出的 subpartition 的数量，过多的 subpartition 可能会影响 hash shuffle 的性能，或者由于小包影响网络传输的性能。

### 局限性
- **只支持批作业**: Adaptive Batch Scheduler 只支持批作业。当提交的是一个流作业时，会抛出异常。
- **只支持所有数据交换都为 BLOCKING 或 HYBRID 模式的作业**: 目前 Adaptive Batch Scheduler 只支持 [shuffle mode]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) 为 ALL_EXCHANGES_BLOCKING 或 ALL_EXCHANGES_HYBRID_FULL 或 ALL_EXCHANGES_HYBRID_SELECTIVE 的作业。请注意，使用 DataSet API 的作业无法识别上述 shuffle 模式，需要将 ExecutionMode 设置为 BATCH_FORCED 才能强制启用 BLOCKING shuffle。
- **不支持 FileInputFormat 类型的 source**: 不支持 FileInputFormat 类型的 source, 包括 `StreamExecutionEnvironment#readFile(...)` `StreamExecutionEnvironment#readTextFile(...)` 和 `StreamExecutionEnvironment#createInput(FileInputFormat, ...)`。 当使用 Adaptive Batch Scheduler 时，用户应该使用新版的 Source API ([FileSystem DataStream Connector]({{< ref "docs/connectors/datastream/filesystem.md" >}}) 或 [FileSystem SQL Connector]({{< ref "docs/connectors/table/filesystem.md" >}})) 来读取文件.
- **Web UI 上展示的上游输出的数据量和下游收到的数据量可能不一致**: 在使用 Adaptive Batch Scheduler 自动推导并行度时，对于 broadcast 边，上游算子发送的数据量和下游算子接收的数据量可能会不相等，这在 Web UI 的显示上可能会困扰用户。细节详见 [FLIP-187](https://cwiki.apache.org/confluence/display/FLINK/FLIP-187%3A+Adaptive+Batch+Job+Scheduler)。

{{< top >}}
