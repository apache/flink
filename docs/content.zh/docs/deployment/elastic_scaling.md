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

过去，作业的并行度在提交时确定，并在整个生命周期中保持不变。批作业无法进行扩缩容，而流作业可以通过制作 Savepoint 后停止，再以不同的并行度重新启动。

本页介绍一类允许 Flink 在运行时调整作业并行度的调度器，使 Flink 向真正的云原生流处理系统更进一步。这类调度器包括用于流作业的 [Adaptive 调度器](#adaptive-scheduler)和用于批作业的 [Adaptive 批调度器]({{< ref "docs/deployment/adaptive_batch" >}})。

<a name="adaptive-scheduler"></a>

## Adaptive 调度器

Adaptive 调度器可以根据可用的 Slot 调整作业并行度。如果可用的 Slot 不足以按原先配置的并行度运行作业，无论是由于提交时资源不足，还是作业运行期间 TaskManager 发生故障，调度器都会自动降低并行度。当有新的 Slot 可用时，作业会再次扩容，最高达到配置的并行度。

在下文介绍的 Reactive 模式下，配置的并行度会被忽略，视为无穷大，使作业始终尽可能多地利用可用资源。

与默认调度器相比，Adaptive 调度器的一个优势是能够通过缩容平稳应对 TaskManager 丢失的情况。

{{< img src="/fig/adaptive_scheduler.png" >}}

Adaptive 调度器基于[声明式资源管理](https://cwiki.apache.org/confluence/display/FLINK/FLIP-138%3A+Declarative+Resource+management)构建。如图所示，JobMaster 不再请求确切数量的 Slot，而是向 ResourceManager 声明期望的资源量（在 Reactive 模式下，上限设为无穷大），由 ResourceManager 尝试满足这些资源需求。

{{< img src="/fig/adaptive_scheduler_rescale.png" >}}

当 JobMaster 在运行期间获得更多资源时，会使用最新可用的 Savepoint 自动对作业进行扩缩容，无需外部编排。

从 **Flink 1.18.x** 开始，你可以通过[外部化声明式资源管理](#externalized-declarative-resource-management)重新声明运行中作业的资源需求。否则当输入速率或工作负载性能发生变化，需要对作业进行扩缩容时，Adaptive 调度器无法处理这类情况。

<a name="externalized-declarative-resource-management"></a>

### 外部化声明式资源管理

{{< hint warning >}}
外部化声明式资源管理目前是一个 MVP（最小可行产品）特性。Flink 社区欢迎用户通过邮件列表提供反馈。使用前请查看本页列出的局限性。
{{< /hint >}}

{{< hint info >}}
你可以将外部化声明式资源管理与 [Apache Flink Kubernetes operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/managing/autoscaler/) 结合使用，实现完整的自动扩缩容功能。
{{< /hint >}}

外部化声明式资源管理旨在支持以下两种部署场景：
1. 在 Session 集群上使用 Adaptive 调度器。多个作业可能竞争资源，因此需要更精细地控制作业之间的资源分配。
2. 在 Application 集群上，将 Adaptive 调度器与主动资源管理器结合使用（例如[Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})）。在这种场景下，你依赖 Flink 以“贪婪”的方式创建新的 TaskManager，同时仍希望使用类似 [Reactive 模式](#reactive-mode)的扩缩容能力。

为此，该特性引入了一个新的 [REST API 端点]({{< ref "docs/ops/rest_api" >}}#jobs-jobid-resource-requirements-1)，允许你通过设置各个作业顶点的并行度上下限，重新声明运行中作业的资源需求。

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

在一定程度上，上述端点可以视为一个“扩缩容端点”，是为 Flink 构建自动扩缩容功能的重要基础。

你可以在 Flink Web 界面的作业概览中，使用任务列表里的扩容和缩容按钮，手动试用此功能。

<a name="usage"></a>

### 使用方法

{{< hint info >}}
如果你在 [Session 集群]({{< ref "docs/deployment/overview" >}}#session-mode)上使用 Adaptive 调度器，当集群资源不足时，无法保证同一 Session 中多个运行作业之间的 Slot 分配。[外部化声明式资源管理](#externalized-declarative-resource-management)可以在一定程度上缓解这一问题，但仍建议在 [Application 集群]({{< ref "docs/deployment/overview" >}}#application-mode)上使用 Adaptive 调度器。
{{< /hint >}}

要使用 Adaptive 调度器替代默认调度器，需要在集群级别将 `jobmanager.scheduler` 设置为 `adaptive`。

```yaml
jobmanager.scheduler: adaptive
```

Adaptive 调度器的行为由[名称以 `jobmanager.adaptive-scheduler` 为前缀的配置项]({{< ref "docs/deployment/config">}}#advanced-scheduling-options)控制。

<a name="limitations"></a>

### 使用限制

- **仅支持流作业**：Adaptive 调度器仅用于流作业。提交批作业时，Flink 会使用批作业的默认调度器，即 [Adaptive 批调度器]({{< ref "docs/deployment/adaptive_batch" >}})。
- **不支持局部故障恢复**：局部故障恢复是指调度器能够重启失败作业的一部分（在 Flink 内部称为“Region”），而不是整个作业。这一限制仅影响易于并行化作业的恢复时间：Flink 默认调度器可以只重启失败的部分，而 Adaptive 调度器会重启整个作业。
- 扩缩容事件会触发作业和任务重启，从而增加任务的执行尝试次数。

<a name="reactive-mode"></a>

## Reactive 模式

Reactive 模式是 Adaptive 调度器的一种特殊模式，假定每个集群只运行一个作业（由 [Application 模式]({{< ref "docs/deployment/overview" >}}#application-mode)保证）。在 Reactive 模式下，作业始终使用集群中所有可用资源。增加 TaskManager 会使作业扩容，减少资源则会使作业缩容。Flink 会管理作业的并行度，始终将其设为当前可达到的最大值。

当发生扩缩容时，Job 会被重启，并且会从最新完成的 Checkpoint 中恢复。这就意味着不需要花费额外的开销去创建 Savepoint（手动扩缩容时则需要创建）。当然，所需要重新处理的数据量取决于 Checkpoint 的间隔时长，而恢复的时间取决于状态的大小。

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
- 最后一个参数是 Job 的主类名。

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

**每个独立算子的并行度都将由调度器来决定**，而不是由配置决定。当并行度在算子上或者整个 Job 上被显式设置时，这些值会被忽略。

而唯一能影响并行度的方式只有通过设置算子的最大并行度（调度器不会忽略这个值）。
最大并行度 maxParallelism 参数的值最大不能超过 2^15（32768）。如果你没有给算子或者整个 Job 设置最大并行度，会采用[默认的最大并行度规则]({{< ref "docs/dev/datastream/execution/parallel" >}}#设置最大并行度)。
这个值很有可能会低于它的最大上限。与默认调度模式一样，使用 Reactive 模式时也应参考[并行度的最佳实践]({{< ref "docs/ops/production_ready" >}}#set-an-explicit-max-parallelism)。

需要注意的是，过大的最大并行度可能会影响 Job 的性能，因为 Flink 为此需要维护更多的[内部结构](https://flink.apache.org/features/2017/07/04/flink-rescalable-state.html)。

当开启 Reactive 模式时，[`jobmanager.adaptive-scheduler.resource-wait-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-wait-timeout) 配置的默认值是 `-1`。这意味着，JobManager 会一直等待，直到拥有足够的资源。
如果你想要 JobManager 在没有拿到足够的 TaskManager 的一段时间后关闭，可以配置这个参数。

当开启 Reactive 模式时，[`jobmanager.adaptive-scheduler.resource-stabilization-timeout`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-resource-stabilization-timeout) 配置的默认值是 `0`：Flink 只要有足够的资源，就会启动 Job。
在 TaskManager 一个一个而不是同时启动的情况下，会造成 Job 在每一个 TaskManager 启动时重启一次。当你希望等待资源稳定后再启动 Job，那么可以增加这个配置的值。
另外，你还可以配置 [`jobmanager.adaptive-scheduler.min-parallelism-increase`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-min-parallelism-increase)：这个配置能够指定在扩容前需要满足的最小额外增加的并行总数。例如，你的 Job 由并行度为 2 的 Source 和并行度为 2 的 Sink 组成，并行总数为 4。这个配置的默认值是 `1`，所以任意并行总数的增加都会导致重启。

你可以通过设置 [`jobmanager.adaptive-scheduler.scaling-interval.max`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-max) 来强制触发扩缩容操作。该配置默认不启用。设置后，当集群新增资源时，即使尚未满足 [`jobmanager.adaptive-scheduler.min-parallelism-increase`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-min-parallelism-increase)，也会在经过 [`jobmanager.adaptive-scheduler.scaling-interval.max`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-max) 指定的时间后安排一次扩缩容。

为避免扩缩容过于频繁，你可以通过 [`jobmanager.adaptive-scheduler.scaling-interval.min`]({{< ref "docs/deployment/config">}}#jobmanager-adaptive-scheduler-scaling-interval-min) 设置两次扩缩容操作之间的最小时间间隔，默认值为 30 秒。

#### 建议

- **为有状态的 Job 配置周期性的 Checkpoint**：Reactive 模式在扩缩容时通过最新完成的 Checkpoint 恢复。如果没有配置周期性的 Checkpoint，你的程序会丢失状态。Checkpoint 同时还配置了**重启策略**，Reactive 模式会使用配置的重启策略：如果没有设置，Reactive 模式会让 Job 失败，而不是执行扩缩容。

- 在 Reactive 模式下，如果 TaskManager 未正常关闭（例如使用了 SIGKILL 而不是 SIGTERM 信号），缩容可能需要更长时间。在这种情况下，Flink 会等待 JobManager 与已停止的 TaskManager 之间的心跳超时。你会看到 Job 停顿大约 50 秒，然后才以更低的并行度重新部署。
  
  这是由于默认的心跳超时时间是 50 秒。在你的基础设施允许的情况下，可以降低 [`heartbeat.timeout`]({{< ref "docs/deployment/config">}}#heartbeat-timeout) 的值。但是，如果 TaskManager 因网络拥堵或长时间 GC 暂停等原因未能响应心跳，过低的心跳超时时间可能导致故障。需要注意的是，[`heartbeat.interval`]({{< ref "docs/deployment/config">}}#heartbeat-interval) 配置需要低于超时时间。

### 局限性

由于 Reactive 模式是一个新的实验特性，并不是所有在默认调度器下的功能都能支持（也包括 Adaptive 调度器）。Flink 社区正在解决这些局限性。

- **仅支持 Standalone 部署模式**。其他主动的部署模式实现（例如：原生的 Kubernetes 以及 YARN）都明确不支持。Session 模式也同样不支持。仅支持单 Job 的部署。

  仅支持如下的部署方式：[Application 模式下的 Standalone 部署]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})（可以参考[上文](#getting-started)）、[Application 模式下的 Docker 部署]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}#application-mode) 以及 [Standalone 的 Kubernetes Application 集群模式]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#deploy-application-cluster)。

[Adaptive 调度器的使用限制](#limitations)同样也适用于 Reactive 模式。

<a name="rescale-history"></a>

## 扩缩容历史

在 Flink 2.3 之前，用户和开发者无法查看 `AdaptiveScheduler` 扩缩容历史的内部细节，这给运维带来了不便。
例如，用户需要了解扩缩容过程中具体的资源变化、并行度调整，以及每次内部状态转换所花费的时间。
这些信息对于调优参数、降低扩缩容延迟和提高稳定性至关重要。

为此，Flink 社区引入了 [FLIP-495](https://cwiki.apache.org/confluence/x/TQr0Ew)，支持记录和存储扩缩容历史，并通过 [FLIP-487](https://cwiki.apache.org/confluence/x/vZCMEw) 支持通过 REST API 查询以及在 Web 界面展示这些历史记录。

对于启用了 `AdaptiveScheduler` 的流作业，你可以将以下配置项设置为正整数来启用扩缩容历史。
该值表示为作业保留的最近扩缩容记录数量。

- [`web.adaptive-scheduler.rescale-history.size`]({{< ref "docs/deployment/config" >}}#web-adaptive-scheduler-rescale-history-size): `4`

该配置项的默认值为 `0`。当配置值小于或等于 `0` 时，此功能将被禁用。

<a name="the-information-and-style-about-rescale-history"></a>

### 扩缩容历史的内容与展示

从 Flink 2.3 开始，Web 界面引入了 `Rescales` 页面，与 `Checkpoints` 页面处于同一层级，并采用类似的展示样式。
它主要包含以下子页面：

- `Overview`  
  此子页面展示处于不同扩缩容终态的最近记录，以及作业扩缩容的基本统计信息，例如作业启动以来的扩缩容总次数、失败次数和成功次数。
  该页面也支持查看扩缩容的详细信息。

- `History`  
  此子页面展示最近扩缩容记录的简要信息，数量不超过 [`web.adaptive-scheduler.rescale-history.size`]({{< ref "docs/deployment/config" >}}#web-adaptive-scheduler-rescale-history-size) 配置的上限。
  该页面也支持查看以下扩缩容详细信息：
    - 一次扩缩容的基本信息
      - <u>Rescale UUID</u>：一次扩缩容的唯一 ID，由 32 个十六进制字符组成（下文中的 UUID 定义与此相同）。
      - <u>Attempt ID</u>：针对相同作业资源需求触发的扩缩容尝试次数。
      - <u>Requirements ID</u>：资源需求的唯一 UUID。
      - <u>Trigger Cause</u>：触发扩缩容的原因。
      - <u>Terminal State</u>：扩缩容的最终状态。
      - <u>Terminated Reason</u>：扩缩容生命周期终止的原因。
      - <u>Start Time</u>：扩缩容开始的时间。
      - <u>Duration</u>：从扩缩容开始到完成所经过的时间；如果尚未完成，则为从开始到当前所经过的时间。
      - <u>End Time</u>：扩缩容已终止时为结束时间，否则为当前时间。
    - 各个 `Job Vertex` 的基本属性与扩缩容变化
      - <u>ID</u>：目标 `Job Vertex` 的唯一 UUID。
      - <u>Name</u>：目标顶点的短名称。
      - <u>Slot Sharing Group ID</u>：目标 `Slot Sharing Group` 的唯一 UUID。
      - <u>Previous Parallelism</u>：目标顶点在本次扩缩容之前的并行度。
      - <u>Acquired Parallelism</u>：目标顶点在本次扩缩容之后的并行度。
      - <u>Sufficient Parallelism</u>：即使无法达到期望并行度，也足以让本次扩缩容继续进行的顶点最小并行度。
      - <u>Desired Parallelism</u>：触发本次扩缩容的初始变更请求中指定的 `Job Vertex` 期望并行度。
    - 各个 `Slot Sharing Group` 的基本属性与扩缩容变化
      - <u>Slot Sharing Group ID</u>：目标 Slot 所属 `Slot Sharing Group` 的 UUID。
      - <u>Slot Sharing Group Name</u>：该 Slot 所属 `Slot Sharing Group` 的名称。
      - <u>Previous Slot</u>：扩缩容之前的 Slot 数量。
      - <u>Acquired Slot</u>：扩缩容之后的 Slot 数量。
      - <u>Desired Slot</u>：本次扩缩容期望的 Slot 数量。
      - <u>Sufficient Slot</u>：本次扩缩容中部署任务所需的最少 Slot 数量。
      - <u>Request Profile</u>：本次扩缩容中 `Slot Sharing Group` 请求的资源规格。
      - <u>Acquired Profile</u>：本次扩缩容中 `Slot Sharing Group` 实际获得的资源规格。
    - 一次扩缩容中 `AdaptiveScheduler` 的内部 `Scheduler State History`（详见 [FLIP-160 中的 AdaptiveScheduler 状态](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173083547#FLIP160:AdaptiveScheduler-Statemachineofthescheduler)）
      - <u>State</u>：调度器的状态名称。
      - <u>Enter Time</u>：进入该状态的时间。
      - <u>Leave Time</u>：离开该状态的时间。
      - <u>Duration</u>：在该状态中停留的时间（Leave Time 减去 Enter Time）。
      - <u>Exception</u>：本次扩缩容在该状态中的异常信息。
- `Summary`  
  此子页面展示作业启动以来的扩缩容事件总数，以及失败和成功次数。
  它还提供扩缩容历史的汇总统计，例如按扩缩容状态分类的耗时统计，包括 `Min`、`Max`、`Avg` 和 `P50` 等指标。
- `Configuration`  
  此子页面展示当前流作业的 `AdaptiveScheduler` 在扩缩容操作中使用的相关参数值。

<a name="more-details"></a>

### 更多详情

更多详情请参阅 [FLIP-495](https://cwiki.apache.org/confluence/x/TQr0Ew) 和 [FLIP-487](https://cwiki.apache.org/confluence/x/vZCMEw)。

{{< top >}}
