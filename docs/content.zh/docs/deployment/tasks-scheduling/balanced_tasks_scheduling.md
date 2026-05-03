---
title: Task 均衡调度
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

<a name="balanced-tasks-scheduling"></a>

# Task 均衡调度

本文档描述了 Task 均衡调度的背景和原理，以及如何在流处理作业中使用它。

<a name="background"></a>

## 背景

当 Flink 流处理作业中所有顶点的并行度不一致时，Flink 部署 Task 的[默认策略]({{< ref "docs/deployment/config" >}}#taskmanager-load-balance-mode)有时会导致某些 TaskManager 部署的 Task 较多，而其他 TaskManager 部署的 Task 较少，从而造成部署 Task 较多的 TaskManager 资源使用过度，成为整个作业处理的瓶颈。

{{< img src="/fig/deployments/tasks-scheduling/tasks_scheduling_skew_case.svg" alt="任务调度倾斜示例" class="offset" width="50%" >}}

如图（a）所示，假设 Flink 作业包含两个顶点：`JobVertex-A(JV-A)` 和 `JobVertex-B(JV-B)`，并行度分别为 `6` 和 `3`，且两个顶点属于同一个 Slot 共享组。在默认 Task 调度策略下，如图（b）所示，Task 在 TaskManager 之间的分布可能导致 Task 负载显著不均。具体来说，Task 数量最多的 TaskManager 可能承载 `4` 个任务，而负载最低的 TaskManager 可能只有 `2` 个任务。因此，承载 4 个 Task 的 TaskManager 容易成为整个作业的性能瓶颈。

因此，Flink 提供了基于 Task 数量的 Task 均衡调度能力。在作业的资源视图中，它旨在确保分配给每个 TaskManager 的任务数量尽可能接近，从而改善 TaskManager 之间的资源使用倾斜。

<span class="label label-info">注意</span> 并非并行度不一致就必须使用此策略，需根据实际情况判断。

<a name="principle"></a>

## 原理

基于 Task 数量的负载均衡调度策略将 Task 分配给 TaskManager 的过程分为两个阶段：
- Task 到 Slot 的分配阶段
- Slot 到 TaskManager 的分配阶段

本节将通过两个示例对基于 Task 数量的调度策略在上述两个阶段中处理 Task 分配的简化流程及其原理加以说明。

<a name="the-tasks-to-slots-assignment-phase"></a>

### Task 到 Slot 的分配阶段

以图（c）所示的作业为例，它包含五个作业顶点，并行度分别为 `1`、`4`、`4`、`2` 和 `3`。这五个作业顶点都属于默认 Slot 共享组。

{{< img src="/fig/deployments/tasks-scheduling/tasks_to_slots_allocation_principle.svg" alt="Task 到 Slot 分配原理示例" class="offset" width="65%" >}}

在 Task 到 Slot 的分配阶段，该调度策略：
- 首先直接将并行度最高顶点的 Task 分配到第 `i` 个 Slot。

  即将任务 `JV-Bi` 直接分配到 `sloti`，将任务 `JV-Ci` 也直接分配到 `sloti`。

- 接下来，对于属于次高并行度作业顶点的 Task ，以轮询方式分配给当前 Slot 共享组内的 Slot，直到所有 Task 分配完毕。

如图（e）所示，在基于 Task 数量的分配策略下，每个 Slot 的任务数量范围（最大值与最小值之差）为 `1`，这优于图（d）所示默认策略下的范围 `3`。

因此，这确保了 Slot 之间 Task 数量更加均衡地分布。

<a name="the-slots-to-taskmanagers-assignment-phase"></a>

### Slot 到 TaskManager 的分配阶段

如图（f）所示，假设 Flink 作业包含两个顶点 `JV-A` 和 `JV-B`，并行度分别为 `6` 和 `3`，且两个顶点属于同一个 Slot 共享组。

{{< img src="/fig/deployments/tasks-scheduling/slots_to_taskmanagers_allocation_principle.svg" alt="Slot 到 TaskManager 分配原理示例" class="offset" width="75%" >}}

第一阶段的分配结果如图（g）所示，其中 `Slot0`、`Slot1` 和 `Slot2` 各包含 `2` 个 Task ，其余 Slot 各包含 `1` 个 Task 。

随后：
- 策略提交所有 Slot 请求，并等待当前作业所需的所有 Slot 资源准备就绪。

Slot 资源准备就绪后：
- 首先根据每个请求包含的 Task 数量按降序对所有 Slot 请求进行排序。然后，依次将每个 Slot 请求分配给当前 Task 负载最小的 TaskManager。此过程持续进行，直到所有 Slot 请求分配完毕。

最终分配结果如图（i）所示，每个 TaskManager 最终恰好承载 `3` 个 Task ，TaskManager 之间的 Task 数量差异为 `0`。相比之下，默认策略下的调度结果如图（h）所示，TaskManager 之间的 Task 数量差异为 `2`。

因此，如果你遇到上述描述的性能瓶颈问题，使用这种 Task 负载均衡调度策略可以改善性能。请注意，如果你没有遇到这些瓶颈问题，则不应使用此策略，因为这可能导致性能下降。

<a name="usage"></a>

## 使用方法

你可以通过以下配置项启用 Task 均衡调度：

- `taskmanager.load-balance.mode`: `tasks`

<span class="label label-info">注意</span> 在故障切换场景中，
当资源被释放并处理资源请求时，资源视图的延迟更新可能导致分配结果未达到最优平衡。
在这种情况下，可通过适当增大
[`slot.request.max-interval`]({{< ref "docs/deployment/config" >}}#slot-request-max-interval) 的数值来改善此问题。
例如，每次可尝试以 `50` 毫秒为增量进行调整。
提高该数值将使作业调度期间的槽位请求与可用资源视图更加稳定，从而使任务能够尽可能均衡分配。
但相应地，这会延长任务调度阶段的整体时长。
因此，增大此配置值也会增加 [`slot.request.timeout`]({{< ref "docs/deployment/config" >}}#slot-request-timeout) 参数超时的风险。
值得强调的是，若在充分提高该选项数值后，仍出现非最优的任务均衡现象，
可在 <a href="https://issues.apache.org/jira/browse/FLINK-38715">FLINK-38715</a> 中报告此问题，
报告中应包含调度相关配置的说明及观察到的具体现象描述。

<a name="more-details"></a>

## 更多详情

更多详细信息请参阅 [FLIP-370](https://cwiki.apache.org/confluence/x/U56zDw)。

{{< top >}}
