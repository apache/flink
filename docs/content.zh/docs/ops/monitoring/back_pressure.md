---
title: "监控反压"
weight: 3
type: docs
aliases:
  - /zh/ops/monitoring/back_pressure.html
  - /zh/internals/back_pressure_monitoring.html
  - /zh/ops/monitoring/back_pressure
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

# 监控反压

Flink Web 界面提供了一个选项卡来监控正在运行 jobs 的反压行为。

## 反压

如果你看到一个 task 发生 **反压警告**（例如： `High`），意味着它生产数据的速率比下游 task 消费数据的速率要快。
在工作流中数据记录是从上游向下游流动的（例如：从 Source 到 Sink）。反压沿着相反的方向传播，沿着数据流向上游传播。

以一个简单的 `Source -> Sink` job 为例。如果看到 `Source` 发生了警告，意味着 `Sink` 消费数据的速率比 `Source` 生产数据的速率要慢。
`Sink` 正在向上游的 `Source` 算子产生反压。

## Task 性能指标

Task（SubTask）的每个并行实例都可以用三个一组的指标评价：
- `backPressuredTimeMsPerSecond`，subtask 被反压的时间
- `idleTimeMsPerSecond`，subtask 等待某类处理的时间
- `busyTimeMsPerSecond`，subtask 实际工作时间
在任何时间点，这三个指标相加都约等于`1000ms`。

这些指标每两秒更新一次，上报的值表示 subtask 在最近两秒被反压（或闲或忙）的平均时长。
当你的工作负荷是变化的时需要尤其引起注意。比如，一个以恒定50%负载工作的 subtask 和另一个每秒钟在满负载和闲置切换的 subtask 的`busyTimeMsPerSecond`值相同，都是`500ms`。 

在内部，反压根据输出 buffers 的可用性来进行判断的。
如果一个 task 没有可用的输出 buffers，那么这个 task 就被认定是在被反压。
相反，如果有可用的输入，则可认定为闲置，

## 示例
WebUI 集合了所有 subTasks 的反压和繁忙指标的最大值，并在 JobGraph 中将集合的值进行显示。除了显示原始的数值，tasks 也用颜色进行了标记，使检查更加容易。

{{< img src="/fig/back_pressure_job_graph.png" class="img-responsive" >}}

闲置的 tasks 为蓝色，完全被反压的 tasks 为黑色，完全繁忙的 tasks 被标记为红色。
中间的所有值都表示为这三种颜色之间的过渡色。

## 反压状态

在 Job Overview 旁的 *Back Pressure* 选项卡中，你可以找到更多细节指标。

{{< img src="/fig/back_pressure_subtasks.png" class="img-responsive" >}}

如果你看到 subtasks 的状态为 **OK** 表示没有反压。**HIGH** 表示这个 subtask 被反压。状态用如下定义：

- **OK**: 0% <= 反压比例 <= 10%
- **LOW**: 10% < 反压比例 <= 50%
- **HIGH**: 50% < 反压比例 <= 100%

除此之外，你还可以找到每一个 subtask 被反压、闲置或是繁忙的时间百分比。

{{< top >}}
