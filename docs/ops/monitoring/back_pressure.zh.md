---
title: "监控反压"
nav-parent_id: monitoring_group
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

Flink Web 界面提供了一个选项卡来监控正在运行 Job 的反压行为。

* ToC
{:toc}

## 反压

如果你看到一个 Task 发生 **反压警告**（例如： `High`），意味着它生产数据的速率比下游 Task 消费数据的速率要快。
在工作流中数据记录是从上游向下游流动的（例如：从 Source 到 Sink）。反压沿着相反的方向传播，沿着数据流向上游传播。

以一个简单的 `Source -> Sink` Job 为例。如果看到 `Source` 发生了警告，意味着 `Sink` 消费数据的速率比 `Source` 生产数据的速率要慢。
`Sink` 正在向上游的 `Source` 算子产生反压。


## 反压采样

通过不断对每个 Task 的反压状态采样来进行反压监控。JobManager 会触发对 Task `Task.isBackPressured()` 的重复调用。

<img src="{% link /fig/back_pressure_sampling.png %}" class="img-responsive">
<!-- https://docs.google.com/drawings/d/1O5Az3Qq4fgvnISXuSf-MqBlsLDpPolNB7EQG7A3dcTk/edit?usp=sharing -->

Task 是否反压是基于输出 Buffer 的可用性判断的，如果一个用于数据输出的 Buffer 都没有了，则表明 Task 被反压了。

默认情况下，JobManager 会触发 100 次采样，每次间隔 50ms 来确定反压。
你在 Web 界面看到的比率表示在获得的样本中有多少表明 Task 正在被反压，例如: `0.01` 表示 100 个样本中只有 1 个反压了。

- **OK**: 0 <= 比例 <= 0.10
- **LOW**: 0.10 < 比例 <= 0.5
- **HIGH**: 0.5 < 比例 <= 1

为了不因为采样导致 TaskManager 负载过重，Web 界面仅在每 60 秒后重新采样。

## 配置参数

你可以使用以下键来配置 JobManager 的样本数：

- `web.backpressure.refresh-interval`: 有效的反压结果被废弃并重新进行采样的时间 (默认: 60000, 1 min)。
- `web.backpressure.num-samples`: 用于确定反压采样的样本数 (默认: 100)。
- `web.backpressure.delay-between-samples`: 用于确定反压采样的间隔时间 (默认: 50, 50 ms)。


## 示例

你可以在 Job 的 Overview 选项卡后面找到 *Back Pressure* 。

### 采样进行中

这意味着 JobManager 对正在运行的 Task 触发了反压采样。默认情况下，大约需要 5 秒完成采样。

注意，点击该行，可触发该算子所有 SubTask 的采样。

<img src="{% link /fig/back_pressure_sampling_in_progress.png %}" class="img-responsive">

### 反压状态

如果你看到 Task 的状态为 **OK** 表示没有反压。**HIGH** 表示这个 Task 被反压。

<img src="{% link /fig/back_pressure_sampling_ok.png %}" class="img-responsive">

<img src="{% link /fig/back_pressure_sampling_high.png %}" class="img-responsive">

{% top %}
