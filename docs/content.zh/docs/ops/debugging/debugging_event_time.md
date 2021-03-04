---
title: "调试窗口与事件时间"
weight: 2
type: docs
aliases:
  - /zh/ops/debugging/debugging_event_time.html
  - /zh/monitoring/debugging_event_time.html
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

# 调试窗口与事件时间

<a name="monitoring-current-event-time"></a>

## 监控当前事件时间（Event Time）

Flink 的[事件时间]({{< ref "docs/concepts/time" >}})和 watermark 支持对于处理乱序事件是十分强大的特性。然而，由于是系统内部跟踪时间进度，所以很难了解究竟正在发生什么。

可以通过 Flink web 界面或[指标系统]({{< ref "docs/ops/metrics" >}})访问 task 的 low watermarks。

Flink 中的 task 通过调用 `currentInputWatermark` 方法暴露一个指标，该指标表示当前 task 所接收到的 the lowest watermark。这个 long 类型值表示“当前事件时间”。该值通过获取上游算子收到的所有 watermarks 的最小值来计算。这意味着用 watermarks 跟踪的事件时间总是由最落后的 source 控制。

**使用 web 界面**可以访问 low watermark 指标，在指标选项卡中选择一个 task，然后选择 ```<taskNr>.currentInputWatermark``` 指标。在新的显示框中，你可以看到此 task 的当前 low watermark。

获取指标的另一种方式是使用**指标报告器**之一，如[指标系统]({{< ref "docs/ops/metrics" >}})文档所述。对于本地集群设置，我们推荐使用 JMX 指标报告器和类似于 [VisualVM](https://visualvm.github.io/) 的工具。




<a name="handling-event-time-stragglers"></a>

## 处理散乱的事件时间

  - 方式 1：延迟的 Watermark（表明完整性），窗口提前触发
  - 方式 2：具有最大延迟启发式的 Watermark，窗口接受迟到的数据

{{< top >}}
