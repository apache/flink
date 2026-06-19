---
title: "火焰图"
weight: 3
type: docs
aliases:
  - /ops/debugging/flame_graphs.html
  - /ops/debugging/flame_graphs
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

# 火焰图

[Flame Graphs](http://www.brendangregg.com/flamegraphs.html) 是一种有效的可视化工具，可以回答以下问题：

- 目前哪些方法正在消耗 CPU 资源？
- 一个方法的消耗与其他方法相比如何？
- 哪一系列的堆栈调用导致了特定方法的执行?

{{< img src="/fig/flame_graph_on_cpu.png" class="img-fluid" width="90%" >}}
{{% center %}}
Flame Graph
{{% /center %}}

火焰图是通过多次采样堆栈跟踪来构建的。每个方法调用都由一个条形图表示，其中条形图的长度与其在样本中出现的次数成比例。

从 Flink 1.13 版本开始支持火焰图。要生成一个火焰图，请导航到正在运行的作业图，选择感兴趣的算子，并在右侧菜单中点击 "Flame Graph" 选项卡： 

{{< img src="/fig/flame_graph_operator.png" class="img-fluid" width="90%" >}}
{{% center %}}
算子级别的 On-CPU 火焰图
{{% /center %}}

{{< hint warning >}}

任何测量过程本身不可避免地会影响被测对象(参考 [double-split experiment](https://en.wikipedia.org/wiki/Double-slit_experiment#Relational_interpretation))。对CPU堆栈跟踪进行采样也不例外。为了防止对生产环境产生意外影响，火焰图目前作为一项选择性功能可用。要启用它，你需要设置 [`rest.flamegraph.enabled: true`]({{< ref "docs/deployment/config">}}#rest-flamegraph-enabled) in [Flink configuration file]({{< ref "docs/deployment/config#flink-配置文件" >}})。我们建议在开发和预生产环境中启用它，但在生产环境中请将其视为实验性功能。

{{< /hint >}}

除了 On-CPU 火焰图之外， [Off-CPU](http://www.brendangregg.com/FlameGraphs/offcpuflamegraphs.html) 还有混合可视化模式可供选择，并可以通过面板顶部的选择器进行切换：

{{< img src="/fig/flame_graph_selector.png" class="img-fluid" width="30%" >}}

Off-CPU 火焰图可视化了在样本中找到的阻塞调用。按如下方式进行区分：
- On-CPU: `Thread.State` in **[RUNNABLE, NEW]**
- Off-CPU: `Thread.State` in **[TIMED_WAITING, WAITING, BLOCKED]**

{{< img src="/fig/flame_graph_off_cpu.png" class="img-fluid" width="90%" >}}
{{% center %}}
Off-CPU Flame Graph
{{% /center %}}

混合模式的火焰图是由处于所有可能状态的线程的堆栈跟踪构建而成。

{{< img src="/fig/flame_graph_mixed.png" class="img-fluid" width="90%" >}}
{{% center %}}
混合模式的火焰图
{{% /center %}}

##  采样过程

堆栈跟踪的收集纯粹在 JVM 内部进行，因此只能看到 Java 运行时内的方法调用（看不到系统调用）。

默认情况下，火焰图的构建是在单个[operator]({{< ref "docs/concepts/glossary" >}}#operator)级别上进行的，
即该算子的所有[task]({{< ref "docs/concepts/glossary" >}}#task)线程并行采样，并将它们的堆栈跟踪合并起来。
如果某个方法调用在其中一个并行任务中占用了100%的资源，但在其他任务中没有占用，则可能会被平均化而掩盖住瓶颈。

Flink 从 1.17 版本开始提供了单并发级别火焰图可视化的功能。
选择一个感兴趣的子任务，您可以看到相应子任务的火焰图。

{{< img src="/fig/flame_graph_subtask.png" class="img-fluid" width="90%" >}}
{{% center %}}
子任务级别的火焰图
{{% /center %}}
