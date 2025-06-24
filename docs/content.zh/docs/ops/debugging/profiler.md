---
title: "Profiler"
weight: 3
type: docs
aliases:
  - /ops/debugging/profiler.html
  - /ops/debugging/profiler
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

# 分析器

自 Flink 1.19 起，我们基于 Flink Web UI ，用 [async-profiler](https://github.com/async-profiler/async-profiler) 交互式地分析JobManager/TaskManager 进程， 让用户可以创建具有任意间隔和事件模式的分析实例，比如 ITIMER、CPU、Lock、Wall-Clock 和 Allocation。

- **CPU**: 在这种模式下，分析器收集包括Java方法、本地调用、JVM代码和内核函数在内的堆栈跟踪样本
- **ALLOCATION**: 在allocation 分析模式下， 每个调用跟踪的顶部框架是被分配对象的类，以及对分配的TLAB或TLAB之外的对象的总大小的计数。
- **Wall-clock**: Wall-Clock选项使得async-profiler在每个给定的时间段内均匀对所有线程进行采样，无论线程状态如何：正在运行、睡眠或阻塞。这个选项在分析应用程序启动时间时会很有帮助
- **Lock**: 在lock分析模式下，顶部框架是锁定/监视器的类，计数器是进入此锁定/监视器所需的纳秒数。
- **ITIMER**: 你可以退回到itimer分析模式。它类似于cpu模式，但不需要perf_events支持。它的缺陷是没有内核堆栈跟踪。

{{< hint warning >}}

任何测量过程本身都不可避免地会影响测量对象。为了防止对生产环境产生意外影响，分析器目前作为一项可选的功能。要启用它，你需要在[Flink配置文件]({{< ref "docs/deployment/config#flink-configuration-file" >}}) 中设置 [`rest.profiling.enabled: true`]({{< ref "docs/deployment/config">}}#rest-profiling-enabled) 。我们建议在开发和预生产环境中启用它，但在生产环境中应将其视为试验性功能。

{{< /hint >}}


## 要求
由于分析器由async-profiler驱动，因此必须在async-profiler支持的平台上运行。

|           | Officially maintained builds | Other available ports                     |
|-----------|------------------------------|-------------------------------------------|
| **Linux** | x64, arm64                   | x86, arm32, ppc64le, riscv64, loongarch64 |
| **macOS** | x64, arm64                   |                                           |

在上述列表之外的平台分析将在 `Message` 列中报错。


##  用途
Flink用户可以通过Flink Web UI方便地完成剖析提交和结果导出。

比如，
- 首先，你应找出存在性能瓶颈的 TaskManager/JobManager，然后切换到相应的 TaskManager/JobManager 页面（分析器选项卡）。
- 你只需通过点击 **创建分析实例**按钮来提交一个具有特定持续时间和模式的剖析实例。 （悬停在模式上时将显示相应分析模式的描述。）
- 一旦分析实例完成，你可以通过点击链接下载交互式 HTML 文件。

{{< img src="/fig/profiler_instance.png" class="img-fluid" width="90%" >}}
{{% center %}}
Profiling Instance
{{% /center %}}


## 故障排除
1. **Failed to profiling in CPU mode: No access to perf events. Try –fdtransfer or –all-user option or ‘sysctl kernel.perf_event_paranoid=1’** \
   这意味着 `perf_event_open()` 系统调用失败。默认情况下，Docker容器限制对 `perf_event_open` 系统调用。建议解决方案是回退到ITIMER分析模式。它类似于cpu模式，但不需要perf_events支持。它的缺点是没有内核堆栈跟踪。

2. **Failed to profiling in Allocation mode: No AllocTracer symbols found. Are JDK debug symbols installed?** \
   OpenJDK debug symbols 在 allocation分析模式下是必需的。 在 [Installing Debug Symbols](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#installing-debug-symbols) 中查看更多详细信息。

{{< hint info >}}

你可以在 [Troubleshooting](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#troubleshooting) 页面的async-profiler中查阅更多案例。

{{< /hint >}}
