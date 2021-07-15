---
title: 及时流处理
weight: 3
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

# 及时流处理

## 介绍

及时流处理（timely stream processing）是[有状态的流处理]({{< ref "docs/concepts/stateful-stream-processing" >}})的扩展，它的时间扮演了一些计算的角色。
除其他事项外，当你进行时间序列分析、基于特定时间段（通常称为窗口）进行聚合时，或者当你进行事件发生时间很重要的事件处理时，就会出现这种情况。

在下面的部分中，我们将重点介绍在使用及时流 Flink 应用程序时应该考虑的一些话题。

{{< top >}}

## 时间的概念：Event Time 和 Processing Time

当在流处理程序中提到时间时（比如定义窗口），可以指不同的*时间*概念：

- **Processing time**：处理时间（processing time）是指正在执行相应操作的机器的系统时间。

  当流程序按处理时间运行时，所有基于时间的操作（如时间窗口）都将使用运行相应算子的机器系统时钟。
  小时处理时间窗口将包括在系统时钟指示整小时之间到达的特定算子的所有记录。
  例如，如果应用程序在上午 9:15 开始运行，第一个小时处理时间窗口将包含在上午 9:15 和 上午 10:00 之间处理的事件，
  下一个窗口将包含在上午 10:00 和 11:00 之间处理的事件，然后以此类推。

  处理时间是最简单的时间概念，不需要流和机器之间的协调。
  它提供了最佳的性能和最低的延迟。然而，在分布式和异步环境中，处理时间不提供确定性保证，
  因为它易受到记录到达系统（例如从消息队列来）的速度，以及记录在系统内部算子之间流动的速度的影响，
  和断电（预定的或者其他方式）的影响。

- **Event time**：事件时间（event time）是每个事件在其生成设备上发生的时间。
  这个时间通常在进入到 Flink 之前就嵌入在记录里面，*事件时间戳* 可以从每个记录中获取。
  在事件时间中，时间的进度取决于数据，而不是任何时钟。
  事件事件程序必须指定如何生成 *事件水位线*（Event Time Watermarks），这是表示事件时间进度的机制。
  这种水位线机制在后面的部分中有描述，[如下](#event-time-and-watermarks)。

  在理想的情况下，事件时间处理将产生完全一致和确定的结果，无论事件何时到达或它们的顺序如何。
  然而，除非已经知道事件按顺序到达（通过时间），不然在等待乱序事件时，事件时间的处理会有一些延迟。
  由于只能等待一段有限的时间，这限制了事件时间应用程序的确定性。

  假如所有的数据都已经到达，事件时间操作会按预期进行，即使在处理乱序或者延迟事件，或重新处理历史数据时， 也能生产出正确且一致的结果。
  例如，小时事件时间窗口将包含所有带有属于该小时的事件时间戳的记录，无论它们到达的顺序或处理时间如何。

  请注意，有时候当事件时间程序在处理实时数据时，它们会使用*处理时间*操作以确保它们及时进行。

{{< img src="/fig/event_processing_time.svg" alt="Event Time 和 Processing Time" width="80%" >}}

{{< top >}}

<a name="event-time-and-watermarks"></a>

## Event Time 和 Watermarks

*注意：Flink 实现了 Dataflow Model 中的很多技术。关于事件事件和水位线的详细介绍，请看以下文章。* 

  - [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) ，作者：Tyler Akidau
  - [Dataflow Model 论文](https://research.google.com/pubs/archive/43864.pdf)


支持*事件时间*的流处理器需要衡量事件时间的进度的方法。
例如，构建小时窗口的窗口算子需要在事件时间通过这个小时之后通知窗口算子，这样算子可以关闭正在进行的窗口。

*事件时间*可以独立于*处理时间*（通过时钟测量）进行。
例如，在程序里，算子当前的*事件时间*可能稍微落后于*处理时间*（考虑到接收事件的延迟），然而两者是以相同的速度进行。
另一方面，另外一个流处理程序通过快速转发一些已经缓冲在 Kafka topic（或另一个消息队列）中的历史数据，
可能只需要几秒钟就可以处理数周的事件时间。

------

Flink 中衡量事件时间进度的机制是**水位线**（watermarks）。水位线作为数据流的一部分流动并带有时间戳 *t*。
*水位线（t）*  声明该流中的事件时间已达到时间 *t*，这意味着流中不应再有时间戳为 *t' <= t* 的元素（即时间戳早于或等于水位线的事件）。

下图展示了带有（逻辑）时间戳和内联流动的水位线的事件流。这个例子中，事件是有序的（相对于它们的时间戳），这意味着水位线只是流中的周期性标记。

{{< img src="/fig/stream_watermark_in_order.svg" alt="带有（排序的）事件和水位线的数据流" width="65%" >}}

水位线对于*乱序*（out-of-order）流至关重要，如下图所示，其中事件没有按时间戳排序。
一般来说，水位线是一种声明，即到流中的那个点，到某个时间戳的所有事件都应该已经到达。
一旦水位线到达算子，算子就可以将其内部*事件时钟*提前到水位线的值。

{{< img src="/fig/stream_watermark_out_of_order.svg" alt="带有事件（乱序）和水位线的数据流" width="65%" >}}

请注意，事件时间由新创建的流元素（或多个元素）从产生它们的事件或触发这些元素创建的水位线继承。

### 并行流中的水位线

水位线在 source functions 处或直接在 source functions 之后生成。
source function 的并行 subtask 通常独立生成它的水位线。这些水位线定义了该特定并行源的事件时间。

由于水位线通过流处理程序流动，它们在到达的算子上提前了事件时间。每当算子提前其事件时间时，它就会为其后继算子在下游生成一个新的水位线。
每当算子提前它的事件时间时，它都会为其后继算子生成新的水位线下游。

一些算子消费多个输入流；例如 union 或在 *keyBy(...)* 或 *partition(...)* 函数后面的算子。
这样的算子的当前事件时间是其输入流事件时间的最小值。当它的输入流更新了它们的事件时间时，算子也会更新。

下图展示了流经并行流的事件和水位线和追踪事件时间的算子的例子。

{{< img src="/fig/parallel_streams_watermarks.svg" alt="有事件和水位线的并行数据流和算子" class="center" width="80%" >}}

## 延迟

某些元素可能会违反水位线条件，这意味着即使在*水位线（t）* 发生之后，也会出现更多时间戳为 *t' <= t* 的元素。
事实上，在很多实际的设置中，有些元素可以任意地延迟，因此无法指定某个事件时间戳的所有元素将发生的时间。
此外，即使延迟是有界的，过多的水位线延迟通常也是不可取的，因为它会导致事件时间窗口的计算延迟太多。

出于这个原因，流处理程序可能会明确地预计有*迟到*元素存在。
迟到的元素是在系统的事件时间时钟（由水位线发出信号）已经超过迟到元素的时间戳时间之后到达的元素。
有关如何在事件时间窗口中处理延迟元素的更多信息，请参阅 [允许的延迟]({{< ref "docs/dev/datastream/operators/windows" >}}#allowed-lateness)。

## Windowing

聚合事件（例如 计数、求和等）在流上的工作方式与批处理不同。例如，我们不可能在流上对所有的元素求和，因为流通常是无限的（无界）。
相反，在流上的聚合（计数、求和等）由**窗口**限定，比如 *"过去 5 分钟的数量"*或*"最后 100 个元素的和"*。

窗口可以是*时间驱动*的（例如：每 30 秒）或*数据驱动*的（例如：每 100 个元素）。
通常区分不同类型的窗口，例如*滚动窗口*（tumbling windows）（无重叠）、*滑动窗口*（sliding windows）（有重叠）和*会话窗口*（session windows）（由不活动的间隙隔开）。

{{< img src="/fig/windows.svg" alt="时间和计数窗口" class="offset" width="80%" >}}

更多窗口示例，请查看这篇[博客](https://flink.apache.org/news/2015/12/04/Introducing-windows.html) 或查看关于 
DataStream API 的[窗口文档]({{< ref "docs/dev/datastream/operators/windows" >}})。

{{< top >}}
