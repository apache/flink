---
title: 流式编程模型
nav-id: programming-model
nav-pos: 1
nav-title: 编程模型
nav-parent_id: concepts
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

* This will be replaced by the TOC
{:toc}

## 抽象的层次（Levels of Abstraction）

Flink为开发流/批程序提供了多种不同层次的抽象。

<img src="{{ site.baseurl }}/fig/levels_of_abstraction.svg" alt="Programming levels of abstraction" class="offset" width="80%" />

  - 最低层次的抽象仅仅提供 **stateful streaming**。它被通过 [Process Function](../dev/stream/operators/process_function.html) 嵌入到 [DataStream API](../dev/datastream_api.html)中。这使得用户可以自由处理一个或多个 stream 中的事件，并在此过程中使用一致的容错*状态*。并且，用户可以注册 “事件时间” 和 “处理时间” 的回调，从而让程序实现复杂的计算。

  - 实际上，大多数程序都不需要用到上面提到的底层抽象，相反会面向
    **Core API** 编程， 比如 [DataStream API](../dev/datastream_api.html) (有边界/无边界 流) 和 [DataSet API](../dev/batch/index.html)
    (有边界的数据集)。这些 fluent API 提供了构建数据处理程序的通用模块，用户可以声明的各种形式的转换(transformation)，比如 连接(join)、聚合(aggregation)、窗口(window)、状态(state) 等等。这些API中的数据类型会用相应编程语言的类来表示。

    底层的 *Process Function* 与 *DataStream API* 结合使用时，就能以低层级的抽象来实现某些特定操作。*DataSet API* 为有界数据集提供了额外的原语，比如 循环(loop)/遍历(iteration)。

  - **Table API** 是一个围绕着 *tables* 的可声明 DSL，它可以动态的改变 table (当代表 stream 时)。[Table API](../dev/table/index.html) 遵循 (拓展的) 关系模型: Table 会附带 schema (就像关系型数据库中的表一样)
    ，并且 API 提供了相应的操作，比如 select、project、join、group-by、aggregate 等等。
    Table API 程序可以声明*应该执行什么样的逻辑操作*而不是声明
   *操作的代码看起来是怎样的*。 尽管 Table API 可以通过用户自己声明的函数来进行各种扩展，它仍然没有 *Core API* 表现力强，但是用起来更简洁（写更少的代码）。
    除此之外，Table API 程序也会经过一个优化器，对其进行执行前优化。

    tables 和 *DataStream*/*DataSet* 之间可以无缝切换，使得 *Table API*、*DataStream API*、*DataSet API* 可在同一个程序中混合使用。

  - Flink 提供的最高层级的抽象是 **SQL**。这种抽象再语义和表达方式上和 *Table API* 相似，但程序是以 SQL 表达式的形式存在。
    [SQL](../dev/table/index.html#sql) 抽象和 Table API 紧密交互，并且 SQL 查询可以在 *Table API* 定义的表上执行。


## 程序和数据流（Programs and Dataflow）

组成 Flink 程序基本的构件是 **stream** 和 **transformation** 。（需要注意的是，Flink 的 DataSet API 中的 DataSet，内部也是一个stream -- 稍后再对此做详细介绍。）理论上讲，一个 *stream* 是（可能永远不停止的）数据记录的流，而一个 *tranformation* 是一个操作，它会以一个或多个 stream 作为输入，并产生一个或多个 stream 作为结果输出。

执行时，Flink 程序被映射为 **streaming dataflow**，它由 **stream** 和 **转换算子**组成。
每一个数据流（dataflow）都从一个或多个 **source** 开始，并以一个或多个 **sink** 作为结尾。数据流（dataflow）和任意的 **有向无环图** *(DAGs)* 类似。尽管通过 *iteration* 构建一个特殊形式的环是被允许的，为简单起见，我们在大多数情况下会忽略这个点。

<img src="{{ site.baseurl }}/fig/program_dataflow.svg" alt="A DataStream program，and its dataflow." class="offset" width="80%" />

通常情况下，程序中的转换（transformation）和数据流（dataflow）中的算子（operator）有一对一的关系。然而，有的时候，一个转换可能会包含多个算子。

Source 和 sink 的文档分别记录在 [streaming connectors](../dev/connectors/index.html)、[batch connectors](../dev/batch/connectors.html)。
转换（Transformations）的文档记录在 [DataStream operators]({{ site.baseurl }}/zh/dev/stream/operators/index.html) 和 [DataSet transformations](../dev/batch/dataset_transformations.html)。

{% top %}

## 并行数据流（Parallel Dataflows）

Flink 程序天生就是并行的、分布式的。在执行的时候，一个 *stream* 有一个或多个 **流分区（stream partitions）**，
而每个 *算子（operator）* 有一个或多个**子任务**。算子的子任务是相互独立的，他们会在不同的线程执行，甚至有可能在不同的机器或容器执行。

一个算子的子任务就是该算子的**并行度（parallelism）**。 流（stream）的并行度和该流接入数据的算子并行度相同。相同程序的不同算子可能会有不同的并行度。

<img src="{{ site.baseurl }}/fig/parallel_dataflow.svg" alt="A parallel dataflow" class="offset" width="80%" />

流在算子之间传递时，可能是*一对一*（or *转发*）模式，也可能是 *重新分配* 模式：

  - **一对一**流 (比如上图中 *Source* 和 *map()* 算子之间的) 元素（element）的分区、顺序保持不变。也就是说，*map()* 算子的 subtask[1] 看到的元素就是 *Source* 算子的 subtask[1] 产生的那些元素，并且看到元素的顺序和产生时是相同的。

  - **重新分配**流（就像上图中 *map()* 和 *keyBy/window* 之间的样子，或者 *keyBy/window* 和 *Sink* 之间那样）会改变流的分区。根据所选转换（transformation）的不同，每一个 *operator subtask* 会将数据发送到不同的目标子任务。比如
    *keyBy()* (通过对 key 进行 hash 来重新分区)、*broadcast()*、*rebalance()*（随机的进行分区）。在 *重新分发* 交换的过程中，元素的顺序只在发送子任务和接受子任务形成的组合（例如 *map()* 的 subtask[1] 和 *keyBy/window* 的 subtask[2]）内得到保证。因此在这个例子中，每个 key 内部的顺序得到了保证，但是并行机制却引入了不确定因素，即不同 key 的聚合结果到达接收器（sink）的时间不同。

配置和控制并发度的细节从文档 [parallel execution](../dev/parallel.html) 中可以找到。

{% top %}

## 窗口（Window）

聚合事件（例如count、sum）在流程序中的工作方式和批处理中不同。
例如，在流程序中不可能计算元素的个数，
因为流一般是不会结束的（是没有边界的）。因此，对流进行聚合（count、sum 等）时，
需要将范围限定在 **window** 之内，比如 *“对最近5分钟的元素进行计数”*，或 *“对最近100个元素求和”*。

窗口（window）能以 *时间驱动*（例如：每30秒） 或 *数据驱动*（例如：每100个元素）。
窗口的典型划分为 *滚动窗口*（无重叠）、
*滑动窗口*（有重叠）、*会话窗口*（以一段空闲的时间作为分隔）。

<img src="{{ site.baseurl }}/fig/windows.svg" alt="Time- and Count Windows" class="offset" width="80%" />

在这里可以找到更多关于窗口的例子 [blog post](https://flink.apache.org/news/2015/12/04/Introducing-windows.html)，
更多细节请看 [window docs](../dev/stream/operators/windows.html)。

{% top %}

## 时间（Time）

当提到流程序中的时间时（比如声明一个窗口），这可能指的是不同概念的时间：

  - **事件时间（Event Time）** 是事件（event）被创建那一刻的时间。它一般用事件中的一个时间戳来表示，这个时间戳可能是生成数据的传感器附加上去的，也可能是产生数据的服务附加上去的。Flink 可以通过 [timestamp assigners]({{ site.baseurl }}/zh/dev/event_timestamps_watermarks.html) 来访问事件时间。

  - **摄取时间（Ingestion time）** 是事件通过数据源算子（source operator）进入到 Flink 数据流的时间。

  - **处理时间（Processing Time）** 每个基于时间进行操作的算子各自的本地时间。

<img src="{{ site.baseurl }}/fig/event_ingestion_processing_time.svg" alt="Event Time，Ingestion Time，and Processing Time" class="offset" width="80%" />

关于如何处理时间的细节请看 [event time docs]({{ site.baseurl }}/zh/dev/event_time.html)。

{% top %}

## 有状态的操作（Stateful Operation）

尽管数据流中的很多操作只关注一个事件在某一时刻出现的那一次（比如一个事件解析器），有些操作则会记住多个事件的信息（比如窗口操作）。我们称这些操作是**有状态的（stateful）**。

有状态的操作会在一个存储中维护它记住的状态，这个存储可以看做是一个嵌入式 key/value 存储。存储的状态会被分区，分区的方式及其分布和当前流保持严格一致
。因此，只能在 *keyBy()* 方法后，从*有key值的流* 中访问这种 key/value 状态，并且仅限于当前事件的 key 关联的值。将流和状态的 key 对齐保证了所有的状态更新都是本地操作，从而在没有事务开销的情况下保证一致性。由于进行了 key 值对齐，Flink 可以重新分配状态并透明地调整流的分区。

<img src="{{ site.baseurl }}/fig/state_partitioning.svg" alt="State and Partitioning" class="offset" width="50%" />

更多信息请看 [state](../dev/stream/state/index.html)。

{% top %}

## 用于容错的Checkpoint（Checkpoints for Fault Tolerance）

Flink 将 **流回放** 和 **checkpoint机制** 结合来实现容错。一个 checkpoint 与每个输入流的某个特定点及该流所有算子相应的状态有关。维护一致性 *(有且仅有一次的处理语义)* 时，通过恢复所有算子的状态并从 checkpoint 处开始回放事件，使得一个数据流可通过一个 checkpoint 得到恢复。

checkpoint 的间隔，实际上是对程序执行的容错开销和恢复时间（需要回放的事件个数）的权衡。

[fault tolerance internals]({{ site.baseurl }}/zh/internals/stream_checkpointing.html) 提供了更多关于 Flink 管理 checkpoint 的信息及相关的话题。开启和配置 checkpoint 的更多细节可从 [checkpointing API docs](../dev/stream/state/checkpointing.html) 找到。


{% top %}

## 基于流的批处理（Batch on Streaming）

Flink 把 [批处理程序](../dev/batch/index.html) 当做特殊的流程序来执行，这种情况下流是有边界的（有限数量的元素）。
一个 *数据集（DataSet）* 在内部也被当做是流数据。因此，上面提到的概念不仅适用于流程序，同样也适用于批处理程序，除了下面几个小点不同外，其他方面没有区别：

  - [批处理程序的容错](../dev/batch/fault_tolerance.html)不使用 checkpoint 机制。
    通过回放整个流来进行故障恢复，这是可行的，因为输入是有边界的。这使得容错恢复的消耗更大，但也使正常处理的消耗更小，因为避免了 checkpoint 机制。

  - DataSet API 中的有状态操作（Stateful operations）使用简单的 内存中/核心外 数据结构，而不是 key/value 索引。

  - DataSet API 引入了特殊的同步 (基于superstep) 迭代器，它只能用于有边界的流。更多细节，请查阅 [iteration docs]({{ site.baseurl }}/dev/batch/iterations.html)。

{% top %}

## 下一步（Next Steps）

继续看 Flink 的基本概念 [Distributed Runtime](runtime.html)。
