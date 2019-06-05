---
title:  "Task 生命周期"
nav-title: Task 生命周期
nav-parent_id: internals
nav-pos: 5
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

Task 是 Flink 的基本执行单元。算子的每个并行实例都是在 task 里执行的。举个例子，一个并行度为 5 的算子，它的每个实例都由一个单独的 task 来执行。

在 Flink 流式计算引擎里，`StreamTask` 是所有不同子类型 task 的基础。这篇文档会深入 `StreamTask` 生命周期的不同阶段，介绍每个阶段的主要方法。

* This will be replaced by the TOC
{:toc}

## 算子生命周期简介

因为 task 是算子并行实例的执行实体，所以它的生命周期跟算子的生命周期紧紧联系到一起。因此，在深入介绍 `StreamTask` 生命周期之前，先简要介绍一下代表算子生命周期的各个基本方法。这些方法列表按调用的先后顺序排列如下所示。考虑到算子可能有用户自定义函数(*UDF*)，我们也提到了 UDF 生命周期里调用的各个方法。如果你的算子继承了 `AbstractUdfStreamOperator` 的话，这些方法都是可用的，`AbstractUdfStreamOperator` 是所有继承 UDF 算子的基类。

        // 初始化阶段
        OPERATOR::setup
             UDF::setRuntimeContext
        OPERATOR::initializeState
        OPERATOR::open
             UDF::open
        
        // 调用处理阶段（通过每条数据或 watermark 来调用）
        OPERATOR::processElement
             UDF::run
        OPERATOR::processWatermark
        
        // checkpointing 阶段（通过每个 checkpoint 异步调用）
        OPERATOR::snapshotState
                
        // 结束阶段
        OPERATOR::close
             UDF::close
        OPERATOR::dispose

简而言之，调用 `setup()` 是初始化算子级别的组件，比如 `RuntimeContext` 和 指标收集的数据结构。在这之后，`initializeState()` 给算子提供初始状态，
 `open()` 方法执行所有算子级别的初始化，比如在继承 `AbstractUdfStreamOperator` 的情况下，打开用户定义的函数。

<span class="label label-danger">注意</span> `initializeState()` 既包含在初始执行时（比如注册 keyed 状态）初始化算子状态的逻辑，又包含作业失败后从 checkpoint 中恢复原有状态的逻辑。在接下来的篇幅会更详细的介绍这块。

当所有初始化都设置之后，算子开始准备处理即将流入的数据。流入的数据可以分为三种类型：正常输入元素、水位 和 checkpoint 屏障。每种类型的数据都有单独的方法来处理。正常输入元素通过 `processElement()` 方法来处理，水位通过 `processWatermark()` 来处理，checkpoint 屏障会触发异步执行的 `snapshotState()` 方法来进行 checkpoint。`processElement()`方法也是用户自定义函数逻辑执行的地方，比如用户自定义 `MapFunction` 里的 `map()` 方法。

最后，在正常无失败的情况下结束算子（比如，如果流式数据是有限的，并且最后一个数据已经到了）会调用 `close()` 方法，进行算子逻辑（比如关闭算子执行期间打开的连接或 I/O 流）要求的最终簿记工作。在这之后会调用 `dispose()` 方法来释放算子持有的资源（比如算子数据持有的本地内存）。

在作业失败或手动取消的情况下结束算子，整个执行过程会直接跳到 `dispose()` 方法，跳过算子在故障发生时所处阶段和 `dispose()` 之间的中间数据阶段。

**Checkpoints:** 算子的 `snapshotState()` 方法是在收到 checkpoint 屏障后异步调用的。Checkpoint 是在处理阶段执行的，即算子打开之后，结束之前的这个阶段。这个方法的职责就是存储算子的当前状态到一个特定的[状态后端]({{ site.baseurl }}/ops/state/state_backends.html)，当作业失败后恢复执行时会从这个后端读取状态数据。下面我们简要描述了 Flink 的 checkpoint 机制，如果想了解更多 Flink checkpoint 相关的原理，可以读一读 [数据流容错]({{ site.baseurl }}/internals/stream_checkpointing.html)。

## Task 生命周期

在上文对算子主要阶段的简要介绍之后，本节将详细介绍 task 在集群执行期间是如何调用相关方法的。这里所说的阶段序列主要包含在 `StreamTask` 类的 `invoke()` 方法里。本文档的剩余部分会分成两个子章节，其中一节描述了 task 在常规没有失败执行的阶段（请参考 [常规执行](#normal-execution)），另外一节描述了 task 取消之后的不同阶段序列（请参考 [中断执行](#interrupted-execution)），不管是手动取消还是其他原因导致的取消，比如执行期间的异常。

### 常规执行

Task 在没有中断的情况下执行直到最终完成所经历的步骤如下所示：

        TASK::setInitialState
        TASK::invoke
            create basic utils (config, etc) and load the chain of operators
            setup-operators
            task-specific-init
            initialize-operator-states
            open-operators
            run
            close-operators
            dispose-operators
            task-specific-cleanup
            common-cleanup

如上所示，在恢复 task 配置和初始化一些重要的运行时参数之后，task 的第一步就是读取它初始的、task 级别的状态数据。这一步是在 `setInitialState()` 方法里完成的，在下面两个场景特别重要：
1. 当 Task 从失败中恢复，从最后一次成功的 checkpoint 重启的时候
2. 当 Task 从 [savepoint]({{ site.baseurl }}/ops/state/savepoints.html) 里恢复的时候。

如果 task 是第一次执行的话，它的初始状态为空。

在恢复初始状态之后，task 进入到 `invoke()` 方法。在这里，首先调用 `setup()` 方法来初始化本地计算涉及到的每个算子，然后调用本地的 `init()` 方法来做 特定 task 的初始化。这里所说的特定 task，取决于 task 的类型 (`SourceTask`，`OneInputStreamTask` 或 `TwoInputStreamTask` 等等)。这一步可能会有差异，但却是申请 task 范围所需资源的地方。举个例子，`OneInputStreamTask` 代表着只接受一个输入流的 task，它初始化输入流不同分区的连接位置。

在申请到必要的资源之后，不同算子和用户定义函数开始从上面读到的 task 范围状态数据里获取他们各自的状态值。这块工作是在各个算子里调用 `initializeState()` 方法完成的。每个有状态的算子都应该重写该方法，包含状态初始化的逻辑，既适用于作业第一次执行的场景，又适用于 task 从失败或 savepoint 中恢复的场景。

现在 task 里的所有算子都已经被初始化了，每个算子里的 `open()` 方法也通过 `StreamTask` 的 `openAllOperators()` 方法调用了。这个方法进行所有操作的初始化，比如在定时器服务里注册获取到的定时器。一个 task 可能会执行多个算子，即一个算子消费它之前算子的输出数据流。在这种情况下，`open()` 方法通过最后一个算子来调用，即算子的输出刚好也是整个 task 的输出。当第一个算子开始处理 task 的输入数据流时，所有下游算子已经准备接收它的输出数据了。

<span class="label label-danger">注意</span> task 里多个连续算子是从后往前依次开启的。

现在 task 可以恢复执行，算子可以开始处理新输入的数据。在这里，task 级别的 `run()` 会被调用。这个方法会一直运行直到没有更多输入数据进来（有限的数据流）或者 task 被取消了（人为的或其他的原因）。这里是算子级别的 `processElement()` 方法和 `processWatermark()` 方法执行的地方。

在运行到完成的情况下，即没有输入数据需要处理，在退出 `run()`  方法之后，task 进入到关闭阶段。首先定时器服务停止注册任何新的定时器（比如从正在执行的定时器里注册），清理掉所有还未启动的定时器，等待当前执行中的定时器运行结束。`closeAllOperators()` 方法通过调用每个算子的 `close()` 方法来优雅的关掉所有参与计算的算子。然后所有缓存的输出数据会刷出去以便下游 task 处理，最终 task 通过调用每个算子的 `dispose()` 方法来尝试清理掉算子持有的所有资源。之前我们提到不同算子开启时，是从后往前依次调用；关闭时刚好相反，从前往后依次调用。

<span class="label label-danger">注意</span> task 里的多个连续算子关闭时是从前往后依次进行。

最后，当所有算子都已经关闭，所有资源都已被释放时，task 关掉它的定时器服务，进行 task 级别的清理操作，即清理掉所有内部缓存，然后进行常规的 task 清理操作，其中包括关闭所有的输出管道，清理所有输出缓存等。 

**Checkpoints:** 之前我们看到在执行 `initializeState()` 方法期间，在从异常失败中恢复的情况下，task 和它内部的所有算子函数都从最后一次成功的 checkpoint 数据里获取对应的状态信息。Flink 里的 checkpoint 是根据用户自定义的时间间隔定时执行的，在一个单独的线程里进行的，与执行算子操作的 task 的主线程不同。这也是我们没有把 checkpoint 过程涵盖在 task 生命周期主要阶段里的原因。简而言之，Flink 作业的输入数据流 task 会定时插入一种叫 `checkpoint 屏障` 的特殊数据，并跟正常数据一起从数据源头流入到最终落盘。数据源头 task 在进入运行模式后会插入这些屏障数据，假设 `checkpoint协调者` 也在运行中。当 task 接收到这样的屏障之后，会通过 task 算子里的 `snapshotState()` 方法调度 checkpoint 线程执行具体任务。在 checkpoint 处理期间，task 依然可以接收输入数据，但是数据会被缓存起来，当 checkpoint 执行成功之后才会被处理和发送到下游算子。 

### 中断执行

在之前的章节，我们描述的是运行直到完成的 task 生命周期。在任意时间点取消 task 的话，正常的执行过程会被中断，从这个时候开始只会进行以下操作，关闭定时器服务，进行 task 级别清理，所有算子的清理，常规 task 清理。

{% top %}