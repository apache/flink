---
title: 流式分析
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

# 流式分析

## Event Time and Watermarks

<a name="introduction"></a>

### 概要

Flink 明确支持以下三种时间语义:

* _事件时间(event time)：_ 事件产生的时间，记录的是设备生产(或者存储)事件的时间

* _摄取时间(ingestion time)：_ Flink 读取事件时记录的时间

* _处理时间(processing time)：_ Flink pipeline 中具体算子处理事件的时间

为了获得可重现的结果，例如在计算过去的特定一天里第一个小时股票的最高价格时，我们应该使用事件时间。这样的话，无论什么时间去计算都不会影响输出结果。然而如果使用处理时间的话，实时应用程序的结果是由程序运行的时间所决定。多次运行基于处理时间的实时程序，可能得到的结果都不相同，也可能会导致再次分析历史数据或者测试新代码变得异常困难。

<a name="working-with-event-time"></a>

### 使用 Event Time

如果想要使用事件时间，需要额外给 Flink 提供一个时间戳提取器和 Watermark 生成器，Flink 将使用它们来跟踪事件时间的进度。这将在选节[使用 Watermarks]({{< ref "docs/learn-flink/streaming_analytics" >}}#working-with-watermarks) 中介绍，但是首先我们需要解释一下 watermarks 是什么。

### Watermarks

让我们通过一个简单的示例来演示为什么需要 watermarks 及其工作方式。

在此示例中，我们将看到带有混乱时间戳的事件流，如下所示。显示的数字表达的是这些事件实际发生时间的时间戳。到达的第一个事件发生在时间 4，随后发生的事件发生在更早的时间 2，依此类推：

<div class="text-center" style="font-size: x-large; word-spacing: 0.5em; margin: 1em 0em;">
··· 23 19 22 24 21 14 17 13 12 15 9 11 7 2 4 →
</div>

假设我们要对数据流排序，我们想要达到的目的是：应用程序应该在数据流里的事件到达时就有一个算子（我们暂且称之为排序）开始处理事件，这个算子所输出的流是按照时间戳排序好的。

让我们重新审视这些数据:

(1) 我们的排序器看到的第一个事件的时间戳是 4，但是我们不能立即将其作为已排序的流释放。因为我们并不能确定它是有序的，并且较早的事件有可能并未到达。事实上，如果站在上帝视角，我们知道，必须要等到时间戳为 2 的元素到来时，排序器才可以有事件输出。

*需要一些缓冲，需要一些时间，但这都是值得的*

(2) 接下来的这一步，如果我们选择的是固执的等待，我们永远不会有结果。首先，我们看到了时间戳为 4 的事件，然后看到了时间戳为 2 的事件。可是，时间戳小于 2 的事件接下来会不会到来呢？可能会，也可能不会。再次站在上帝视角，我们知道，我们永远不会看到时间戳 1。

*最终，我们必须勇于承担责任，并发出指令，把带有时间戳 2 的事件作为已排序的事件流的开始*

(3)然后，我们需要一种策略，该策略定义：对于任何给定时间戳的事件，Flink 何时停止等待较早事件的到来。

*这正是 watermarks 的作用* — 它们定义何时停止等待较早的事件。

Flink 中事件时间的处理取决于 *watermark 生成器*，后者将带有时间戳的特殊元素插入流中形成 *watermarks*。事件时间 _t_ 的 watermark 代表 _t_ 之前（很可能）都已经到达。

当 watermark 以 2 或更大的时间戳到达时，事件流的排序器应停止等待，并输出 2 作为已经排序好的流。

(4) 我们可能会思考，如何决定 watermarks 的不同生成策略

每个事件都会延迟一段时间后到达，然而这些延迟有所不同，有些事件可能比其他事件延迟得更多。一种简单的方法是假定这些延迟受某个最大延迟的限制。Flink 将此策略称为 *最大无序边界 (bounded-out-of-orderness)* watermark。当然，我们可以想像出更好的生成 watermark 的方法，但是对于大多数应用而言，固定延迟策略已经足够了。

<a name="latency-vs-completeness"></a>
### 延迟 VS 正确性

watermarks 给了开发者流处理的一种选择，它们使开发人员在开发应用程序时可以控制延迟和完整性之间的权衡。与批处理不同，批处理中的奢侈之处在于可以在产生任何结果之前完全了解输入，而使用流式传输，我们不被允许等待所有的时间都产生了，才输出排序好的数据，这与流相违背。

我们可以把 watermarks 的边界时间配置的相对较短，从而冒着在输入了解不完全的情况下产生结果的风险-即可能会很快产生错误结果。或者，你可以等待更长的时间，并利用对输入流的更全面的了解来产生结果。

当然也可以实施混合解决方案，先快速产生初步结果，然后在处理其他（最新）数据时向这些结果提供更新。对于有一些对延迟的容忍程度很低，但是又对结果有很严格的要求的场景下，或许是一个福音。

<a name="latency"></a>

### 延迟

延迟是相对于 watermarks 定义的。`Watermark(t)` 表示事件流的时间已经到达了 _t_; watermark 之后的时间戳 &le; _t_ 的任何事件都被称之为延迟事件。

<a name="working-with-watermarks"></a>

### 使用 Watermarks

如果想要使用基于带有事件时间戳的事件流，Flink 需要知道与每个事件相关的时间戳，而且流必须包含 watermark。

动手练习中使用的出租车数据源已经为我们处理了这些详细信息。但是，在您自己的应用程序中，您将必须自己进行处理，这通常是通过实现一个类来实现的，该类从事件中提取时间戳，并根据需要生成 watermarks。最简单的方法是使用 `WatermarkStrategy`：

```java
DataStream<Event> stream = ...

WatermarkStrategy<Event> strategy = WatermarkStrategy
        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withTimestampAssigner((event, timestamp) -> event.timestamp);

DataStream<Event> withTimestampsAndWatermarks =
    stream.assignTimestampsAndWatermarks(strategy);
```

{{< top >}}

## Windows

Flink 在窗口的场景处理上非常有表现力。

在本节中，我们将学习：

* 如何使用窗口来计算无界流上的聚合,
* Flink 支持哪种类型的窗口，以及
* 如何使用窗口聚合来实现 DataStream 程序

<a name="introduction-1"></a>

### 概要

我们在操作无界数据流时，经常需要应对以下问题，我们经常把无界数据流分解成有界数据流聚合分析:

* 每分钟的浏览量
* 每位用户每周的会话数
* 每个传感器每分钟的最高温度

用 Flink 计算窗口分析取决于两个主要的抽象操作：_Window Assigners_，将事件分配给窗口（根据需要创建新的窗口对象），以及 _Window Functions_，处理窗口内的数据。

Flink 的窗口 API 还具有 _Triggers_ 和 _Evictors_ 的概念，_Triggers_ 确定何时调用窗口函数，而 _Evictors_ 则可以删除在窗口中收集的元素。

举一个简单的例子，我们一般这样使用键控事件流（基于 key 分组的输入事件流）：

```java
stream.
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce|aggregate|process(<window function>)
```

您不是必须使用键控事件流（keyed stream），但是值得注意的是，如果不使用键控事件流，我们的程序就不能 _并行_ 处理。

```java
stream.
    .windowAll(<window assigner>)
    .reduce|aggregate|process(<window function>)
```

<a name="window-assigners"></a>

### 窗口分配器

Flink 有一些内置的窗口分配器，如下所示：

{{< img src="/fig/window-assigners.svg" alt="Window assigners" class="center" width="80%" >}}

通过一些示例来展示关于这些窗口如何使用，或者如何区分它们：

* 滚动时间窗口
  * _每分钟页面浏览量_
  * `TumblingEventTimeWindows.of(Time.minutes(1))`
* 滑动时间窗口
  * _每10秒钟计算前1分钟的页面浏览量_
  * `SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10))`
* 会话窗口
  * _每个会话的网页浏览量，其中会话之间的间隔至少为30分钟_
  * `EventTimeSessionWindows.withGap(Time.minutes(30))`

以下都是一些可以使用的间隔时间 `Time.milliseconds(n)`, `Time.seconds(n)`, `Time.minutes(n)`,
 `Time.hours(n)`, 和 `Time.days(n)`。

基于时间的窗口分配器（包括会话时间）既可以处理 `事件时间`，也可以处理 `处理时间`。这两种基于时间的处理没有哪一个更好，我们必须折衷。使用 `处理时间`，我们必须接受以下限制：

* 无法正确处理历史数据,
* 无法正确处理超过最大无序边界的数据,
* 结果将是不确定的,

但是有自己的优势，较低的延迟。

使用基于计数的窗口时，请记住，只有窗口内的事件数量到达窗口要求的数值时，这些窗口才会触发计算。尽管可以使用自定义触发器自己实现该行为，但无法应对超时和处理部分窗口。

我们可能在有些场景下，想使用全局 window assigner 将每个事件（相同的 key）都分配给某一个指定的全局窗口。 很多情况下，一个比较好的建议是使用 `ProcessFunction`，具体介绍在[这里]({{< ref "docs/learn-flink/event_driven" >}}#process-functions)。

<a name="window-functions"></a>

### 窗口应用函数

我们有三种最基本的操作窗口内的事件的选项:

1. 像批量处理，`ProcessWindowFunction` 会缓存 `Iterable` 和窗口内容，供接下来全量计算；
1. 或者像流处理，每一次有事件被分配到窗口时，都会调用 `ReduceFunction` 或者 `AggregateFunction` 来增量计算；
1. 或者结合两者，通过 `ReduceFunction` 或者 `AggregateFunction` 预聚合的增量计算结果在触发窗口时，
   提供给 `ProcessWindowFunction` 做全量计算。

接下来展示一段 1 和 3 的示例，每一个实现都是计算传感器的最大值。在每一个一分钟大小的事件时间窗口内, 生成一个包含 `(key,end-of-window-timestamp, max_value)` 的一组结果。

<a name="processwindowfunction-example"></a>

#### ProcessWindowFunction 示例

```java
DataStream<SensorReading> input = ...

input
    .keyBy(x -> x.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(new MyWastefulMax());

public static class MyWastefulMax extends ProcessWindowFunction<
        SensorReading,                  // 输入类型
        Tuple3<String, Long, Integer>,  // 输出类型
        String,                         // 键类型
        TimeWindow> {                   // 窗口类型

    @Override
    public void process(
            String key,
            Context context,
            Iterable<SensorReading> events,
            Collector<Tuple3<String, Long, Integer>> out) {

        int max = 0;
        for (SensorReading event : events) {
            max = Math.max(event.value, max);
        }
        out.collect(Tuple3.of(key, context.window().getEnd(), max));
    }
}
```

在当前实现中有一些值得关注的地方：

* Flink 会缓存所有分配给窗口的事件流，直到触发窗口为止。这个操作可能是相当昂贵的。
* Flink 会传递给 `ProcessWindowFunction` 一个 `Context` 对象，这个对象内包含了一些窗口信息。`Context` 接口
  展示大致如下:

```java
public abstract class Context implements java.io.Serializable {
    public abstract W window();

    public abstract long currentProcessingTime();
    public abstract long currentWatermark();

    public abstract KeyedStateStore windowState();
    public abstract KeyedStateStore globalState();
}
```

`windowState` 和 `globalState` 可以用来存储当前的窗口的 key、窗口或者当前 key 的每一个窗口信息。这在一些场景下会很有用，试想，我们在处理当前窗口的时候，可能会用到上一个窗口的信息。

<a name="incremental-aggregation-example"></a>

#### 增量聚合示例

```java
DataStream<SensorReading> input = ...

input
    .keyBy(x -> x.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .reduce(new MyReducingMax(), new MyWindowFunction());

private static class MyReducingMax implements ReduceFunction<SensorReading> {
    public SensorReading reduce(SensorReading r1, SensorReading r2) {
        return r1.value() > r2.value() ? r1 : r2;
    }
}

private static class MyWindowFunction extends ProcessWindowFunction<
    SensorReading, Tuple3<String, Long, SensorReading>, String, TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<SensorReading> maxReading,
            Collector<Tuple3<String, Long, SensorReading>> out) {

        SensorReading max = maxReading.iterator().next();
        out.collect(Tuple3.of(key, context.window().getEnd(), max));
    }
}
```

请注意 `Iterable<SensorReading>` 将只包含一个读数 -- `MyReducingMax` 计算出的预先汇总的最大值。

<a name="late-events"></a>
### 晚到的事件

默认场景下，超过最大无序边界的事件会被删除，但是 Flink 给了我们两个选择去控制这些事件。

您可以使用一种称为[旁路输出]({{< ref "docs/learn-flink/event_driven" >}}#side-outputs) 的机制来安排将要删除的事件收集到侧输出流中，这里是一个示例:


```java
OutputTag<Event> lateTag = new OutputTag<Event>("late"){};

SingleOutputStreamOperator<Event> result = stream.
    .keyBy(...)
    .window(...)
    .sideOutputLateData(lateTag)
    .process(...);

DataStream<Event> lateStream = result.getSideOutput(lateTag);
```

我们还可以指定 _允许的延迟(allowed lateness)_ 的间隔，在这个间隔时间内，延迟的事件将会继续分配给窗口（同时状态会被保留），默认状态下，每个延迟事件都会导致窗口函数被再次调用（有时也称之为 _late firing_ ）。

默认情况下，允许的延迟为 0。换句话说，watermark 之后的元素将被丢弃（或发送到侧输出流）。

举例说明:

```java
stream.
    .keyBy(...)
    .window(...)
    .allowedLateness(Time.seconds(10))
    .process(...);
```

当允许的延迟大于零时，只有那些超过最大无序边界以至于会被丢弃的事件才会被发送到侧输出流（如果已配置）。

<a name="surprises"></a>
### 深入了解窗口操作

Flink 的窗口 API 某些方面有一些奇怪的行为，可能和我们预期的行为不一致。 根据 [Flink 用户邮件列表](https://flink.apache.org/community.html#mailing-lists) 和其他地方一些频繁被问起的问题, 以下是一些有关 Windows 的底层事实，这些信息可能会让您感到惊讶。

<a name="sliding-windows-make-copies"></a>
#### 滑动窗口是通过复制来实现的

滑动窗口分配器可以创建许多窗口对象，并将每个事件复制到每个相关的窗口中。例如，如果您每隔 15 分钟就有 24 小时的滑动窗口，则每个事件将被复制到 4 * 24 = 96 个窗口中。

<a name="time-windows-are-aligned-to-the-epoch"></a>
#### 时间窗口会和时间对齐

仅仅因为我们使用的是一个小时的处理时间窗口并在 12:05 开始运行您的应用程序，并不意味着第一个窗口将在 1:05 关闭。第一个窗口将长 55 分钟，并在 1:00 关闭。

 请注意，滑动窗口和滚动窗口分配器所采用的 offset 参数可用于改变窗口的对齐方式。有关详细的信息，请参见
[滚动窗口]({{< ref "docs/dev/datastream/operators/windows" >}}#tumbling-windows) 和
[滑动窗口]({{< ref "docs/dev/datastream/operators/windows" >}}#sliding-windows) 。

<a name="windows-can-follow-windows"></a>
#### window 后面可以接 window

比如说:

```java
stream
    .keyBy(t -> t.key)
    .window(<window assigner>)
    .reduce(<reduce function>)
    .windowAll(<same window assigner>)
    .reduce(<same reduce function>)
```

可能我们会猜测以 Flink 的能力，想要做到这样看起来是可行的（前提是你使用的是 ReduceFunction 或 AggregateFunction ），但不是。

之所以可行，是因为时间窗口产生的事件是根据窗口结束时的时间分配时间戳的。例如，一个小时小时的窗口所产生的所有事件都将带有标记一个小时结束的时间戳。后面的窗口内的数据消费和前面的流产生的数据是一致的。

<a name="no-results-for-empty-timewindows"></a>

#### 空的时间窗口不会输出结果

事件会触发窗口的创建。换句话说，如果在特定的窗口内没有事件，就不会有窗口，就不会有输出结果。

#### Late Events Can Cause Late Merges

会话窗口的实现是基于窗口的一个抽象能力，窗口可以 _聚合_。会话窗口中的每个数据在初始被消费时，都会被分配一个新的窗口，但是如果窗口之间的间隔足够小，多个窗口就会被聚合。延迟事件可以弥合两个先前分开的会话间隔，从而产生一个虽然有延迟但是更加准确地结果。

{{< top >}}

<a name="hands-on"></a>

## 实践练习

本节附带的动手练习是 {{< training_link file="/hourly-tips" name="Hourly Tips Exercise">}}.

{{< top >}}

<a name="further-reading"></a>
## 延伸阅读

- [Timely Stream Processing]({{< ref "docs/concepts/time" >}})
- [Windows]({{< ref "docs/dev/datastream/operators/windows" >}})

{{< top >}}
