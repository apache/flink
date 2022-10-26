---
title: 窗口
weight: 2 
type: docs
aliases:
  - /zh/dev/stream/operators/windows.html
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

# 窗口

窗口（Window）是处理无界流的关键所在。窗口可以将数据流装入大小有限的“桶”中，再对每个“桶”加以处理。
本文的重心将放在 Flink 如何进行窗口操作以及开发者如何尽可能地利用 Flink 所提供的功能。

下面展示了 Flink 窗口在 *keyed* streams 和 *non-keyed* streams 上使用的基本结构。
我们可以看到，这两者唯一的区别仅在于：keyed streams 要调用  `keyBy(...)`后再调用 `window(...)` ，
而 non-keyed streams 只用直接调用 `windowAll(...)`。留意这个区别，它能帮我们更好地理解后面的内容。

**Keyed Windows**

{{< tabs "Keyed Windows" >}}

{{< tab "Java/Scala" >}}
    stream
           .keyBy(...)               <-  仅 keyed 窗口需要
           .window(...)              <-  必填项："assigner"
          [.trigger(...)]            <-  可选项："trigger" (省略则使用默认 trigger)
          [.evictor(...)]            <-  可选项："evictor" (省略则不使用 evictor)
          [.allowedLateness(...)]    <-  可选项："lateness" (省略则为 0)
          [.sideOutputLateData(...)] <-  可选项："output tag" (省略则不对迟到数据使用 side output)
           .reduce/aggregate/apply()      <-  必填项："function"
          [.getSideOutput(...)]      <-  可选项："output tag"

{{< /tab >}}

{{< tab "Python" >}}
    stream
           .key_by(...)                  <-  仅 keyed 窗口需要
           .window(...)                  <-  必填项："assigner"
          [.trigger(...)]                <-  可选项："trigger" (省略则使用默认 trigger)
          [.allowed_lateness(...)]       <-  可选项："lateness" (省略则为 0)
          [.side_output_late_data(...)]  <-  可选项："output tag" (省略则不对迟到数据使用 side output)
           .reduce/aggregate/apply()     <-  必填项："function"
          [.get_side_output(...)]        <-  可选项："output tag"

{{< /tab >}}
{{< /tabs >}}

**Non-Keyed Windows**

{{< tabs "Non-Keyed Windows" >}}

{{< tab "Java/Scala" >}}
    stream
           .windowAll(...)           <-  必填项："assigner"
          [.trigger(...)]            <-  可选项："trigger" (else default trigger)
          [.evictor(...)]            <-  可选项："evictor" (else no evictor)
          [.allowedLateness(...)]    <-  可选项："lateness" (else zero)
          [.sideOutputLateData(...)] <-  可选项："output tag" (else no side output for late data)
           .reduce/aggregate/apply()      <-  必填项："function"
          [.getSideOutput(...)]      <-  可选项："output tag"

{{< /tab >}}

{{< tab "Python" >}}
    stream
           .window_all(...)             <-  必填项："assigner"
          [.trigger(...)]               <-  可选项："trigger" (else default trigger)
          [.allowed_lateness(...)]      <-  可选项："lateness" (else zero)
          [.side_output_late_data(...)] <-  可选项："output tag" (else no side output for late data)
           .reduce/aggregate/apply()    <-  必填项："function"
          [.get_side_output(...)]       <-  可选项："output tag"

{{< /tab >}}

{{< /tabs >}}

上面方括号（[...]）中的命令是可选的。也就是说，Flink 允许你自定义多样化的窗口操作来满足你的需求。
{{< hint info >}} Note: `Evictor` 在 Python DataStream API 中还不支持. {{< /hint >}}


## 窗口的生命周期

简单来说，一个窗口在第一个属于它的元素到达时就会被**创建**，然后在时间（event 或 processing time）
超过窗口的“结束时间戳 + 用户定义的 `allowed lateness` （详见 [Allowed Lateness](#allowed-lateness)）”时
被**完全删除**。Flink 仅保证删除基于时间的窗口，其他类型的窗口不做保证，
比如全局窗口（详见 [Window Assigners](#window-assigners)）。
例如，对于一个基于 event time 且范围互不重合（滚动）的窗口策略，
如果窗口设置的时长为五分钟、可容忍的迟到时间（allowed lateness）为 1 分钟，
那么第一个元素落入 `12:00` 至 `12:05` 这个区间时，Flink 就会为这个区间创建一个新的窗口。
当 watermark 越过 `12:06` 时，这个窗口将被摧毁。

另外，每个窗口会设置自己的 `Trigger` （详见 [Triggers](#triggers)）和 function 
(`ProcessWindowFunction`、`ReduceFunction`、或 `AggregateFunction`，
详见 [Window Functions](#窗口函数window-functions)）。该 function 决定如何计算窗口中的内容，
而 `Trigger` 决定何时窗口中的数据可以被 function 计算。
Trigger 的触发（fire）条件可能是“当窗口中有多于 4 条数据”或“当 watermark 越过窗口的结束时间”等。
Trigger 还可以在 window 被创建后、删除前的这段时间内定义何时清理（purge）窗口中的数据。
这里的数据仅指窗口内的元素，不包括窗口的 meta data。也就是说，窗口在 purge 后仍然可以加入新的数据。

除此之外，你也可以指定一个 `Evictor` （详见 [Evictors](#evictors)），在 trigger 触发之后，Evictor 可以在窗口函数的前后删除数据。

接下来我们会更详细地介绍上面提到的内容。开头的例子中有必填项和可选项。
我们先从必填项开始（详见 [Keyed vs Non-Keyed Windows](#keyed-和-non-keyed-windows)、
[Window Assigners](#window-assigners)、[Window Functions](#窗口函数window-functions)）。

## Keyed 和 Non-Keyed Windows

首先必须要在定义窗口前确定的是你的 stream 是 keyed 还是 non-keyed。
`keyBy(...)` 会将你的无界 stream 分割为逻辑上的 keyed stream。
如果 `keyBy(...)` 没有被调用，你的 stream 就不是 keyed。

对于 keyed stream，其中数据的任何属性都可以作为 key
（详见[此处]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-datastream)）。
使用 keyed stream 允许你的窗口计算由多个 task 并行，因为每个逻辑上的 keyed stream 都可以被单独处理。
属于同一个 key 的元素会被发送到同一个 task。

对于 non-keyed stream，原始的 stream 不会被分割为多个逻辑上的 stream，
所以所有的窗口计算会被同一个 task 完成，也就是 parallelism 为 1。

## Window Assigners

指定了你的 stream 是否为 keyed 之后，下一步就是定义 *window assigner*。

Window assigner 定义了 stream 中的元素如何被分发到各个窗口。
你可以在 `window(...)`（用于 *keyed* streams）或 `windowAll(...)`
（用于 non-keyed streams）中指定一个 `WindowAssigner`。
`WindowAssigner` 负责将 stream 中的每个数据分发到一个或多个窗口中。
Flink 为最常用的情况提供了一些定义好的 window assigner，也就是  *tumbling windows*、
*sliding windows*、 *session windows* 和 *global windows*。
你也可以继承 `WindowAssigner` 类来实现自定义的 window assigner。
所有内置的 window assigner（除了 global window）都是基于时间分发数据的，processing time 或 event time 均可。
请阅读我们对于 [event time]({{< ref "docs/concepts/time" >}}) 的介绍来了解这两者的区别，
以及 timestamp 和 watermark 是如何产生的。

基于时间的窗口用 *start timestamp*（包含）和 *end timestamp*（不包含）描述窗口的大小。
在代码中，Flink 处理基于时间的窗口使用的是 `TimeWindow`，
它有查询开始和结束 timestamp 以及返回窗口所能储存的最大 timestamp 的方法 `maxTimestamp()`。

接下来我们会说明 Flink 内置的 window assigner 如何工作，以及他们如何用在 DataStream 程序中。
下面的图片展示了每种 assigner 如何工作。
紫色的圆圈代表 stream 中按 key 划分的元素（本例中是按 *user 1*、*user 2*  和 *user 3* 划分）。
x 轴表示时间的进展。

### 滚动窗口（Tumbling Windows）

滚动窗口的 assigner 分发元素到指定大小的窗口。滚动窗口的大小是固定的，且各自范围之间不重叠。
比如说，如果你指定了滚动窗口的大小为 5 分钟，那么每 5 分钟就会有一个窗口被计算，且一个新的窗口被创建（如下图所示）。

{{< img src="/fig/tumbling-windows.svg" alt="Tumbling Windows" >}}

下面的代码展示了如何使用滚动窗口。

{{< tabs "cb126c86-cbcc-4d11-bfac-ccf663073c38" >}}
{{< tab "Java" >}}
```java
DataStream<T> input = ...;

// 滚动 event-time 窗口
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滚动 processing-time 窗口
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 长度为一天的滚动 event-time 窗口， 偏移量为 -8 小时。
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[T] = ...

// 滚动 event-time 窗口
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// 滚动 processing-time 窗口
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// 长度为一天的滚动 event-time 窗口，偏移量为 -8 小时。
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

# 滚动 event-time 窗口
input \
    .key_by(<key selector>) \
    .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
    .<windowed transformation>(<window function>)

# 滚动 processing-time 窗口
input \
    .key_by(<key selector>) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
    .<windowed transformation>(<window function>)

# 长度为一天的滚动 event-time 窗口，偏移量为 -8 小时。
input \
    .key_by(<key selector>) \
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) \
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< /tabs >}}

时间间隔可以用 `Time.milliseconds(x)`、`Time.seconds(x)`、`Time.minutes(x)` 等来指定。

如上一个例子所示，滚动窗口的 assigners 也可以传入可选的 `offset` 参数。这个参数可以用来对齐窗口。
比如说，不设置 offset 时，长度为一小时的滚动窗口会与 linux 的 epoch 对齐。
你会得到如 `1:00:00.000 - 1:59:59.999`、`2:00:00.000 - 2:59:59.999` 等。
如果你想改变对齐方式，你可以设置一个 offset。如果设置了 15 分钟的 offset，
你会得到 `1:15:00.000 - 2:14:59.999`、`2:15:00.000 - 3:14:59.999` 等。
一个重要的 offset 用例是根据 UTC-0 调整窗口的时差。比如说，在中国你可能会设置 offset 为 `Time.hours(-8)`。

### 滑动窗口（Sliding Windows）

与滚动窗口类似，滑动窗口的 assigner 分发元素到指定大小的窗口，窗口大小通过 *window size*  参数设置。
滑动窗口需要一个额外的滑动距离（*window slide*）参数来控制生成新窗口的频率。
因此，如果 slide 小于窗口大小，滑动窗口可以允许窗口重叠。这种情况下，一个元素可能会被分发到多个窗口。

比如说，你设置了大小为 10 分钟，滑动距离 5 分钟的窗口，你会在每 5 分钟得到一个新的窗口，
里面包含之前 10 分钟到达的数据（如下图所示）。

{{< img src="/fig/sliding-windows.svg" alt="sliding windows" >}}

下面的代码展示了如何使用滑动窗口。

{{< tabs "89a9d497-6404-4333-b7bc-c8a465620279" >}}
{{< tab "Java" >}}
```java
DataStream<T> input = ...;

// 滑动 event-time 窗口
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滑动 processing-time 窗口
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滑动 processing-time 窗口，偏移量为 -8 小时
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[T] = ...

// 滑动 event-time 窗口
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// 滑动 processing-time 窗口
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// 滑动 processing-time 窗口，偏移量为 -8 小时
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

# 滑动 event-time 窗口
input \
    .key_by(<key selector>) \
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
    .<windowed transformation>(<window function>)

# 滑动 processing-time 窗口
input \
    .key_by(<key selector>) \
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
    .<windowed transformation>(<window function>)

# 滑动 processing-time 窗口，偏移量为 -8 小时
input \
    .key_by(<key selector>) \
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8))) \
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< /tabs >}}

时间间隔可以使用 `Time.milliseconds(x)`、`Time.seconds(x)`、`Time.minutes(x)` 等来指定。

如上一个例子所示，滚动窗口的 assigners 也可以传入可选的 `offset` 参数。这个参数可以用来对齐窗口。
比如说，不设置 offset 时，长度为一小时、滑动距离为 30 分钟的滑动窗口会与 linux 的 epoch 对齐。
你会得到如 `1:00:00.000 - 1:59:59.999`, `1:30:00.000 - 2:29:59.999` 等。
如果你想改变对齐方式，你可以设置一个 offset。
如果设置了 15 分钟的 offset，你会得到 `1:15:00.000 - 2:14:59.999`、`1:45:00.000 - 2:44:59.999` 等。
一个重要的 offset 用例是根据 UTC-0 调整窗口的时差。比如说，在中国你可能会设置 offset 为 `Time.hours(-8)`。

### 会话窗口（Session Windows）

*会话窗口*的 assigner 会把数据按活跃的会话分组。
与*滚动窗口*和*滑动窗口*不同，会话窗口不会相互重叠，且没有固定的开始或结束时间。
会话窗口在一段时间没有收到数据之后会关闭，即在一段不活跃的间隔之后。
会话窗口的 assigner 可以设置固定的会话间隔（session gap）或
用 *session gap extractor* 函数来动态地定义多长时间算作不活跃。
当超出了不活跃的时间段，当前的会话就会关闭，并且将接下来的数据分发到新的会话窗口。

{{< img src="/fig/session-windows.svg" alt="session windows" >}} 

下面的代码展示了如何使用会话窗口。

{{< tabs "9178a99c-4a54-491a-8182-499d23a7432c" >}}
{{< tab "Java" >}}
```java
DataStream<T> input = ...;

// 设置了固定间隔的 event-time 会话窗口
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);
    
// 设置了动态间隔的 event-time 会话窗口
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withDynamicGap((element) -> {
        // 决定并返回会话间隔
    }))
    .<windowed transformation>(<window function>);

// 设置了固定间隔的 processing-time session 窗口
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);
    
// 设置了动态间隔的 processing-time 会话窗口
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withDynamicGap((element) -> {
        // 决定并返回会话间隔
    }))
    .<windowed transformation>(<window function>);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[T] = ...

// 设置了固定间隔的 event-time 会话窗口
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)

// 设置了动态间隔的 event-time 会话窗口
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[String] {
      override def extract(element: String): Long = {
        // 决定并返回会话间隔
      }
    }))
    .<windowed transformation>(<window function>)

// 设置了固定间隔的 processing-time 会话窗口
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)


// 设置了动态间隔的 processing-time 会话窗口
input
    .keyBy(<key selector>)
    .window(DynamicProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[String] {
      override def extract(element: String): Long = {
        // 决定并返回会话间隔
      }
    }))
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

class MySessionWindowTimeGapExtractor(SessionWindowTimeGapExtractor):

    def extract(self, element: tuple) -> int:
        # 决定并返回会话间隔

# 设置了固定间隔的 event-time 会话窗口
input \
    .key_by(<key selector>) \
    .window(EventTimeSessionWindows.with_gap(Time.minutes(10))) \
    .<windowed transformation>(<window function>)

# 设置了动态间隔的 event-time 会话窗口
input \
    .key_by(<key selector>) \
    .window(EventTimeSessionWindows.with_dynamic_gap(MySessionWindowTimeGapExtractor())) \
    .<windowed transformation>(<window function>)

# 设置了固定间隔的 processing-time 会话窗口
input \
    .key_by(<key selector>) \
    .window(ProcessingTimeSessionWindows.with_gap(Time.minutes(10))) \
    .<windowed transformation>(<window function>)

# 设置了动态间隔的 processing-time 会话窗口
input \
    .key_by(<key selector>) \
    .window(DynamicProcessingTimeSessionWindows.with_dynamic_gap(MySessionWindowTimeGapExtractor())) \
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< /tabs >}}

固定间隔可以使用 `Time.milliseconds(x)`、`Time.seconds(x)`、`Time.minutes(x)` 等来设置。

动态间隔可以通过实现 `SessionWindowTimeGapExtractor` 接口来指定。

{{< hint info >}}
会话窗口并没有固定的开始或结束时间，所以它的计算方法与滑动窗口和滚动窗口不同。在 Flink 内部，会话窗口的算子会为每一条数据创建一个窗口，
然后将距离不超过预设间隔的窗口合并。
想要让窗口可以被合并，会话窗口需要拥有支持合并的 [Trigger](#triggers) 和 [Window Function](#窗口函数window-functions)，
比如说 `ReduceFunction`、`AggregateFunction` 或 `ProcessWindowFunction`。
{{< /hint >}}

### 全局窗口（Global Windows）

*全局窗口*的 assigner 将拥有相同 key 的所有数据分发到一个*全局窗口*。
这样的窗口模式仅在你指定了自定义的 [trigger](#triggers) 时有用。
否则，计算不会发生，因为全局窗口没有天然的终点去触发其中积累的数据。

{{< img src="/fig/non-windowed.svg" alt="global windows" >}}

下面的代码展示了如何使用全局窗口。

{{< tabs "3f113886-ba6f-46eb-96fe-db56292a8285" >}}
{{< tab "Java" >}}
```java
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[T] = ...

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input \
    .key_by(<key selector>) \
    .window(GlobalWindows.create()) \
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< /tabs >}}

## 窗口函数（Window Functions）

定义了 window assigner 之后，我们需要指定当窗口触发之后，我们如何计算每个窗口中的数据，
这就是 *window function* 的职责了。关于窗口如何触发，详见 [triggers](#triggers)。

窗口函数有三种：`ReduceFunction`、`AggregateFunction` 或 `ProcessWindowFunction`。
前两者执行起来更高效（详见 [State Size](#关于状态大小的考量)）因为 Flink 可以在每条数据到达窗口后
进行增量聚合（incrementally aggregate）。
而 `ProcessWindowFunction` 会得到能够遍历当前窗口内所有数据的 `Iterable`，以及关于这个窗口的 meta-information。

使用 `ProcessWindowFunction` 的窗口转换操作没有其他两种函数高效，因为 Flink 在窗口触发前必须缓存里面的*所有*数据。
`ProcessWindowFunction` 可以与 `ReduceFunction` 或 `AggregateFunction` 合并来提高效率。
这样做既可以增量聚合窗口内的数据，又可以从 `ProcessWindowFunction` 接收窗口的 metadata。
我们接下来看看每种函数的例子。

### ReduceFunction

`ReduceFunction` 指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同。
Flink 使用 `ReduceFunction` 对窗口中的数据进行增量聚合。

`ReduceFunction` 可以像下面这样定义：

{{< tabs "e49c2bc0-a4cf-4ead-acbc-d96a98e1c6ff" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce(new ReduceFunction<Tuple2<String, Long>>() {
      public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
      }
    });
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .reduce(lambda v1, v2: (v1[0], v1[1] + v2[1]),
            output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))
```
{{< /tab >}}
{{< /tabs >}}

上面的例子是对窗口内元组的第二个属性求和。

### AggregateFunction

`ReduceFunction` 是 `AggregateFunction` 的特殊情况。
`AggregateFunction` 接收三个类型：输入数据的类型(`IN`)、累加器的类型（`ACC`）和输出数据的类型（`OUT`）。
输入数据的类型是输入流的元素类型，`AggregateFunction` 接口有如下几个方法：
把每一条元素加进累加器、创建初始累加器、合并两个累加器、从累加器中提取输出（`OUT` 类型）。我们通过下例说明。

与 `ReduceFunction` 相同，Flink 会在输入数据到达窗口时直接进行增量聚合。

`AggregateFunction` 可以像下面这样定义：

{{< tabs "7084ece9-370e-42e3-8130-e47cc9a6c600" >}}
{{< tab "Java" >}}
```java

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
private static class AverageAggregate
    implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
  @Override
  public Tuple2<Long, Long> createAccumulator() {
    return new Tuple2<>(0L, 0L);
  }

  @Override
  public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
    return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
  }

  @Override
  public Double getResult(Tuple2<Long, Long> accumulator) {
    return ((double) accumulator.f0) / accumulator.f1;
  }

  @Override
  public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
  }
}

DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .aggregate(new AverageAggregate());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

/**
 * The accumulator is used to keep a running sum and a count. The [getResult] method
 * computes the average.
 */
class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def createAccumulator() = (0L, 0L)

  override def add(value: (String, Long), accumulator: (Long, Long)) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  override def getResult(accumulator: (Long, Long)) = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}

val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .aggregate(new AverageAggregate)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class AverageAggregate(AggregateFunction):
 
    def create_accumulator(self) -> Tuple[int, int]:
        return 0, 0

    def add(self, value: Tuple[str, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return accumulator[0] + value[1], accumulator[1] + 1

    def get_result(self, accumulator: Tuple[int, int]) -> float:
        return accumulator[0] / accumulator[1]

    def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        return a[0] + b[0], a[1] + b[1]

input = ...  # type: DataStream

input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .aggregate(AverageAggregate(),
               accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
               output_type=Types.DOUBLE())
```
{{< /tab >}}
{{< /tabs >}}

上例计算了窗口内所有元素第二个属性的平均值。

### ProcessWindowFunction

ProcessWindowFunction 有能获取包含窗口内所有元素的 Iterable，
以及用来获取时间和状态信息的 Context 对象，比其他窗口函数更加灵活。
ProcessWindowFunction 的灵活性是以性能和资源消耗为代价的，
因为窗口中的数据无法被增量聚合，而需要在窗口触发前缓存所有数据。

`ProcessWindowFunction` 的签名如下：

{{< tabs "ce96f848-dcbf-4f8b-a079-afc301036da2" >}}
{{< tab "Java" >}}
```java
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> implements Function {

    /**
     * Evaluates the window and outputs none or several elements.
     *
     * @param key The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     *
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public abstract void process(
            KEY key,
            Context context,
            Iterable<IN> elements,
            Collector<OUT> out) throws Exception;

    /**
     * Deletes any state in the {@code Context} when the Window expires (the watermark passes its
     * {@code maxTimestamp} + {@code allowedLateness}).
     *
     * @param context The context to which the window is being evaluated
     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
     */
    public void clear(Context context) throws Exception {}

    /**
     * The context holding window metadata.
     */
    public abstract class Context implements java.io.Serializable {
        /**
         * Returns the window that is being evaluated.
         */
        public abstract W window();

        /** Returns the current processing time. */
        public abstract long currentProcessingTime();

        /** Returns the current event-time watermark. */
        public abstract long currentWatermark();

        /**
         * State accessor for per-key and per-window state.
         *
         * <p><b>NOTE:</b>If you use per-window state you have to ensure that you clean it up
         * by implementing {@link ProcessWindowFunction#clear(Context)}.
         */
        public abstract KeyedStateStore windowState();

        /**
         * State accessor for per-key global state.
         */
        public abstract KeyedStateStore globalState();
    }

}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window] extends Function {

  /**
    * Evaluates the window and outputs none or several elements.
    *
    * @param key      The key for which this window is evaluated.
    * @param context  The context in which the window is being evaluated.
    * @param elements The elements in the window being evaluated.
    * @param out      A collector for emitting elements.
    * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
    */
  def process(
      key: KEY,
      context: Context,
      elements: Iterable[IN],
      out: Collector[OUT])

  /**
   * Deletes any state in the [[Context]] when the Window expires
   * (the watermark passes its `maxTimestamp` + `allowedLateness`).
   *
   * @param context The context to which the window is being evaluated
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  @throws[Exception]
  def clear(context: Context) {}

  /**
    * The context holding window metadata
    */
  abstract class Context {
    /**
      * Returns the window that is being evaluated.
      */
    def window: W

    /**
      * Returns the current processing time.
      */
    def currentProcessingTime: Long

    /**
      * Returns the current event-time watermark.
      */
    def currentWatermark: Long

    /**
      * State accessor for per-key and per-window state.
      */
    def windowState: KeyedStateStore

    /**
      * State accessor for per-key global state.
      */
    def globalState: KeyedStateStore
  }

}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class ProcessWindowFunction(Function, Generic[IN, OUT, KEY, W]):

    @abstractmethod
    def process(self,
                key: KEY,
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[IN]) -> Iterable[OUT]:
        """
        Evaluates the window and outputs none or several elements.
    
        :param key: The key for which this window is evaluated.
        :param context: The context in which the window is being evaluated.
        :param elements: The elements in the window being evaluated.
        :return: The iterable object which produces the elements to emit.
        """
        pass

    @abstractmethod
    def clear(self, context: 'ProcessWindowFunction.Context') -> None:
        """
        Deletes any state in the :class:`Context` when the Window expires (the watermark passes its
        max_timestamp + allowed_lateness).
    
        :param context: The context to which the window is being evaluated.
        """
        pass

    class Context(ABC, Generic[W2]):
        """
        The context holding window metadata.
        """
    
        @abstractmethod
        def window(self) -> W2:
            """
            :return: The window that is being evaluated.
            """
            pass
    
        @abstractmethod
        def current_processing_time(self) -> int:
            """
            :return: The current processing time.
            """
            pass
    
        @abstractmethod
        def current_watermark(self) -> int:
            """
            :return: The current event-time watermark.
            """
            pass
    
        @abstractmethod
        def window_state(self) -> KeyedStateStore:
            """
            State accessor for per-key and per-window state.
      
            .. note::
                If you use per-window state you have to ensure that you clean it up by implementing
                :func:`~ProcessWindowFunction.clear`.
      
            :return: The :class:`KeyedStateStore` used to access per-key and per-window states.
            """
            pass
    
        @abstractmethod
        def global_state(self) -> KeyedStateStore:
            """
            State accessor for per-key global state.
            """
            pass
```
{{< /tab >}}
{{< /tabs >}}

`key` 参数由 `keyBy()` 中指定的 `KeySelector` 选出。
如果是给出 key 在 tuple 中的 index 或用属性名的字符串形式指定 key，这个 key 的类型将总是 `Tuple`，
并且你需要手动将它转换为正确大小的 tuple 才能提取 key。

`ProcessWindowFunction` 可以像下面这样定义：

{{< tabs "23f086d2-fc10-4dc6-9edc-0e69a2deefdb" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, Long>> input = ...;

input
  .keyBy(t -> t.f0)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .process(new MyProcessWindowFunction());

/* ... */

public class MyProcessWindowFunction 
    extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

  @Override
  public void process(String key, Context context, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
    long count = 0;
    for (Tuple2<String, Long> in: input) {
      count++;
    }
    out.collect("Window: " + context.window() + "count: " + count);
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[(String, Long)] = ...

input
  .keyBy(_._1)
  .window(TumblingEventTimeWindows.of(Time.minutes(5)))
  .process(new MyProcessWindowFunction())

/* ... */

class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

  def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"Window ${context.window} count: $count")
  }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input \
    .key_by(lambda v: v[0]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .process(MyProcessWindowFunction())

# ...

class MyProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
        count = 0
        for _ in elements:
            count += 1
        yield "Window: {} count: {}".format(context.window(), count)
```
{{< /tab >}}
{{< /tabs >}}

上例使用 `ProcessWindowFunction` 对窗口中的元素计数，并且将窗口本身的信息一同输出。

{{< hint info >}}
注意，使用 `ProcessWindowFunction` 完成简单的聚合任务是非常低效的。
下一章会说明如何将 `ReduceFunction` 或 `AggregateFunction` 与 `ProcessWindowFunction` 组合成既能
增量聚合又能获得窗口额外信息的窗口函数。
{{< /hint >}}

### 增量聚合的 ProcessWindowFunction

`ProcessWindowFunction` 可以与 `ReduceFunction` 或 `AggregateFunction` 搭配使用，
使其能够在数据到达窗口的时候进行增量聚合。当窗口关闭时，`ProcessWindowFunction` 将会得到聚合的结果。
这样它就可以增量聚合窗口的元素并且从 ProcessWindowFunction` 中获得窗口的元数据。

你也可以对过时的 `WindowFunction` 使用增量聚合。

#### 使用 ReduceFunction 增量聚合

下例展示了如何将 `ReduceFunction` 与 `ProcessWindowFunction` 组合，返回窗口中的最小元素和窗口的开始时间。

{{< tabs "de5305c3-7e43-4a83-a63e-6eb93be8eb9b" >}}
{{< tab "Java" >}}
```java
DataStream<SensorReading> input = ...;

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .reduce(new MyReduceFunction(), new MyProcessWindowFunction());

// Function definitions

private static class MyReduceFunction implements ReduceFunction<SensorReading> {

  public SensorReading reduce(SensorReading r1, SensorReading r2) {
      return r1.value() > r2.value() ? r2 : r1;
  }
}

private static class MyProcessWindowFunction
    extends ProcessWindowFunction<SensorReading, Tuple2<Long, SensorReading>, String, TimeWindow> {

  public void process(String key,
                    Context context,
                    Iterable<SensorReading> minReadings,
                    Collector<Tuple2<Long, SensorReading>> out) {
      SensorReading min = minReadings.iterator().next();
      out.collect(new Tuple2<Long, SensorReading>(context.window().getStart(), min));
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val input: DataStream[SensorReading] = ...

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .reduce(
    (r1: SensorReading, r2: SensorReading) => { if (r1.value > r2.value) r2 else r1 },
    ( key: String,
      context: ProcessWindowFunction[_, _, _, TimeWindow]#Context,
      minReadings: Iterable[SensorReading],
      out: Collector[(Long, SensorReading)] ) =>
      {
        val min = minReadings.iterator.next()
        out.collect((context.window.getStart, min))
      }
  )

```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .reduce(lambda r1, r2: r2 if r1.value > r2.value else r1,
            window_function=MyProcessWindowFunction(),
            output_type=Types.TUPLE([Types.STRING(), Types.LONG()]))

# Function definition

class MyProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: str, context: ProcessWindowFunction.Context,
                min_readings: Iterable[SensorReading]) -> Iterable[Tuple[int, SensorReading]]:
        min = next(iter(min_readings))
        yield context.window().start, min
```
{{< /tab >}}
{{< /tabs >}}

#### 使用 AggregateFunction 增量聚合

下例展示了如何将 `AggregateFunction` 与 `ProcessWindowFunction` 组合，计算平均值并与窗口对应的 key 一同输出。

{{< tabs "404eb3d8-042c-4aef-8175-0ebc8c34cf01" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, Long>> input = ...;

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .aggregate(new AverageAggregate(), new MyProcessWindowFunction());

// Function definitions

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
private static class AverageAggregate
    implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
  @Override
  public Tuple2<Long, Long> createAccumulator() {
    return new Tuple2<>(0L, 0L);
  }

  @Override
  public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
    return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
  }

  @Override
  public Double getResult(Tuple2<Long, Long> accumulator) {
    return ((double) accumulator.f0) / accumulator.f1;
  }

  @Override
  public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
  }
}

private static class MyProcessWindowFunction
    extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {

  public void process(String key,
                    Context context,
                    Iterable<Double> averages,
                    Collector<Tuple2<String, Double>> out) {
      Double average = averages.iterator().next();
      out.collect(new Tuple2<>(key, average));
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val input: DataStream[(String, Long)] = ...

input
  .keyBy(<key selector>)
  .window(<window assigner>)
  .aggregate(new AverageAggregate(), new MyProcessWindowFunction())

// Function definitions

/**
 * The accumulator is used to keep a running sum and a count. The [getResult] method
 * computes the average.
 */
class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def createAccumulator() = (0L, 0L)

  override def add(value: (String, Long), accumulator: (Long, Long)) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  override def getResult(accumulator: (Long, Long)) = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}

class MyProcessWindowFunction extends ProcessWindowFunction[Double, (String, Double), String, TimeWindow] {

  def process(key: String, context: Context, averages: Iterable[Double], out: Collector[(String, Double)]) = {
    val average = averages.iterator.next()
    out.collect((key, average))
  }
}

```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .aggregate(AverageAggregate(),
               window_function=MyProcessWindowFunction(),
               accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
               output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))

# Function definitions

class AverageAggregate(AggregateFunction):
    """
    The accumulator is used to keep a running sum and a count. The :func:`get_result` method
    computes the average.
    """

    def create_accumulator(self) -> Tuple[int, int]:
        return 0, 0

    def add(self, value: Tuple[str, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return accumulator[0] + value[1], accumulator[1] + 1

    def get_result(self, accumulator: Tuple[int, int]) -> float:
        return accumulator[0] / accumulator[1]

    def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        return a[0] + b[0], a[1] + b[1]

class MyProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: str, context: ProcessWindowFunction.Context,
                averages: Iterable[float]) -> Iterable[Tuple[str, float]]:
        average = next(iter(averages))
        yield key, average

```
{{< /tab >}}
{{< /tabs >}}

### 在 ProcessWindowFunction 中使用 per-window state

除了访问 keyed state （任何富函数都可以），`ProcessWindowFunction` 还可以使用作用域仅为
“当前正在处理的窗口”的 keyed state。在这种情况下，理解 *per-window* 中的 window 指的是什么非常重要。
总共有以下几种窗口的理解：

 - 在窗口操作中定义的窗口：比如定义了*长一小时的滚动窗口*或*长两小时、滑动一小时的滑动窗口*。
 - 对应某个 key 的窗口实例：比如 *以 user-id xyz 为 key，从 12:00 到 13:00 的时间窗口*。
 具体情况取决于窗口的定义，根据具体的 key 和时间段会产生诸多不同的窗口实例。

Per-window state 作用于后者。也就是说，如果我们处理有 1000 种不同 key 的事件，
并且目前所有事件都处于 *[12:00, 13:00)* 时间窗口内，那么我们将会得到 1000 个窗口实例，
且每个实例都有自己的 keyed per-window state。

`process()` 接收到的 `Context` 对象中有两个方法允许我们访问以下两种 state：

 - `globalState()`，访问全局的 keyed state
 - `windowState()`, 访问作用域仅限于当前窗口的 keyed state

如果你可能将一个 window 触发多次（比如当你的迟到数据会再次触发窗口计算，
或你自定义了根据推测提前触发窗口的 trigger），那么这个功能将非常有用。
这时你可能需要在 per-window state 中储存关于之前触发的信息或触发的总次数。

当使用窗口状态时，一定记得在删除窗口时清除这些状态。他们应该定义在 `clear()` 方法中。

### WindowFunction（已过时）

在某些可以使用 `ProcessWindowFunction` 的地方，你也可以使用 `WindowFunction`。
它是旧版的 `ProcessWindowFunction`，只能提供更少的环境信息且缺少一些高级的功能，比如 per-window state。
这个接口会在未来被弃用。

`WindowFunction` 的签名如下：

{{< tabs "cfdb7c7a-7a5d-4ac3-b92d-b0f5aa32240f" >}}
{{< tab "Java" >}}
```java
public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param key The key for which this window is evaluated.
   * @param window The window that is being evaluated.
   * @param input The elements in the window being evaluated.
   * @param out A collector for emitting elements.
   *
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception;
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {

  /**
    * Evaluates the window and outputs none or several elements.
    *
    * @param key    The key for which this window is evaluated.
    * @param window The window that is being evaluated.
    * @param input  The elements in the window being evaluated.
    * @param out    A collector for emitting elements.
    * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
    */
  def apply(key: KEY, window: W, input: Iterable[IN], out: Collector[OUT])
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class WindowFunction(Function, Generic[IN, OUT, KEY, W]):

    @abstractmethod
    def apply(self, key: KEY, window: W, inputs: Iterable[IN]) -> Iterable[OUT]:
        """
        Evaluates the window and outputs none or several elements.
    
        :param key: The key for which this window is evaluated.
        :param window: The window that is being evaluated.
        :param inputs: The elements in the window being evaluated.
        """
        pass
```
{{< /tab >}}
{{< /tabs >}}

它可以像下例这样使用：

{{< tabs "4992209c-3237-42c2-82fd-894a30d2546a" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .apply(new MyWindowFunction());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .apply(new MyWindowFunction())
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .apply(MyWindowFunction())
```
{{< /tab >}}
{{< /tabs >}}

## Triggers

`Trigger` 决定了一个窗口（由 *window assigner* 定义）何时可以被 *window function* 处理。
每个 `WindowAssigner` 都有一个默认的 `Trigger`。
如果默认 trigger 无法满足你的需要，你可以在 `trigger(...)` 调用中指定自定义的 trigger。

Trigger 接口提供了五个方法来响应不同的事件：

* `onElement()` 方法在每个元素被加入窗口时调用。
* `onEventTime()` 方法在注册的 event-time timer 触发时调用。
* `onProcessingTime()` 方法在注册的 processing-time timer 触发时调用。
* `onMerge()` 方法与有状态的 trigger 相关。该方法会在两个窗口合并时，
将窗口对应 trigger 的状态进行合并，比如使用会话窗口时。
* 最后，`clear()` 方法处理在对应窗口被移除时所需的逻辑。

有两点需要注意：

1) 前三个方法通过返回 `TriggerResult` 来决定 trigger 如何应对到达窗口的事件。应对方案有以下几种：

* `CONTINUE`: 什么也不做
* `FIRE`: 触发计算
* `PURGE`: 清空窗口内的元素
* `FIRE_AND_PURGE`: 触发计算，计算结束后清空窗口内的元素

2) 上面的任意方法都可以用来注册 processing-time 或 event-time timer。

### 触发（Fire）与清除（Purge）

当 trigger 认定一个窗口可以被计算时，它就会触发，也就是返回 `FIRE` 或 `FIRE_AND_PURGE`。
这是让窗口算子发送当前窗口计算结果的信号。
如果一个窗口指定了 `ProcessWindowFunction`，所有的元素都会传给 `ProcessWindowFunction`。
如果是 `ReduceFunction` 或 `AggregateFunction`，则直接发送聚合的结果。

当 trigger 触发时，它可以返回 `FIRE` 或 `FIRE_AND_PURGE`。
`FIRE` 会保留被触发的窗口中的内容，而 `FIRE_AND_PURGE` 会删除这些内容。
Flink 内置的 trigger 默认使用 `FIRE`，不会清除窗口的状态。

{{< hint warning >}} 
Purge 只会移除窗口的内容，
不会移除关于窗口的 meta-information 和 trigger 的状态。
{{< /hint >}}

### WindowAssigner 默认的 Triggers

`WindowAssigner` 默认的 `Trigger` 足以应付诸多情况。
比如说，所有的 event-time window assigner 都默认使用 `EventTimeTrigger`。
这个 trigger 会在 watermark 越过窗口结束时间后直接触发。

`GlobalWindow` 的默认 trigger 是永远不会触发的 `NeverTrigger`。因此，使用 `GlobalWindow` 时，你必须自己定义一个 trigger。 

{{< hint warning >}}
当你在 `trigger()` 中指定了一个 trigger 时，
你实际上覆盖了当前 `WindowAssigner` 默认的 trigger。
比如说，如果你指定了一个 `CountTrigger` 给 `TumblingEventTimeWindows`，你的窗口将不再根据时间触发，
而是根据元素数量触发。如果你希望即响应时间，又响应数量，就需要自定义 trigger 了。
{{< /hint >}}

### 内置 Triggers 和自定义 Triggers

Flink 包含一些内置 trigger。

* 之前提到过的 `EventTimeTrigger` 根据 watermark 测量的 event time 触发。
* `ProcessingTimeTrigger` 根据 processing time 触发。
* `CountTrigger` 在窗口中的元素超过预设的限制时触发。
* `PurgingTrigger` 接收另一个 trigger 并将它转换成一个会清理数据的 trigger。

如果你需要实现自定义的 trigger，你应该看看这个抽象类
{{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java" name="Trigger" >}} 。
请注意，这个 API 仍在发展，所以在之后的 Flink 版本中可能会发生变化。

## Evictors

Flink 的窗口模型允许在 `WindowAssigner` 和 `Trigger` 之外指定可选的 `Evictor`。
如本文开篇的代码中所示，通过 `evictor(...)` 方法传入 `Evictor`。
Evictor 可以在 trigger 触发后、调用窗口函数之前或之后从窗口中删除元素。
`Evictor` 接口提供了两个方法实现此功能：

    /**
     * Optionally evicts elements. Called before windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

    /**
     * Optionally evicts elements. Called after windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

`evictBefore()` 包含在调用窗口函数前的逻辑，而 `evictAfter()` 包含在窗口函数调用之后的逻辑。
在调用窗口函数之前被移除的元素不会被窗口函数计算。

Flink 内置有三个 evictor：

* `CountEvictor`: 仅记录用户指定数量的元素，一旦窗口中的元素超过这个数量，多余的元素会从窗口缓存的开头移除
* `DeltaEvictor`: 接收 `DeltaFunction` 和 `threshold` 参数，计算最后一个元素与窗口缓存中所有元素的差值，
并移除差值大于或等于 `threshold` 的元素。
* `TimeEvictor`: 接收 `interval` 参数，以毫秒表示。
它会找到窗口中元素的最大 timestamp `max_ts` 并移除比 `max_ts - interval` 小的所有元素。

默认情况下，所有内置的 evictor 逻辑都在调用窗口函数前执行。

{{< hint danger >}}
指定一个 evictor 可以避免预聚合，因为窗口中的所有元素在计算前都必须经过 evictor。
{{< /hint >}}

{{< hint info >}} Note: `Evictor` 在 Python DataStream API 中还不支持. {{< /hint >}}

Flink 不对窗口中元素的顺序做任何保证。也就是说，即使 evictor 从窗口缓存的开头移除一个元素，这个元素也不一定是最先或者最后到达窗口的。


## Allowed Lateness

在使用 *event-time* 窗口时，数据可能会迟到，即 Flink 用来追踪 event-time 进展的 watermark 已经
越过了窗口结束的 timestamp 后，数据才到达。对于 Flink 如何处理 event time，
[event time]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}) 和 [late elements]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#late-elements) 有更详细的探讨。

默认情况下，watermark 一旦越过窗口结束的 timestamp，迟到的数据就会被直接丢弃。
但是 Flink 允许指定窗口算子最大的 *allowed lateness*。
Allowed lateness 定义了一个元素可以在迟到多长时间的情况下不被丢弃，这个参数默认是 0。
在 watermark 超过窗口末端、到达窗口末端加上 allowed lateness 之前的这段时间内到达的元素，
依旧会被加入窗口。取决于窗口的 trigger，一个迟到但没有被丢弃的元素可能会再次触发窗口，比如 `EventTimeTrigger`。

为了实现这个功能，Flink 会将窗口状态保存到 allowed lateness 超时才会将窗口及其状态删除
（如 [Window Lifecycle](#window-lifecycle) 所述）。


默认情况下，allowed lateness 被设为 `0`。即 watermark 之后到达的元素会被丢弃。

你可以像下面这样指定 allowed lateness：

{{< tabs "7adb4f13-71c9-46df-96ef-9454d1dfa4ea" >}}
{{< tab "Java" >}}
```java
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .<windowed transformation>(<window function>);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[T] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream
input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .allowed_lateness(<time>) \
    .<windowed transformation>(<window function>)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
使用 `GlobalWindows` 时，没有数据会被视作迟到，因为全局窗口的结束 timestamp 是 `Long.MAX_VALUE`。
{{< /hint >}}

### 从旁路输出（side output）获取迟到数据

通过 Flink 的 [旁路输出]({{< ref "docs/dev/datastream/side_output" >}}) 功能，你可以获得迟到数据的数据流。

首先，你需要在开窗后的 stream 上使用 `sideOutputLateData(OutputTag)` 表明你需要获取迟到数据。
然后，你就可以从窗口操作的结果中获取旁路输出流了。

{{< tabs "0c858203-277f-4a7c-ba86-3ebc5d53cf96" >}}
{{< tab "Java" >}}
```java
final OutputTag<T> lateOutputTag = new OutputTag<T>("late-data"){};

DataStream<T> input = ...;

SingleOutputStreamOperator<T> result = input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .sideOutputLateData(lateOutputTag)
    .<windowed transformation>(<window function>);

DataStream<T> lateStream = result.getSideOutput(lateOutputTag);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val lateOutputTag = OutputTag[T]("late-data")

val input: DataStream[T] = ...

val result = input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .sideOutputLateData(lateOutputTag)
    .<windowed transformation>(<window function>)

val lateStream = result.getSideOutput(lateOutputTag)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
late_output_tag = OutputTag("late-data", type_info)

input = ...  # type: DataStream

result = input \
    .key_by(<key selector>) \
    .window(<window assigner>) \
    .allowed_lateness(<time>) \
    .side_output_late_data(late_output_tag) \
    .<windowed transformation>(<window function>)

late_stream = result.get_side_output(late_output_tag)
```
{{< /tab >}}
{{< /tabs >}}

### 迟到数据的一些考虑

当指定了大于 0 的 allowed lateness 时，窗口本身以及其中的内容仍会在 watermark 越过窗口末端后保留。
这时，如果一个迟到但未被丢弃的数据到达，它可能会再次触发这个窗口。
这种触发被称作 `late firing`，与表示第一次触发窗口的 `main firing` 相区别。
如果是使用会话窗口的情况，late firing 可能会进一步合并已有的窗口，因为他们可能会连接现有的、未被合并的窗口。

{{< hint info >}}
你应该注意：late firing 发出的元素应该被视作对之前计算结果的更新，即你的数据流中会包含一个相同计算任务的多个结果。你的应用需要考虑到这些重复的结果，或去除重复的部分。
{{< /hint >}}

## Working with window results

窗口操作的结果会变回 `DataStream`，并且窗口操作的信息不会保存在输出的元素中。
所以如果你想要保留窗口的 meta-information，你需要在 `ProcessWindowFunction` 里手动将他们放入输出的元素中。
输出元素中保留的唯一相关的信息是元素的 *timestamp*。
它被设置为窗口能允许的最大 timestamp，也就是 *end timestamp - 1*，因为窗口末端的 timestamp 是排他的。
这个情况同时适用于 event-time 窗口和 processing-time 窗口。
也就是说，在窗口操作之后，元素总是会携带一个 event-time 或 processing-time timestamp。
对 Processing-time 窗口来说，这并不意味着什么。
而对于 event-time 窗口来说，“输出携带 timestamp” 以及 “watermark 与窗口的相互作用”
这两者使建立窗口大小相同的连续窗口操作（[consecutive windowed operations](#consecutive-windowed-operations)）
变为可能。我们先看看 watermark 与窗口的相互作用，然后再来讨论它。

### Interaction of watermarks and windows

继续阅读这个章节之前，你可能想要先了解一下我们介绍 [event time 和 watermarks]({{< ref "docs/concepts/time" >}}) 的内容。

当 watermark 到达窗口算子时，它触发了两件事：

 - 这个 watermark 触发了所有最大 timestamp（即 *end-timestamp - 1*）小于它的窗口
 - 这个 watermark 被原封不动地转发给下游的任务。

通俗来讲，watermark 将当前算子中那些“一旦这个 watermark 被下游任务接收就肯定会就超时”的窗口全部冲走。

### Consecutive windowed operations

如之前提到的，窗口结果的 timestamp 如何计算以及 watermark 如何与窗口相互作用使串联多个窗口操作成为可能。
这提供了一种便利的方法，让你能够有两个连续的窗口，他们即能使用不同的 key，
又能让上游操作中某个窗口的数据出现在下游操作的相同窗口。参考下例：

{{< tabs "4ae5a699-7953-47ea-a1d3-2abd28a90740" >}}
{{< tab "Java" >}}
```java
DataStream<Integer> input = ...;

DataStream<Integer> resultsPerKey = input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(new Summer());

DataStream<Integer> globalResults = resultsPerKey
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    .process(new TopKWindowFunction());

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[Int] = ...

val resultsPerKey = input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(new Summer())

val globalResults = resultsPerKey
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
    .process(new TopKWindowFunction())
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream

results_per_key = input \
    .key_by(<key selector>) \
    .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
    .reduce(Summer())

global_results = results_per_key \
    .window_all(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
    .process(TopKWindowFunction())
```
{{< /tab >}}
{{< /tabs >}}

这个例子中，第一个操作中时间窗口`[0, 5)` 的结果会出现在下一个窗口操作的 `[0, 5)` 窗口中。
这就可以让我们先在一个窗口内按 key 求和，再在下一个操作中找出这个窗口中 top-k 的元素。

## 关于状态大小的考量

窗口可以被定义在很长的时间段上（比如几天、几周或几个月）并且积累下很大的状态。
当你估算窗口计算的储存需求时，可以铭记几条规则：

1. Flink 会为一个元素在它所属的每一个窗口中都创建一个副本。
因此，一个元素在滚动窗口的设置中只会存在一个副本（一个元素仅属于一个窗口，除非它迟到了）。
与之相反，一个元素可能会被拷贝到多个滑动窗口中，就如我们在 [Window Assigners](#window-assigners) 中描述的那样。
因此，设置一个大小为一天、滑动距离为一秒的滑动窗口可能不是个好想法。

2. `ReduceFunction` 和 `AggregateFunction` 可以极大地减少储存需求，因为他们会就地聚合到达的元素，
且每个窗口仅储存一个值。而使用 `ProcessWindowFunction` 需要累积窗口中所有的元素。

3. 使用 `Evictor` 可以避免预聚合，
因为窗口中的所有数据必须先经过 evictor 才能进行计算（详见 [Evictors](#evictors)）。

{{< top >}}
