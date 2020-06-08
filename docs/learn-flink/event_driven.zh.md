---
title: 事件驱动应用
nav-id: 事件驱动
nav-pos: 5
nav-title: 事件驱动应用
nav-parent_id: learn-flink
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

## 处理函数（Process Functions）

### 简介

`ProcessFunction` 将事件处理与 Timer，State 结合在一起，使其成为流处理应用的强大构建模块。
这是使用 Flink 创建事件驱动应用程序的基础。它和 `RichFlatMapFunction` 十分相似， 但是增加了 Timer。

### 示例

如果你已经体验了
[流式分析训练]({% link learn-flink/streaming_analytics.zh.md %})
的[动手实践]({% link learn-flink/streaming_analytics.zh.md %}#hands-on)，
你应该记得，它是采用 `TumblingEventTimeWindow` 来计算每个小时内每个司机的小费总和，
像下面的示例这样：

{% highlight java %}
// 计算每个司机每小时的小费总和
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .process(new AddTips());
{% endhighlight %}

使用 `KeyedProcessFunction` 去实现相同的操作更加直接且更有学习意义。
让我们开始用以下代码替换上面的代码：

{% highlight java %}
// 计算每个司机每小时的小费总和
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));
{% endhighlight %}

在这个代码片段中，一个名为 `PseudoWindow` 的 `KeyedProcessFunction` 被应用于 KeyedStream，
其结果是一个 `DataStream<Tuple3<Long, Long, Float>>` （与使用 Flink 内置时间窗口的实现生成的流相同）。

`PseudoWindow` 的总体轮廓示意如下：

{% highlight java %}
// 在时长跨度为一小时的窗口中计算每个司机的小费总和。
// 司机ID作为 key。
public static class PseudoWindow extends 
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private final long durationMsec;

    public PseudoWindow(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    // 在初始化期间调用一次。
    public void open(Configuration conf) {
        . . .
    }

    @Override
    // 每个票价事件（TaxiFare-Event）输入（到达）时调用，以处理输入的票价事件。
    public void processElement(
            TaxiFare fare,
            Context ctx,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }

    @Override
    // 当当前水印（watermark）表明窗口现在需要完成的时候调用。
    public void onTimer(long timestamp, 
            OnTimerContext context, 
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }
}
{% endhighlight %}

注意事项：

* 有几种类型的 ProcessFunctions -- 不仅包括 `KeyedProcessFunction`，还包括
  `CoProcessFunctions`、`BroadcastProcessFunctions` 等. 

* `KeyedProcessFunction` 是一种 `RichFunction`。作为 `RichFunction`，它可以访问使用 Managed Keyed State 所需的 `open`
  和 `getRuntimeContext` 方法。

* 有两个回调方法须要实现： `processElement` 和 `onTimer`。每个输入事件都会调用 `processElement` 方法；
  当计时器触发时调用 `onTimer`。它们可以是基于事件时间（event time）的 timer，也可以是基于处理时间（processing time）的 timer。
  除此之外，`processElement` 和 `onTimer` 都提供了一个上下文对象，该对象可用于与 `TimerService` 交互。
  这两个回调还传递了一个可用于发出结果的 `Collector`。

#### `open()` 方法

{% highlight java %}
// 每个窗口都持有托管的 Keyed state 的入口，并且根据窗口的结束时间执行 keyed 策略。
// 每个司机都有一个单独的MapState对象。
private transient MapState<Long, Float> sumOfTips;

@Override
public void open(Configuration conf) {

    MapStateDescriptor<Long, Float> sumDesc =
            new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
    sumOfTips = getRuntimeContext().getMapState(sumDesc);
}
{% endhighlight %}

由于票价事件（fare-event）可能会乱序到达，有时需要在计算输出前一个小时结果前，处理下一个小时的事件。
这样能够保证“乱序造成的延迟数据”得到正确处理（放到前一个小时中）。
实际上，如果 Watermark 延迟比窗口长度长得多，则可能有多个窗口同时打开，而不仅仅是两个。
此实现通过使用 `MapState` 来支持处理这一点，该 `MapState` 将每个窗口的结束时间戳映射到该窗口的小费总和。

#### `processElement()` 方法

{% highlight java %}
public void processElement(
        TaxiFare fare,
        Context ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long eventTime = fare.getEventTime();
    TimerService timerService = ctx.timerService();

    if (eventTime <= timerService.currentWatermark()) {
        // 事件延迟；其对应的窗口已经触发。
    } else {
        // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的末尾时间点。
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        // 在窗口完成时将启用回调
        timerService.registerEventTimeTimer(endOfWindow);

        // 将此票价的小费添加到该窗口的总计中。
        Float sum = sumOfTips.get(endOfWindow);
        if (sum == null) {
            sum = 0.0F;
        }
        sum += fare.tip;
        sumOfTips.put(endOfWindow, sum);
    }
}
{% endhighlight %}

需要考虑的事项：

* 延迟的事件怎么处理？watermark 后面的事件（即延迟的）正在被删除。
  如果你想做一些比这更高级的操作，可以考虑使用旁路输出（Side outputs），这将在[下一节]({% link 
  learn-flink/event_driven.zh.md%}#side-outputs)中解释。

* 本例使用一个 `MapState`，其中 keys 是时间戳（timestamp），并为同一时间戳设置一个 Timer。
  这是一种常见的模式；它使得在 Timer 触发时查找相关信息变得简单高效。

#### `onTimer()` 方法

{% highlight java %}
public void onTimer(
        long timestamp, 
        OnTimerContext context, 
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long driverId = context.getCurrentKey();
    // 查找刚结束的一小时结果。
    Float sumOfTips = this.sumOfTips.get(timestamp);

    Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
    out.collect(result);
    this.sumOfTips.remove(timestamp);
}
{% endhighlight %}

注意：

* 传递给 `onTimer` 的 `OnTimerContext context` 可用于确定当前 key。

* 我们的 pseudo-windows 在当前 Watermark 到达每小时结束时触发，此时调用 `onTimer`。
  这个 `onTimer` 方法从 `sumOfTips` 中删除相关的条目，这样做的效果是不可能容纳延迟的事件。
  这相当于在使用 Flink 的时间窗口时将 allowedLateness 设置为零。

### 性能考虑

Flink 提供了为 RocksDB 优化的 `MapState` 和 `ListState` 类型。
相对于 `ValueState`，更建议使用 `MapState` 和 `ListState`，因为使用 RocksDBStateBackend 的情况下，
`MapState` 和 `ListState` 比 `ValueState` 性能更好。
RocksDBStateBackend 可以附加到 `ListState`，而无需进行（反）序列化，
对于 `MapState`，每个 key/value 都是一个单独的 RocksDB 对象，因此可以有效地访问和更新 `MapState`。

{% top %}

## 旁路输出（Side Outputs）

### 简介

有几个很好的理由希望从 Flink 算子获得多个输出流，如下报告条目：

* 异常情况（exceptions）
* 格式错误的事件（malformed events）
* 延迟的事件（late events）
* operator 告警（operational alerts），如与外部服务的连接超时

旁路输出（Side outputs）是一种方便的方法。除了错误报告之外，旁路输出也是实现流的 n 路分割的好方法。

### 示例

现在你可以对上一节中忽略的延迟事件执行某些操作。

Side output channel 与 `OutputTag<T>` 相关联。这些标记拥有自己的名称，并与对应 DataStream 类型一致。

{% highlight java %}
private static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {};
{% endhighlight %}

上面显示的是一个静态 `OutputTag<TaxiFare>` ，当在 `PseudoWindow` 的 `processElement` 方法中发出延迟事件时，可以引用它：

{% highlight java %}
if (eventTime <= timerService.currentWatermark()) {
    // 事件延迟，其对应的窗口已经触发。
    ctx.output(lateFares, fare);
} else {
    . . .
}
{% endhighlight %}

以及当在作业的 `main` 中从该旁路输出访问流时：

{% highlight java %}
// 计算每个司机每小时的小费总和
SingleOutputStreamOperator hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));

hourlyTips.getSideOutput(lateFares).print();
{% endhighlight %}

或者，可以使用两个同名的 OutputTag 来引用同一个旁路输出，但如果这样做，它们必须具有相同的类型。

{% top %}

## 结语

在本例中，你已经了解了如何使用 `ProcessFunction` 重新实现一个简单的时间窗口。
当然，如果 Flink 内置的窗口 API 能够满足你的开发需求，那么一定要优先使用它。
但如果你发现自己在考虑用 Flink 的窗口做些错综复杂的事情，不要害怕自己动手。

此外，`ProcessFunctions` 对于计算分析之外的许多其他用例也很有用。
下面的实践练习提供了一个完全不同的例子。

`ProcessFunctions` 的另一个常见用例是清理过时 State。如果你回想一下
[Rides and Fares Exercise](https://github.com/apache/flink-training/tree/{% if site.is_stable %}release-{{ site.version_title }}{% else %}master{% endif %}/rides-and-fares)，
其中使用 `RichCoFlatMapFunction` 来计算简单 Join，那么示例方案假设 TaxiRides 和 TaxiFares 两个事件是严格匹配为一个有效
数据对（必须同时出现）并且每一组这样的有效数据对都和一个唯一的 `rideId` 严格对应。如果数据对中的某个 TaxiRides 事件（TaxiFares 事件）
丢失，则同一 `rideId` 对应的另一个出现的 TaxiFares 事件（TaxiRides 事件）对应的 State 则永远不会被清理掉。
所以这里可以使用 `KeyedCoProcessFunction` 的实现代替它（`RichCoFlatMapFunction`），并且可以使用计时器来检测和清除任何过时
的 State。

{% top %}

## 实践练习

本节的实践练习是 [Long Ride Alerts
Exercise](https://github.com/apache/flink-training/tree/{% if site.is_stable %}release-{{ site.version_title }}{% else %}master{% endif %}/long-ride-alerts).

{% top %}

## 延伸阅读

- [处理函数（ProcessFunction）]({% link dev/stream/operators/process_function.zh.md %})
- [旁路输出（Side Outputs）]({% link dev/stream/side_output.zh.md %})

{% top %}
