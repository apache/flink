---
title: "Process Function (Low-level Operations)"
nav-title: "Process Function"
nav-parent_id: streaming_operators
nav-pos: 35
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

## ProcessFunction

`ProcessFunction` 是初级的流处理算子，
允许访问所有（无环）流应用程序的基本构建块：

  - 事件 （流的元素）
  - 状态 （容错、一致性，仅作用于 keyed stream）
  - 计时器 （事件时间和处理时间，仅作用于 keyed stream）

`ProcessFunction` 可以被视为可以访问 keyed state 和计时器的 `FlatMapFunction`。
它通过被输入流中每个事件调用的方式处理事件。

对于状态容错，`ProcessFunction` 允许访问 Flink 的 [keyed state]({{ site.baseurl }}/zh/dev/stream/state/state.html)，可以通过 `RuntimeContext` 访问，
与其他状态函数访问 keyed state 的方式类似。

计时器允许应用对事件时间和[事件时间]({{ site.baseurl }}/zh/dev/event_time.html)的变化做出反应。
每次调用函数 `processElement(...)` 时都会返回一个 `Context` 对象，它可以访问元素的事件时间戳以及 *TimeService*。
*TimeService* 可以用于为以后的事件/处理时间实例注册回调。对于事件时间计时器，`onTimer(...)` 方法会在当前 watermark 先于或早于计时器的时间戳时被调用，
而对于处理时间计时器，`onTimer(...)` 方法会在机器时间达到某些特殊时刻时被调用。在该调用期间，所有状态再次被限定在创建计时器的键的范围内，从而允许计时器操作 keyed state。

<span class="label label-info">注意</span> 如果你想要访问 keyed state 和计时器，
你必须在 keyed stream 上使用 `ProcessFunction`。

{% highlight java %}
stream.keyBy(...).process(new MyProcessFunction())
{% endhighlight %}


## 初级 Joins

为了在两个输入端使用初级算子，应用程序可以使用 `CoProcessFunction` 或者 `KeyedCoProcessFunction`。
该函数与两个不同的输入绑定，
并获取对来自两个不同输入端的记录 `processElement1(...)` 和 `processElement2(...)` 的单独调用。

实现初级 join 通常遵循下述规则：

  - 为一个（或两个）输入端创建一个创建一个状态对象
  - 通过从输入源接收到的元素更新状态
  - 从其他输入接收元素后，检测状态并生成 join 结果

例如，你可能正在将客户数据 join 至金融交易中，同时保留客户数据的状态。
如果你需要确保在处理无序事件时有完整且确定的 join 操作，
当客户数据流的 watermark 超过交易时间时，
你可以使用计时器计算并发出交易的 join 操作。

## 实例

在下面的事例中，`KeyedProcessFunction` 维护着每个键的计数，并且每隔一分钟（基于事件时间）就输出一个键/计数对，而不更新该键：

  - 计数、键和最近一次修改的时间戳存储在 `ValueState` 中，被键隐式地限制了作用域
  - 对于每条记录，`KeyedProcessFunction` 增加计数器并且设置最近一次修改的时间戳
  - 该函数还将回调安排在一分钟后（基于事件时间）
  - 每次回调时，它都会根据存储计数的最后修改时间检查回调的事件时间戳，
  如果匹配，则输出键/计数对（即，在这一分钟内不会做进一步的更新）

<span class="label label-info">注意</span> 这个简单的例子可以通过会话窗口实现。
我们在这里使用 `KeyedProcessFunction` 来说明它提供的基本模式。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


// the source data stream
DataStream<Tuple2<String, String>> stream = ...;

// apply the process function onto a keyed stream
DataStream<Tuple2<String, Long>> result = stream
    .keyBy(0)
    .process(new CountWithTimeoutFunction());

/**
 * The data type stored in the state
 */
public class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
public class CountWithTimeoutFunction
        extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value,
            Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// the source data stream
val stream: DataStream[Tuple2[String, String]] = ...

// apply the process function onto a keyed stream
val result: DataStream[Tuple2[String, Long]] = stream
  .keyBy(0)
  .process(new CountWithTimeoutFunction())

/**
  * The data type stored in the state
  */
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

/**
  * The implementation of the ProcessFunction that maintains the count and timeouts
  */
class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, Long)] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


  override def processElement(
      value: (String, String),
      ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#Context,
      out: Collector[(String, Long)]): Unit = {

    // initialize or retrieve/update the state
    val current: CountWithTimestamp = state.value match {
      case null =>
        CountWithTimestamp(value._1, 1, ctx.timestamp)
      case CountWithTimestamp(key, count, lastModified) =>
        CountWithTimestamp(key, count + 1, ctx.timestamp)
    }

    // write the state back
    state.update(current)

    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#OnTimerContext,
      out: Collector[(String, Long)]): Unit = {

    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
        out.collect((key, count))
      case _ =>
    }
  }
}
{% endhighlight %}
</div>
</div>

{% top %}


**注意:** 在 Flink 1.4.0 以前，通过处理时间计时器调用时，`ProcessFunction.onTimer()` 方法将当前处理时间设置为事件时间戳。
这种操作非常微妙，用户可能不会注意到。然而，这样是不对的，因为处理时间戳是不确定的，并且与 watermark 不一致。
此外，用户实现的逻辑依赖于这个错误的时间戳很可能导致意料之外的错误。
所以我们决定将其修复。
升级到 1.4.0 时，使用此错误事件时间戳的 Flink 作业将失败，用户应根据正确的逻辑调整其作业。

## KeyedProcessFunction

`KeyedProcessFunction`，作为 `ProcessFunction` 的拓展，
可以通过它的 `onTimer(...)` 方法访问计时器的键。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT]): Unit = {
  var key = ctx.getCurrentKey
  // ...
}
{% endhighlight %}
</div>
</div>

## 计时器

两种计时器（处理时间和事件时间）都由 `TimerService` 内部维护，并排队等待执行。

`TimerService` 对每个键和时间戳的计时器进行重复数据消除，即每个键和时间戳最多有一个计时器。如果为同一时间戳注册了多个计时器，则只调用一次 `onTimer()` 方法。

<span class="label label-info">注意</span> Flink 对 `onTimer()` 和 `processElement()` 的调用是同步的。因此，用户不必担心状态的并发修改。

### 容错

计数器是容错的，并且会与应用状态一起存入 checkpoint。

在故障恢复或从 savepoint 启动应用程序时，将还原计时器。

<span class="label label-info">注意</span> 已存入 checkpoint 且本应该在恢复之前触发的处理时间计时器，将会立即触发。
当应用程序从故障中恢复或从 savepoint 启动时，可能会发生这种情况。

<span class="label label-info">注意</span> 计时器存入 checkpoint 的方式总是异步的，除了 RocksDB backend/与增量快照/与基于堆的计时器的组合（将在 `Flink-10026` 中解决）。
注意，大量的计时器会增加 checkpoint 的时间因为计时器也是 checkpoint 状态的一部分。参考“计时器合并”章节了解如何减少计时器的数目。

### 计时器合并

由于 Flink 只为每个键和时间戳维护一个计时器，你可以通过降低计时器的分辨率以合并它们的方式来减少计时器的数目。

对于1秒（事件或处理时间）的计时器分辨率，可以将目标时间舍入为整秒。
计时器最多会提前1秒，但不迟于要求的毫秒精度。
因此，每个键每秒至多有一个计时器。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val coalescedTime = ((ctx.timestamp + timeout) / 1000) * 1000
ctx.timerService.registerProcessingTimeTimer(coalescedTime)
{% endhighlight %}
</div>
</div>

由于事件时间计时器仅在 watermark 进入时触发，
因此你还可以使用当前的 watermark 调度这些计时器并将其与下一个 watermark 合并：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val coalescedTime = ctx.timerService.currentWatermark + 1
ctx.timerService.registerEventTimeTimer(coalescedTime)
{% endhighlight %}
</div>
</div>

计时器也可以通过下面的方法停止和删除：

停止处理时间计时器：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long timestampOfTimerToStop = ...
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val timestampOfTimerToStop = ...
ctx.timerService.deleteProcessingTimeTimer(timestampOfTimerToStop)
{% endhighlight %}
</div>
</div>

停止事件时间计时器：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long timestampOfTimerToStop = ...
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val timestampOfTimerToStop = ...
ctx.timerService.deleteEventTimeTimer(timestampOfTimerToStop)
{% endhighlight %}
</div>
</div>

<span class="label label-info">注意</span> 如果没有注册具有给定时间戳的计时器，则停止计时器无效。

{% top %}
