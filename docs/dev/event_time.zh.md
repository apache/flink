---
title: "事件时间"
nav-id: event_time
nav-show_overview: true
nav-parent_id: streaming
nav-pos: 2
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

本节你将学到如何写可感知时间变化的 Flink 程序，可以先看看[实时流处理]({% link concepts/timely-stream-processing.zh.md %})了解相关概念。

想了解如何在 Flink 程序中使用时间特性，请参阅[窗口]({% link dev/stream/operators/windows.zh.md %})和[处理函数]({% link dev/stream/operators/process_function.zh.md %})。

* toc
{:toc}

## 设置时间特征

写 Flink DataStream 程序时，通常先设置基础的*时间特性*，该设置定义了数据流的行为方式（例如，是否分配时间戳），以及窗口算子使用哪种时间概念，例如 `KeyedStream.timeWindow(Time.seconds(30))` 。

以下示例展示了一个按小时聚合事件的 Flink 程序，窗口的行为与时间特征相对应。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

// alternatively:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer010<MyEvent>(topic, schema, props));

stream
    .keyBy( (event) -> event.getUser() )
    .timeWindow(Time.hours(1))
    .reduce( (a, b) -> a.add(b) )
    .addSink(...);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

// alternatively:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val stream: DataStream[MyEvent] = env.addSource(new FlinkKafkaConsumer010[MyEvent](topic, schema, props))

stream
    .keyBy( _.getUser )
    .timeWindow(Time.hours(1))
    .reduce( (a, b) => a.add(b) )
    .addSink(...)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()

env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

# alternatively:
# env.set_stream_time_characteristic(TimeCharacteristic.IngestionTime)
# env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

值得注意的是，如果要用*事件时间*作为时间特征运行此示例，程序需要使用那些给数据直接定义事件时间并能自己发出水印的源，或者程序必须在收到源发出的事件流之后注入“时间戳分配器和水印生成器”，这些功能描述了访问事件时间戳的方法，以及事件流呈现的乱序程度。

可以参考[生成时间戳/水印]({{ site.baseurl }}/zh/dev/event_timestamps_watermarks.html)，了解如何使用 Flink DataStream API 分配时间戳和生成水印。

{% top %}

## 空闲源

当前，对于纯粹的事件时间水印生成器，如果没有要处理的事件，水印是不会生成并下发的，无法推进。这意味着在输入数据存在间隙的情况下，事件时间将不会继续前进，例如无法触发窗口算子，因此现有窗口将无法生成任何输出数据。

为了避免这种情况，可以使用周期性水印分配器，这种分配器不仅基于事件时间戳，还会在没有事件的时候产生新水印。 比如在长时间没有观测到事件流入时，可以采用系统当前时间来生成水印。

可使用 `SourceFunction.SourceContext#markAsTemporarilyIdle` 标记源是空闲的，详情可参考这个方法以及 `StreamStatus` 类的 Javadoc 。

## 调试水印

请参考[调试窗口&事件时间]({{ site.baseurl }}/zh/monitoring/debugging_event_time.html)了解运行阶段的水印调试。

## 算子如何处理水印

通常，要求算子在将给定水印转发到下游之前，必须对这个水印进行完全处理。例如，`WindowOperator` 首先评估应该触发哪些窗口，然后只有在产生了由水印触发的所有输出之后，水印本身才会被发送到下游。换句话说，由水印而触发的所有元素都将在水印之前被发出。

这个规则同样适用于 `TwoInputStreamOperator`。但是，在这种情况下，算子的当前水印被定义为两个输入的最小值。

该行为具体由 `OneInputStreamOperator#processWatermark`、`TwoInputStreamOperator#processWatermark1` 和 `TwoInputStreamOperator#processWatermark2`方法定义。

{% top %}
