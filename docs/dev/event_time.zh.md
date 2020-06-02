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

使用*事件时间*进行流处理的先决条件是设置合适的*时间特性*，该设置定义了数据流的行为方式（例如，是否分配时间戳），以及窗口算子使用哪种时间概念，例如 `KeyedStream.timeWindow(Time.seconds(30))` 。

你可以调用 `StreamExecutionEnvironment.setStreamTimeCharacteristic()` 设置时间特性：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer<MyEvent>(topic, schema, props));

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

env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val stream: DataStream[MyEvent] = env.addSource(new FlinkKafkaConsumer[MyEvent](topic, schema, props))

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

值得注意的是，为了能够使用*事件时间*作为时间特征运行此示例，程序需要使用那些能给数据直接定义事件时间并自己发出水印的源，或者程序必须在收到源发出的事件流之后注入“时间戳分配器和水印生成器”，这些功能描述了访问事件时间戳的方法，以及事件流呈现的乱序程度。

## 接下来看什么?

* [生成水印]({% link dev/event_timestamps_watermarks.zh.md %})：描述了在写可感知事件时间的 Flink 应用程序时，如何定义时间戳分配器和水印生成器。
* [内置水印生成器]({% link dev/event_timestamp_extractors.zh.md %})：概述了 Flink 自带的水印生成器。
* [调试窗口&事件时间]({% link monitoring/debugging_event_time.zh.md %})：描述了在可感知事件时间的 Flink 应用程序里，如何调试水印和时间戳。

{% top %}
