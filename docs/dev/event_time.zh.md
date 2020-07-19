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

在本节中，你将学习编写可感知时间变化（time-aware）的 Flink 程序。可以参阅[实时流处理]({% link concepts/timely-stream-processing.zh.md %})小节以了解实时流处理的概念。

有关如何在 Flink 程序中使用时间特性，请参阅[窗口]({% link dev/stream/operators/windows.zh.md %})和 [ProcessFunction]({% link dev/stream/operators/process_function.zh.md %}) 小节。

使用*事件时间*处理数据之前需要在程序中设置正确的*时间语义*。此项设置会定义源数据的处理方式（例如：程序是否会对数据分配时间戳），以及程序应使用什么时间语义执行 `KeyedStream.timeWindow(Time.seconds(30))` 之类的窗口操作。

可以通过 `StreamExecutionEnvironment.setStreamTimeCharacteristic()` 设置程序的时间语义，示例如下：

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

env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

{% endhighlight %}
</div>
</div>

注意：为了以*事件时间*的语义运行上述示例，程序需要满足下列其中一种条件，要么其消费的数据源直接为其数据定义了事件时间并且可以发出 watermark，要么程序必须在数据源之后显示声明*时间戳分配器和 Watermark 生成器*（*Timestamp Assigner＆Watermark Generator*）。这些函数可以定义 Flink 程序如何获取到事件时间戳以及定义事件流的乱序程度。

## 接下来学习的内容？

* [生成 Watermark]({% link dev/event_timestamps_watermarks.zh.md %})：展示如何编写 Flink 应用程序感知事件时间所必需的时间戳分配器和 watermark 生成器。
* [内置 Watermark 生成器]({% link dev/event_timestamp_extractors.zh.md %})：概述了 Flink 框架内置的 watermark 生成器。
* [调试窗口和事件时间]({% link monitoring/debugging_event_time.zh.md %})：展示如何在含有事件时间语义的 Flink 应用程序中调试 watermark 和时间戳相关的问题。

{% top %}
