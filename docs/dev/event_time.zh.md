---
title: "Event Time"
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

In this section you will learn about writing time-aware Flink programs. Please
take a look at [Timely Stream Processing]({% link
concepts/timely-stream-processing.zh.md %}) to learn about the concepts behind
timely stream processing.

For information about how to use time in Flink programs refer to
[windowing]({% link dev/stream/operators/windows.zh.md %}) and
[ProcessFunction]({% link
dev/stream/operators/process_function.zh.md %}).

* toc
{:toc}

## Setting a Time Characteristic

The first part of a Flink DataStream program usually sets the base *time characteristic*. That setting
defines how data stream sources behave (for example, whether they will assign timestamps), and what notion of
time should be used by window operations like `KeyedStream.timeWindow(Time.seconds(30))`.

The following example shows a Flink program that aggregates events in hourly time windows. The behavior of the
windows adapts with the time characteristic.

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

Note that in order to run this example in *event time*, the program needs to either use sources
that directly define event time for the data and emit watermarks themselves, or the program must
inject a *Timestamp Assigner & Watermark Generator* after the sources. Those functions describe how to access
the event timestamps, and what degree of out-of-orderness the event stream exhibits.

The section below describes the general mechanism behind *timestamps* and *watermarks*. For a guide on how
to use timestamp assignment and watermark generation in the Flink DataStream API, please refer to
[Generating Timestamps / Watermarks]({{ site.baseurl }}/dev/event_timestamps_watermarks.html).

{% top %}

## Idling sources

Currently, with pure event time watermarks generators, watermarks can not progress if there are no elements
to be processed. That means in case of gap in the incoming data, event time will not progress and for
example the window operator will not be triggered and thus existing windows will not be able to produce any
output data.

To circumvent this one can use periodic watermark assigners that don't only assign based on
element timestamps. An example solution could be an assigner that switches to using current processing time
as the time basis after not observing new events for a while.

Sources can be marked as idle using `SourceFunction.SourceContext#markAsTemporarilyIdle`. For details please refer to the Javadoc of
this method as well as `StreamStatus`.

## Debugging Watermarks

Please refer to the [Debugging Windows & Event Time]({{ site.baseurl }}/monitoring/debugging_event_time.html) section for debugging
watermarks at runtime.

## How operators are processing watermarks

As a general rule, operators are required to completely process a given watermark before forwarding it downstream. For example,
`WindowOperator` will first evaluate which windows should be fired, and only after producing all of the output triggered by
the watermark will the watermark itself be sent downstream. In other words, all elements produced due to occurrence of a watermark
will be emitted before the watermark.

The same rule applies to `TwoInputStreamOperator`. However, in this case the current watermark of the operator is defined as
the minimum of both of its inputs.

The details of this behavior are defined by the implementations of the `OneInputStreamOperator#processWatermark`,
`TwoInputStreamOperator#processWatermark1` and `TwoInputStreamOperator#processWatermark2` methods.

{% top %}
