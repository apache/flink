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
concepts/timely-stream-processing.md %}) to learn about the concepts behind
timely stream processing.

For information about how to use time in Flink programs refer to
[windowing]({% link dev/stream/operators/windows.md %}) and
[ProcessFunction]({% link
dev/stream/operators/process_function.md %}).

A prerequisite for using *event time* processing is setting the right *time
characteristic*. That setting defines how data stream sources behave (for
example, whether they will assign timestamps), and what notion of time should
be used by window operations like `KeyedStream.timeWindow(Time.seconds(30))`.

You can set the time characteristic using
`StreamExecutionEnvironment.setStreamTimeCharacteristic()`:

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

# alternatively:
# env.set_stream_time_characteristic(TimeCharacteristic.IngestionTime)
# env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
{% endhighlight %}
</div>
</div>

Note that in order to run this example in *event time*, the program needs to
either use sources that directly define event time for the data and emit
watermarks themselves, or the program must inject a *Timestamp Assigner &
Watermark Generator* after the sources. Those functions describe how to access
the event timestamps, and what degree of out-of-orderness the event stream
exhibits.

## Where to go next?

* [Generating Watermarks]({% link dev/event_timestamps_watermarks.md
  %}): Shows how to write timestamp assigners and watermark generators, which
  are needed for event-time aware Flink applications.
* [Builtin Watermark Generators]({% link dev/event_timestamp_extractors.md %}):
  Gives an overview of the builtin watermark generators.
* [Debugging Windows & Event Time]({{ site.baseurl
  }}/monitoring/debugging_event_time.html): Show how to debug problems around
  watermarks and timestamps in event-time Flink applications.

{% top %}
