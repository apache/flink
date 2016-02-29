---
title: "Working with Time"
is_beta: false
sub-nav-group: streaming
sub-nav-pos: 1
sub-nav-parent: windows
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

* toc
{:toc}

When working with time windows it becomes necessary to think about the concept of time
in a streaming program. Flink has support for three kinds of time:

- **Processing time:** Processing time is simply the wall clock time of the machine that happens to be
    executing the transformation. Processing time is the simplest notion of time and provides the best
    performance. However, in distributed and asynchronous environments processing time does not provide
    determinism.

- **Event time:** Event time is the time that each individual event occurred. This time is
    typically embedded within the records before they enter Flink or can be extracted from their contents.
    When using event time, out-of-order events can be properly handled. For example, an event with a lower
    timestamp may arrive after an event with a higher timestamp, but transformations will handle these events
    correctly. Event time processing provides predictable results, but incurs more latency, as out-of-order
    events need to be buffered

- **Ingestion time:** Ingestion time is the time that events enter Flink. In particular, the timestamp of
    an event is assigned by the source operator as the current wall clock time of the machine that executes
    the source task at the time the records enter the Flink source. Ingestion time is more predictable
    than processing time, and gives lower latencies than event time as the latency does not depend on
    external systems. Ingestion time provides thus a middle ground between processing time and event time.
    Ingestion time is a special case of event time (and indeed, it is treated by Flink identically to
    event time).

When dealing with event time, transformations need to avoid indefinite wait times for events to
arrive. *Watermarks* provide the mechanism to control the event time/processing time skew.
Watermarks can be emitted by the sources. A watermark with a certain timestamp denotes the knowledge
that no event with timestamp lower than the timestamp of the watermark will ever arrive.

Per default, a Flink Job is only set up for processing time semantics, so in order to write a
program with processing time semantics nothing needs to be specified (e.g., the first [example
](index.html#example-program) in this guide follows processing time semantics). To perform processing-time
windowing you would use window assigners such as `SlidingProcessingTimeWindows` and
`TumblingProcessingTimeWindows`.

In order to work with event time semantics, i.e. if you want to use window assigners such as
`TumblingEventTimeWindows` or `SlidingEventTimeWindows`, you need to follow these steps:

- Set `enableTimestamps()`, as well the interval for watermark emission
(`setAutoWatermarkInterval(long milliseconds)`) in `ExecutionConfig`.

- Use `DataStream.assignTimestamps(...)` in order to tell Flink how timestamps relate to events
(e.g., which record field is the timestamp)

For example, assume that we have a data stream of tuples, in which the first field is the timestamp (assigned
by the system that generates these data streams), and we know that the lag between the current processing
time and the timestamp of an event is never more than 1 second:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple4<Long,Integer,Double,String>> stream = //...
stream.assignTimestamps(new TimestampExtractor<Tuple4<Long,Integer,Double,String>>{
    @Override
    public long extractTimestamp(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0;
    }

    @Override
    public long extractWatermark(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0 - 1000;
    }

    @Override
    public long getCurrentWatermark() {
        return Long.MIN_VALUE;
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[(Long,Int,Double,String)] = null;
stream.assignTimestampts(new TimestampExtractor[(Long, Int, Double, String)] {
  override def extractTimestamp(element: (Long, Int, Double, String), currentTimestamp: Long): Long = element._1

  override def extractWatermark(element: (Long, Int, Double, String), currentTimestamp: Long): Long = element._1 - 1000

  override def getCurrentWatermark: Long = Long.MinValue
})
{% endhighlight %}
</div>
</div>

If you know that timestamps of events are always ascending, i.e., elements arrive in order, you can use
the `AscendingTimestampExtractor`, and the system generates watermarks automatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple4<Long,Integer,Double,String>> stream = //...
stream.assignTimestamps(new AscendingTimestampExtractor<Tuple4<Long,Integer,Double,String>>{
    @Override
    public long extractAscendingTimestamp(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0;
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.extractAscendingTimestamp(record => record._1)
{% endhighlight %}
</div>
</div>

Flink also has a shortcut for working with time, the `stream time characteristic`. It can
be specified as:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight java %}
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

For `EventTime`, this will enable timestamps and also set a default watermark interval.
The `timeWindow()` and `timeWindowAll()` transformations will respect this time characteristic and
instantiate the correct window assigner based on the time characteristic.

In order to write a program with ingestion time semantics, you need to set
`env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)`. You can think of this setting
as a shortcut for writing a `TimestampExtractor` which assignes timestamps to events at the sources
based on the current source wall-clock time. Flink injects this timestamp extractor automatically.