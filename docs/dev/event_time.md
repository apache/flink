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

* toc
{:toc}

# Event Time / Processing Time / Ingestion Time

Flink supports different notions of *time* in streaming programs.

- **Processing time:** Processing time refers to the system time of the machine that is executing the
    respective operation.

    When a streaming program runs on processing time, all time-based operations (like time windows) will
    use the system clock of the machines that run the respective operator. For example, an hourly
    processing time window will include all records that arrived at a specific operator between the
    times when the system clock indicated the full hour.

    Processing time is the simplest notion of time and requires no coordination between streams and machines.
    It provides the best performance and the lowest latency. However, in distributed and asynchronous
    environments processing time does not provide determinism, because it is susceptible to the speed at which
    records arrive in the system (for example from the message queue), and to the speed at which the
    records flow between operators inside the system.

- **Event time:** Event time is the time that each individual event occurred on its producing device.
    This time is typically embedded within the records before they enter Flink and that *event timestamp*
    can be extracted from the record. An hourly event time window will contain all records that carry an
    event timestamp that falls into that hour, regardless of when the records arrive, and in what order
    they arrive.

    Event time gives correct results even on out-of-order events, late events, or on replays
    of data from backups or persistent logs. In event time, the progress of time depends on the data,
    not on any wall clocks. Event time programs must specify how to generate *Event Time Watermarks*,
    which is the mechanism that signals progress in event time. The mechanism is
    described below.

    Event time processing often incurs a certain latency, due to its nature of waiting a certain time for
    late events and out-of-order events. Because of that, event time programs are often combined with
    *processing time* operations.

- **Ingestion time:** Ingestion time is the time that events enter Flink. At the source operator each
    record gets the source's current time as a timestamp, and time-based operations (like time windows)
    refer to that timestamp.

    *Ingestion time* sits conceptually in between *event time* and *processing time*. Compared to
    *processing time*, it is slightly more expensive, but gives more predictable results. Because
    *ingestion time* uses stable timestamps (assigned once at the source), different window operations
    over the records will refer to the same timestamp, whereas in *processing time* each window operator
    may assign the record to a different window (based on the local system clock and any transport delay).

    Compared to *event time*, *ingestion time* programs cannot handle any out-of-order events or late data,
    but the programs don't have to specify how to generate *watermarks*.

    Internally, *ingestion time* is treated much like *event time*, but with automatic timestamp assignment and
    automatic watermark generation.

<img src="{{ site.baseurl }}/fig/times_clocks.svg" class="center" width="80%" />


### Setting a Time Characteristic

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

DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer09<MyEvent>(topic, schema, props));

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

val stream: DataStream[MyEvent] = env.addSource(new FlinkKafkaConsumer09[MyEvent](topic, schema, props))

stream
    .keyBy( _.getUser )
    .timeWindow(Time.hours(1))
    .reduce( (a, b) => a.add(b) )
    .addSink(...)
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


# Event Time and Watermarks

*Note: Flink implements many techniques from the Dataflow Model. For a good introduction to event time and watermarks, have a look at the articles below.*

  - [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by Tyler Akidau
  - The [Dataflow Model paper](https://research.google.com/pubs/archive/43864.pdf)


A stream processor that supports *event time* needs a way to measure the progress of event time.
For example, a window operator that builds hourly windows needs to be notified when event time has passed beyond the
end of an hour, so that the operator can close the window in progress.

*Event time* can progress independently of *processing time* (measured by wall clocks).
For example, in one program the current *event time* of an operator may trail slightly behind the *processing time*
(accounting for a delay in receiving the events), while both proceed at the same speed.
On the other hand, another streaming program might progress through weeks of event time with only a few seconds of processing,
by fast-forwarding through some historic data already buffered in a Kafka topic (or another message queue).

------

The mechanism in Flink to measure progress in event time is **watermarks**.
Watermarks flow as part of the data stream and carry a timestamp *t*. A *Watermark(t)* declares that event time has reached time
*t* in that stream, meaning that there should be no more elements from the stream with a timestamp *t' <= t* (i.e. events with timestamps
older or equal to the watermark).

The figure below shows a stream of events with (logical) timestamps, and watermarks flowing inline. In this example the events are in order
(with respect to their timestamps), meaning that the watermarks are simply periodic markers in the stream.

<img src="{{ site.baseurl }}/fig/stream_watermark_in_order.svg" alt="A data stream with events (in order) and watermarks" class="center" width="65%" />

Watermarks are crucial for *out-of-order* streams, as illustrated below, where the events are not ordered by their timestamps.
In general a watermark is a declaration that by that point in the stream, all events up to a certain timestamp should have arrived.
Once a watermark reaches an operator, the operator can advance its internal *event time clock* to the value of the watermark.

<img src="{{ site.baseurl }}/fig/stream_watermark_out_of_order.svg" alt="A data stream with events (out of order) and watermarks" class="center" width="65%" />


## Watermarks in Parallel Streams

Watermarks are generated at, or directly after, source functions. Each parallel subtask of a source function usually
generates its watermarks independently. These watermarks define the event time at that particular parallel source.

As the watermarks flow through the streaming program, they advance the event time at the operators where they arrive. Whenever an
operator advances its event time, it generates a new watermark downstream for its successor operators.

Some operators consume multiple input streams; a union, for example, or operators following a *keyBy(...)* or *partition(...)* function.
Such an operator's current event time is the minimum of its input streams' event times. As its input streams
update their event times, so does the operator.

The figure below shows an example of events and watermarks flowing through parallel streams, and operators tracking event time.

<img src="{{ site.baseurl }}/fig/parallel_streams_watermarks.svg" alt="Parallel data streams and operators with events and watermarks" class="center" width="80%" />


## Late Elements

It is possible that certain elements will violate the watermark condition, meaning that even after the *Watermark(t)* has occurred,
more elements with timestamp *t' <= t* will occur. In fact, in many real world setups, certain elements can be arbitrarily
delayed, making it impossible to specify a time by which all elements of a certain event timestamp will have occurred.
Furthermore, even if the lateness can be bounded, delaying the watermarks by too much is often not desirable, because it
causes too much delay in the evaluation of the event time windows.

For this reason, streaming programs may explicitly expect some *late* elements. Late elements are elements that
arrive after the system's event time clock (as signaled by the watermarks) has already passed the time of the late element's
timestamp. See [Allowed Lateness]({{ site.baseurl }}/dev/stream/operators/windows.html#allowed-lateness) for more information on how to work
with late elements in event time windows.


## Debugging Watermarks

Please refer to the [Debugging Windows & Event Time]({{ site.baseurl }}/monitoring/debugging_event_time.html) section for debugging
watermarks at runtime.

{% top %}
