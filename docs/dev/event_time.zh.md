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

事件时间/处理小时/ Ingestion time
flink支持流处理程序中以下的时间处理概念：

处理时间：处理时间指的是机器执行每个操作的时间

在所有的流式处理的程序中，所有基于时间的操作都将使用每台机器各自的系统时间去运行的各自的操作.以一小时为单位的时间窗口将会对所有以系统时间为标准的整小时的所有的收集的数据进行计算。例如，如果应用程序开始运行的9点15分，第一时间处理窗口将包含事件处理之间的9点15分到10点整的数据，在下一个窗口将会处理10点到11点之间的数据



处理时间是建立在数据流和机器之间不协调需求的时间概念。它提供了最好的性能和最低延迟。然而，在分布式式和异步的环境中，处理时间并不起决定性的作用，这是因为处理时间会受到数据进入到系统速度(例如来自消息队列的信息),也有可能会受到数据流在不同操作操作的速度，或者过时(计划，或者其他原因)

事件时间：时是每个个体事件在其生产设备产生的时间。这个时间在进入到flink平台时，已经内嵌到记录当中，同时事件时间能够通过记录进行提取。在事件时间中，时间的进入依赖于数据，不会依赖任何时间墙。处理事件时间必须说明如何生成事件时间watermark(watermark是一个基于事件时间的单进度机制),这种机制会在后面进行阐述。

在一个理想情况下，事件处理时间会完全持续的，并对计算结果起决定性作用，而数据什么时候进入系统中或者数据的排列不是决定性的。然而，除非事件已知事件是顺序进入到大数据平台并且事件会产生一些延迟在等待乱序的事件。使得在处理数据时有可能会无限期的等待，这取决于事件时间程序的限制。

假设所有数据已经到达flink平台中，那么事件时间将会按照理想的处理方式进行处理，对于乱序的数据和过时的数据，甚至是处理历史数据都会得到正确的结果。例如，一个小时的时间窗口将包含所有在那一个小时内事件记录，而数据进入到flink的顺序和数据处理的时间是可以忽视的。(可以看迟到时间部分了解详细信息)


注意，有时当事件处理程序的时间的实时数据时，会使用一些基于时间的操作，来保证对于数据处理是按照实时的模式进行处理。



Ingestion time:Ingestion time是事件进入到flink平台的时间。在source 操作中，每一个记录会获取source的当前时间作为一个时间戳。基于时间的操作(像时间窗口)会指向这个时间。

Ingestion time(入侵时间)在概念上是位于在事件时间和处理时间之间。相比较于处理时间，入侵时间会显得重要一点，对于结果的可预测上面。因为入侵时间使用的是稳定的时间戳(在source时会进行分发),基于record的不同的窗口操作将会指向相同的时间戳，尽管每一个窗口操作时会将记录分布在不同的窗口中在处理时间中(基于本地系统时钟和传递延迟)

相比于事件时间，入侵时间程序将不会处理任何过时和迟到数据，但是程序需要决定生成水印。



进一步讲，ingestion 时间更像事件处理的时间，除了自动生成时间戳与自动配置和自动水印的生成。


# 事件时间 / 处理时间 / Ingestion Time

FLink支持流处理程序中以下的时间处理概念:
 
- **处理时间：** 处理时间指的是机器执行每个操作的时间
      
    在一个流式处理的程序在运行过程中,所有基于时间的操作都将使用每台机器各自的系统时间去运行的各自的操作。一个整小时运行的时间
    When a streaming program runs on processing time, all time-based operations (like time windows) will
    use the system clock of the machines that run the respective operator. An hourly
    processing time window will include all records that arrived at a specific operator between the
    times when the system clock indicated the full hour. For example, if an application
    begins running at 9:15am, the first hourly processing time window will include events
    processed between 9:15am and 10:00am, the next window will include events processed between 10:00am and 11:00am, and so on.

    Processing time is the simplest notion of time and requires no coordination between streams and machines.
    It provides the best performance and the lowest latency. However, in distributed and asynchronous
    environments processing time does not provide determinism, because it is susceptible to the speed at which
    records arrive in the system (for example from the message queue), to the speed at which the
    records flow between operators inside the system, and to outages (scheduled, or otherwise).

- **Event time:** Event time is the time that each individual event occurred on its producing device.
    This time is typically embedded within the records before they enter Flink, and that *event timestamp*
    can be extracted from each record. In event time, the progress of time depends on the data,
    not on any wall clocks. Event time programs must specify how to generate *Event Time Watermarks*,
    which is the mechanism that signals progress in event time. This watermarking mechanism is
    described in a later section, [below](#event-time-and-watermarks).

    In a perfect world, event time processing would yield completely consistent and deterministic results, regardless of when events arrive, or their ordering.
    However, unless the events are known to arrive in-order (by timestamp), event time processing incurs some latency while waiting for out-of-order events. As it is only possible to wait for a finite period of time, this places a limit on how deterministic event time applications can be.

    Assuming all of the data has arrived, event time operations will behave as expected, and produce correct and consistent results even when working with out-of-order or late events, or when reprocessing historic data. For example, an hourly event time window will contain all records
    that carry an event timestamp that falls into that hour, regardless of the order in which they arrive, or when they are processed. (See the section on [late events](#late-elements) for more information.)



    Note that sometimes when event time programs are processing live data in real-time, they will use some *processing time* operations in order to guarantee that they are progressing in a timely fashion.

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

Note that event time is inherited by a freshly created stream element (or elements) from either the event that produced them or
from watermark that triggered creation of those elements.

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

Note that the Kafka source supports per-partition watermarking, which you can read more about [here]({{ site.baseurl }}/dev/event_timestamps_watermarks.html#timestamps-per-kafka-partition).


## Late Elements

It is possible that certain elements will violate the watermark condition, meaning that even after the *Watermark(t)* has occurred,
more elements with timestamp *t' <= t* will occur. In fact, in many real world setups, certain elements can be arbitrarily
delayed, making it impossible to specify a time by which all elements of a certain event timestamp will have occurred.
Furthermore, even if the lateness can be bounded, delaying the watermarks by too much is often not desirable, because it
causes too much delay in the evaluation of event time windows.

For this reason, streaming programs may explicitly expect some *late* elements. Late elements are elements that
arrive after the system's event time clock (as signaled by the watermarks) has already passed the time of the late element's
timestamp. See [Allowed Lateness]({{ site.baseurl }}/dev/stream/operators/windows.html#allowed-lateness) for more information on how to work
with late elements in event time windows.

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
