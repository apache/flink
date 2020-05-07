---
title: Streaming Analytics
nav-id: analytics
nav-pos: 4
nav-title: Streaming Analytics
nav-parent_id: training
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

## Event Time and Watermarks

### Introduction

Flink explicitly supports three different notions of time:

* _event time:_ the time when an event occurred, as recorded by the device producing (or storing) the event

* _ingestion time:_ a timestamp recorded by Flink at the moment it ingests the event

* _processing time:_ the time when a specific operator in your pipeline is processing the event

For reproducible results, e.g., when computing the maximum price a stock reached during the first
hour of trading on a given day, you should use event time. In this way the result won't depend on
when the calculation is performed. This kind of real-time application is sometimes performed using
processing time, but then the results are determined by the events that happen to be processed
during that hour, rather than the events that occurred then. Computing analytics based on processing
time causes inconsistencies, and makes it difficult to re-analyze historic data or test new
implementations.

### Working with Event Time

By default, Flink will use processing time. To change this, you can set the Time Characteristic:

{% highlight java %}
final StreamExecutionEnvironment env =
    StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}

If you want to use event time, you will also need to supply a Timestamp Extractor and Watermark
Generator that Flink will use to track the progress of event time. This will be covered in the
section below on [Working with Watermarks]({% link
training/streaming_analytics.md %}#working-with-watermarks), but first we should explain what
watermarks are.

### Watermarks

Let's work through a simple example that will show why watermarks are needed, and how they work.

In this example you have a stream of timestamped events that arrive somewhat out of order, as shown
below. The numbers shown are timestamps that indicate when these events actually occurred. The first
event to arrive happened at time 4, and it is followed by an event that happened earlier, at time 2,
and so on:

<div class="text-center" style="font-size: x-large; word-spacing: 0.5em; margin: 1em 0em;">
··· 23 19 22 24 21 14 17 13 12 15 9 11 7 2 4 →
</div>

Now imagine that you are trying create a stream sorter. This is meant to be an application that
processes each event from a stream as it arrives, and emits a new stream containing the same events,
but ordered by their timestamps.

Some observations:

(1) The first element your stream sorter sees is the 4, but you can't just immediately release it as
the first element of the sorted stream. It may have arrived out of order, and an earlier event might
yet arrive. In fact, you have the benefit of some god-like knowledge of this stream's future, and
you can see that your stream sorter should wait at least until the 2 arrives before producing any
results.

*Some buffering, and some delay, is necessary.*

(2) If you do this wrong, you could end up waiting forever. First the sorter saw an event from time
4, and then an event from time 2. Will an event with a timestamp less than 2 ever arrive? Maybe.
Maybe not. You could wait forever and never see a 1.

*Eventually you have to be courageous and emit the 2 as the start of the sorted stream.*

(3) What you need then is some sort of policy that defines when, for any given timestamped event, to
stop waiting for the arrival of earlier events.

*This is precisely what watermarks do* — they define when to stop waiting for earlier events.

Event time processing in Flink depends on *watermark generators* that insert special timestamped
elements into the stream, called *watermarks*. A watermark for time _t_ is an assertion that the
stream is (probably) now complete up through time _t_.

When should this stream sorter stop waiting, and push out the 2 to start the sorted stream? When a
watermark arrives with a timestamp of 2, or greater.

(4) You might imagine different policies for deciding how to generate watermarks.

Each event arrives after some delay, and these delays vary, so some events are delayed more than
others. One simple approach is to assume that these delays are bounded by some maximum delay. Flink
refers to this strategy as *bounded-out-of-orderness* watermarking. It is easy to imagine more
complex approaches to watermarking, but for most applications a fixed delay works well enough.

### Latency vs. Completeness

Another way to think about watermarks is that they give you, the developer of a streaming
application, control over the tradeoff between latency and completeness. Unlike in batch processing,
where one has the luxury of being able to have complete knowledge of the input before producing any
results, with streaming you must eventually stop waiting to see more of the input, and produce some
sort of result.

You can either configure your watermarking aggressively, with a short bounded delay, and thereby
take the risk of producing results with rather incomplete knowledge of the input -- i.e., a possibly
wrong result, produced quickly. Or you can wait longer, and produce results that take advantage of
having more complete knowledge of the input stream(s).

It is also possible to implement hybrid solutions that produce initial results quickly, and then
supply updates to those results as additional (late) data is processed. This is a good approach for
some applications.

### Lateness

Lateness is defined relative to the watermarks. A `Watermark(t)` asserts that the stream is complete
up through time _t_; any event following this watermark whose timestamp is &le; _t_ is late.

### Working with Watermarks

In order to perform event-time-based event processing, Flink needs to know the time associated with
each event, and it also needs the stream to include watermarks.

The Taxi data sources used in the hands-on exercises take care of these details for you. But in your
own applications you will have to take care of this yourself, which is usually done by implementing
a class that extracts the timestamps from the events, and generates watermarks on demand. The
easiest way to do this is by extending the `BoundedOutOfOrdernessTimestampExtractor`:

{% highlight java %}
DataStream<Event> stream = ...

DataStream<Event> withTimestampsAndWatermarks =
    stream.assignTimestampsAndWatermarks(new TimestampsAndWatermarks(Time.seconds(10)));

public static class TimestampsAndWatermarks
        extends BoundedOutOfOrdernessTimestampExtractor<Event> {

    public TimestampsAndWatermarks(Time t) {
        super(t);
    }

    @Override
    public long extractTimestamp(Event event) {
        return event.timestamp;
    }
}
{% endhighlight %}

Note that the constructor for `BoundedOutOfOrdernessTimestampExtractor` takes a parameter which
specifies the maximum expected out-of-orderness (10 seconds, in this example).

{% top %}

## Windows

Flink features very expressive window semantics.

In this section you will learn:

* how windows are used to compute aggregates on unbounded streams,
* which types of windows Flink supports, and
* how to implement a DataStream program with a windowed aggregation

### Introduction

It is natural when doing stream processing to want to compute aggregated analytics on bounded subsets
of the streams in order to answer questions like these:

* number of page views per minute
* number of sessions per user per week
* maximum temperature per sensor per minute

Computing windowed analytics with Flink depends on two principal abstractions: _Window Assigners_
that assign events to windows (creating new window objects as necessary), and _Window Functions_
that are applied to the events assigned to a window.

Flink's windowing API also has notions of _Triggers_, which determine when to call the window
function, and _Evictors_, which can remove elements collected in a window.

In its basic form, you apply windowing to a keyed stream like this:

{% highlight java %}
stream.
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce|aggregate|process(<window function>)
{% endhighlight %}

You can also use windowing with non-keyed streams, but keep in mind that in this case, the
processing will _not_ be done in parallel:

{% highlight java %}
stream.
    .windowAll(<window assigner>)
    .reduce|aggregate|process(<window function>)
{% endhighlight %}

### Window Assigners

Flink has several built-in types of window assigners, which are illustrated below:

<img src="{{ site.baseurl }}/fig/window-assigners.svg" alt="Window assigners" class="center" width="80%" />

Some examples of what these window assigners might be used for, and how to specify them:

* Tumbling time windows
  * _page views per minute_
  * `TumblingEventTimeWindows.of(Time.minutes(1))`
* Sliding time windows
  * _page views per minute computed every 10 seconds_
  * `SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10))`
* Session windows 
  * _page views per session, where sessions are defined by a gap of at least 30 minutes between sessions_
  * `EventTimeSessionWindows.withGap(Time.minutes(30))`

Durations can be specified using one of `Time.milliseconds(n)`, `Time.seconds(n)`, `Time.minutes(n)`, `Time.hours(n)`, and `Time.days(n)`.

The time-based window assigners (including session windows) come in both event time and processing
time flavors. There are significant tradeoffs between these two types of time windows. With
processing time windowing you have to accept these limitations:

* can not correctly process historic data,
* can not correctly handle out-of-order data,
* results will be non-deterministic,

but with the advantage of lower latency. 

When working with count-based windows, keep in mind that these windows will not fire until a batch
is complete. There's no option to time-out and process a partial window, though you could implement
that behavior yourself with a custom Trigger.

A global window assigner assigns every event (with the same key) to the same global window. This is
only useful if you are going to do your own custom windowing, with a custom Trigger. In many cases
where this might seem useful you will be better off using a `ProcessFunction` as described
[in another section]({% link training/event_driven.md %}#process-functions).

### Window Functions

You have three basic options for how to process the contents of your windows:

1. as a batch, using a `ProcessWindowFunction` that will be passed an `Iterable` with the window's contents;
1. incrementally, with a `ReduceFunction` or an `AggregateFunction` that is called as each event is assigned to the window;
1. or with a combination of the two, wherein the pre-aggregated results of a `ReduceFunction` or an `AggregateFunction` are supplied to a `ProcessWindowFunction` when the window is triggered.

Here are examples of approaches 1 and 3. Each implementation finds the peak value from each sensor
in 1 minute event time windows, and producing a stream of Tuples containing `(key,
end-of-window-timestamp, max_value)`.

#### ProcessWindowFunction Example

{% highlight java %}
DataStream<SensorReading> input = ...

input
    .keyBy(x -> x.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(new MyWastefulMax());

public static class MyWastefulMax extends ProcessWindowFunction<
        SensorReading,                  // input type
        Tuple3<String, Long, Integer>,  // output type
        String,                         // key type
        TimeWindow> {                   // window type
    
    @Override
    public void process(
            String key,
            Context context, 
            Iterable<SensorReading> events,
            Collector<Tuple3<String, Long, Integer>> out) {

        int max = 0;
        for (SensorReading event : events) {
            max = Math.max(event.value, max);
        }
        out.collect(Tuple3.of(key, context.window().getEnd(), max));
    }
}
{% endhighlight %}

A couple of things to note in this implementation:

* All of the events assigned to the window have to be buffered in keyed Flink state until the window
  is triggered. This is potentially quite expensive.
* Our `ProcessWindowFunction` is being passed a `Context` object from which contains information about
  the window. Its interface looks like this:

{% highlight java %}
public abstract class Context implements java.io.Serializable {
    public abstract W window();
    
    public abstract long currentProcessingTime();
    public abstract long currentWatermark();

    public abstract KeyedStateStore windowState();
    public abstract KeyedStateStore globalState();
}
{% endhighlight %}

`windowState` and `globalState` are places where you can store per-key, per-window, or global
per-key information for all windows of that key. This might be useful, for example, if you want to
record something about the
current window and use that when processing a subsequent window.

#### Incremental Aggregation Example

{% highlight java %}
DataStream<SensorReading> input = ...

input
    .keyBy(x -> x.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .reduce(new MyReducingMax(), new MyWindowFunction());

private static class MyReducingMax implements ReduceFunction<SensorReading> {
    public SensorReading reduce(SensorReading r1, SensorReading r2) {
        return r1.value() > r2.value() ? r1 : r2;
    }
}

private static class MyWindowFunction extends ProcessWindowFunction<
    SensorReading, Tuple3<String, Long, SensorReading>, String, TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<SensorReading> maxReading,
            Collector<Tuple3<String, Long, SensorReading>> out) {

        SensorReading max = maxReading.iterator().next();
        out.collect(Tuple3.of(key, context.window().getEnd(), max));
    }
}
{% endhighlight %}

Notice that the `Iterable<SensorReading>`
will contain exactly one reading -- the pre-aggregated maximum computed by `MyReducingMax`.

### Late Events

By default, when using event time windows, late events are dropped. There are two optional parts of
the window API that give you more control over this.

You can arrange for the events that would be dropped to be collected to an alternate output stream
instead, using a mechanism called
[Side Outputs]({% link training/event_driven.md %}#side-outputs).
Here is an example of what that might look like:

{% highlight java %}
OutputTag<Event> lateTag = new OutputTag<Event>("late"){};

SingleOutputStreamOperator<Event> result = stream.
    .keyBy(...)
    .window(...)
    .sideOutputLateData(lateTag)
    .process(...);
  
DataStream<Event> lateStream = result.getSideOutput(lateTag);
{% endhighlight %}

You can also specify an interval of _allowed lateness_ during which the late events will continue to
be assigned to the appropriate window(s) (whose state will have been retained). By default each late
event will cause the window function to be called again (sometimes called a _late firing_).

By default the allowed lateness is 0. In other words, elements behind the watermark are dropped (or
sent to the side output).

For example:

{% highlight java %}
stream.
    .keyBy(...)
    .window(...)
    .allowedLateness(Time.seconds(10))
    .process(...);
{% endhighlight %}

When the allowed lateness is greater than zero, only those events that are so late that they would
be dropped are sent to the side output (if it has been configured).

### Surprises

Some aspects of Flink's windowing API may not behave in the way you would expect. Based on
frequently asked questions on the [flink-user mailing
list](https://flink.apache.org/community.html#mailing-lists) and elsewhere, here are some facts
about windows that may surprise you.

#### Sliding Windows Make Copies

Sliding window assigners can create lots of window objects, and will copy each event into every
relevant window. For example, if you have sliding windows every 15 minutes that are 24-hours in
length, each event will be copied into 4 * 24 = 96 windows.

#### Time Windows are Aligned to the Epoch

Just because you are using hour-long processing-time windows and start your application running at
12:05 does not mean that the first window will close at 1:05. The first window will be 55 minutes
long and close at 1:00.

Note, however, that the tumbling and sliding window assigners take an optional offset parameter
that can be used to change the alignment of the windows. See
[Tumbling Windows]({% link dev/stream/operators/windows.md %}#tumbling-windows) and
[Sliding Windows]({% link dev/stream/operators/windows.md %}#sliding-windows) for details.

#### Windows Can Follow Windows

For example, it works to do this:

{% highlight java %}
stream
    .keyBy(t -> t.key)
    .timeWindow(<time specification>)
    .reduce(<reduce function>)
    .timeWindowAll(<same time specification>)
    .reduce(<same reduce function>)
{% endhighlight %}

You might expect Flink's runtime to be smart enough to do this parallel pre-aggregation for you
(provided you are using a ReduceFunction or AggregateFunction), but it's not.

The reason why this works is that the events produced by a time window are assigned timestamps
based on the time at the end of the window. So, for example, all of the events produced
by an hour-long window will have timestamps marking the end of an hour. Any subsequent window
consuming those events should have a duration that is the same as, or a multiple of, the
previous window.

#### No Results for Empty TimeWindows

Windows are only created when events are assigned to them. So if there are no events in a given time
frame, no results will be reported.

#### Late Events Can Cause Late Merges

Session windows are based on an abstraction of windows that can _merge_. Each element is initially
assigned to a new window, after which windows are merged whenever the gap between them is small
enough. In this way, a late event can bridge the gap separating two previously separate sessions,
producing a late merge.

{% top %}

## Hands-on

The hands-on exercise that goes with this section is the [Hourly Tips
Exercise](https://github.com/apache/flink-training/tree/{% if site.is_stable %}release-{{ site.version_title }}{% else %}master{% endif %}/hourly-tips).

{% top %}

## Further Reading

- [Timely Stream Processing]({% link concepts/timely-stream-processing.md %})
- [Windows]({% link dev/stream/operators/windows.md %})

{% top %}
