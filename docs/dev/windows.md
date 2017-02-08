---
title: "Windows"
nav-parent_id: streaming
nav-id: windows
nav-pos: 10
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

Windows are at the heart of processing infinite streams. Windows split the stream into "buckets" of finite size,
over which we can apply computations. This document focuses on how windowing is performed in Flink and how the
programmer can benefit to the maximum from its offered functionality.

The general structure of a windowed Flink program is presented below. The first snippet refers to *keyed* streams,
while the second to *non-keyed* ones. As one can see, the only difference is the `keyBy(...)` call for the keyed streams
and the `window(...)` which becomes `windowAll(...)` for non-keyed streams. These is also going to serve as a roadmap
for the rest of the page.

**Keyed Windows**

    stream
           .keyBy(...)          <-  keyed versus non-keyed windows
           .window(...)         <-  required: "assigner"
          [.trigger(...)]       <-  optional: "trigger" (else default trigger)
          [.evictor(...)]       <-  optional: "evictor" (else no evictor)
          [.allowedLateness()]  <-  optional, else zero
           .reduce/fold/apply() <-  required: "function"

**Non-Keyed Windows**

    stream
           .windowAll(...)      <-  required: "assigner"
          [.trigger(...)]       <-  optional: "trigger" (else default trigger)
          [.evictor(...)]       <-  optional: "evictor" (else no evictor)
          [.allowedLateness()]  <-  optional, else zero
           .reduce/fold/apply() <-  required: "function"

In the above, the commands in square brackets ([...]) are optional. This reveals that Flink allows you to customize your
windowing logic in many different ways so that it best fits your needs.

* This will be replaced by the TOC
{:toc}

## Window Lifecycle

In a nutshell, a window is **created** as soon as the first element that should belong to this window arrives, and the
window is **completely removed** when the time (event or processing time) passes its end timestamp plus the user-specified
`allowed lateness` (see [Allowed Lateness](#allowed-lateness)). Flink guarantees removal only for time-based
windows and not for other types, *e.g.* global windows (see [Window Assigners](#window-assigners)). For example, with an
event-time-based windowing strategy that creates non-overlapping (or tumbling) windows every 5 minutes and has an allowed
lateness of 1 min, Flink will create a new window for the interval between `12:00` and `12:05` when the first element with
a timestamp that falls into this interval arrives, and it will remove it when the watermark passes the `12:06`
timestamp.

In addition, each window will have a `Trigger` (see [Triggers](#triggers)) and a function (`WindowFunction`, `ReduceFunction` or
`FoldFunction`) (see [Window Functions](#window-functions)) attached to it. The function will contain the computation to
be applied to the contents of the window, while the `Trigger` specifies the conditions under which the window is
considered ready for the function to be applied. A triggering policy might be something like "when the number of elements
in the window is more than 4", or "when the watermark passes the end of the window". A trigger can also decide to
purge a window's contents any time between its creation and removal. Purging in this case only refers to the elements
in the window, and *not* the window metadata. This means that new data can still be added to that window.

Apart from the above, you can specify an `Evictor` (see [Evictors](#evictors)) which will be able to remove
elements from the window after the trigger fires and before and/or after the function is applied.

In the following we go into more detail for each of the components above. We start with the required parts in the above
snippet (see [Keyed vs Non-Keyed Windows](#keyed-vs-non-keyed-windows), [Window Assigner](#window-assigner), and
[Window Function](#window-function)) before moving to the optional ones.

## Keyed vs Non-Keyed Windows

The first thing to specify is whether your stream should be keyed or not. This has to be done before defining the window.
Using the `keyBy(...)` will split your infinite stream into logical keyed streams. If `keyBy(...)` is not called, your
stream is not keyed.

In the case of keyed streams, any attribute of your incoming events can be used as a key
(more details [here]({{ site.baseurl }}/dev/api_concepts.html#specifying-keys)). Having a keyed stream will
allow your windowed computation to be performed in parallel by multiple tasks, as each logical keyed stream can be processed
independently from the rest. All elements referring to the same key will be sent to the same parallel task.

In case of non-keyed streams, your original stream will not be split into multiple logical streams and all the windowing logic
will be performed by a single task, *i.e.* with parallelism of 1.

## Window Assigners

After specifying whether your stream is keyed or not, the next step is to define a *window assigner*.
The window assigner defines how elements are assigned to windows. This is done by specifying the `WindowAssigner`
of your choice in the `window(...)` (for *keyed* streams) or the `windowAll()` (for *non-keyed* streams) call.

A `WindowAssigner` is responsible for assigning each incoming element to one or more windows. Flink comes
with pre-defined window assigners for the most common use cases, namely *tumbling windows*,
*sliding windows*, *session windows* and *global windows*. You can also implement a custom window assigner by
extending the `WindowAssigner` class. All built-in window assigners (except the global
windows) assign elements to windows based on time, which can either be processing time or event
time. Please take a look at our section on [event time]({{ site.baseurl }}/dev/event_time.html) to learn
about the difference between processing time and event time and how timestamps and watermarks are generated.

In the following, we show how Flink's pre-defined window assigners work and how they are used
in a DataStream program. The following figures visualize the workings of each assigner. The purple circles
represent elements of the stream, which are partitioned by some key (in this case *user 1*, *user 2* and *user 3*).
The x-axis shows the progress of time.

### Tumbling Windows

A *tumbling windows* assigner assigns each element to a window of a specified *window size*.
Tumbling windows have a fixed size and do not overlap. For example, if you specify a tumbling
window with a size of 5 minutes, the current window will be evaluated and a new window will be
started every five minutes as illustrated by the following figure.

<img src="{{ site.baseurl }}/fig/tumbling-windows.svg" class="center" style="width: 100%;" />

The following code snippets show how to use tumbling windows.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<T> input = ...;

// tumbling event-time windows
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// tumbling processing-time windows
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// daily tumbling event-time windows offset by -8 hours.
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[T] = ...

// tumbling event-time windows
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// tumbling processing-time windows
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>)

// daily tumbling event-time windows offset by -8 hours.
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
{% endhighlight %}
</div>
</div>

Time intervals can be specified by using one of `Time.milliseconds(x)`, `Time.seconds(x)`,
`Time.minutes(x)`, and so on.

As shown in the last example, tumbling window assigners also take an optional `offset`
parameter that can be used to change the alignment of windows. For example, without offsets
hourly tumbling windows are aligned with epoch, that is you will get windows such as
`1:00:00.000 - 1:59:59.999`, `2:00:00.000 - 2:59:59.999` and so on. If you want to change
that you can give an offset. With an offset of 15 minutes you would, for example, get
`1:15:00.000 - 2:14:59.999`, `2:15:00.000 - 3:14:59.999` etc.
An important use case for offsets is to adjust windows to timezones other than UTC-0.
For example, in China you would have to specify an offset of `Time.hours(-8)`.

### Sliding Windows

The *sliding windows* assigner assigns elements to windows of fixed length. Similar to a tumbling
windows assigner, the size of the windows is configured by the *window size* parameter.
An additional *window slide* parameter controls how frequently a sliding window is started. Hence,
sliding windows can be overlapping if the slide is smaller than the window size. In this case elements
are assigned to multiple windows.

For example, you could have windows of size 10 minutes that slides by 5 minutes. With this you get every
5 minutes a window that contains the events that arrived during the last 10 minutes as depicted by the
following figure.

<img src="{{ site.baseurl }}/fig/sliding-windows.svg" class="center" style="width: 100%;" />

The following code snippets show how to use sliding windows.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<T> input = ...;

// sliding event-time windows
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// sliding processing-time windows offset by -8 hours
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[T] = ...

// sliding event-time windows
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// sliding processing-time windows
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>)

// sliding processing-time windows offset by -8 hours
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
{% endhighlight %}
</div>
</div>

Time intervals can be specified by using one of `Time.milliseconds(x)`, `Time.seconds(x)`,
`Time.minutes(x)`, and so on.

As shown in the last example, sliding window assigners also take an optional `offset` parameter
that can be used to change the alignment of windows. For example, without offsets hourly windows
sliding by 30 minutes are aligned with epoch, that is you will get windows such as
`1:00:00.000 - 1:59:59.999`, `1:30:00.000 - 2:29:59.999` and so on. If you want to change that
you can give an offset. With an offset of 15 minutes you would, for example, get
`1:15:00.000 - 2:14:59.999`, `1:45:00.000 - 2:44:59.999` etc.
An important use case for offsets is to adjust windows to timezones other than UTC-0.
For example, in China you would have to specify an offset of `Time.hours(-8)`.

### Session Windows

The *session windows* assigner groups elements by sessions of activity. Session windows do not overlap and
do not have a fixed start and end time, in contrast to *tumbling windows* and *sliding windows*. Instead a
session window closes when it does not receive elements for a certain period of time, *i.e.*, when a gap of
inactivity occurred. A session window assigner is configured with the *session gap* which
defines how long is the required period of inactivity. When this period expires, the current session closes
and subsequent elements are assigned to a new session window.

<img src="{{ site.baseurl }}/fig/session-windows.svg" class="center" style="width: 100%;" />

The following code snippets show how to use session windows.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<T> input = ...;

// event-time session windows
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);

// processing-time session windows
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[T] = ...

// event-time session windows
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)

// processing-time session windows
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>)
{% endhighlight %}
</div>
</div>

Time intervals can be specified by using one of `Time.milliseconds(x)`, `Time.seconds(x)`,
`Time.minutes(x)`, and so on.

<span class="label label-danger">Attention</span> Since session windows do not have a fixed start and end,
they are  evaluated differently than tumbling and sliding windows. Internally, a session window operator
creates a new window for each arriving record and merges windows together if their are closer to each other
than the defined gap.
In order to be mergeable, a session window operator requires a merging [Trigger](#triggers) and a merging
[Window Function](#window-functions), such as `ReduceFunction` or `WindowFunction`
(`FoldFunction` cannot merge.)

### Global Windows

A *global windows* assigner assigns all elements with the same key to the same single *global window*.
This windowing scheme is only useful if you also specify a custom [trigger](#triggers). Otherwise,
no computation will be performed, as the global window does not have a natural end at
which we could process the aggregated elements.

<img src="{{ site.baseurl }}/fig/non-windowed.svg" class="center" style="width: 100%;" />

The following code snippets show how to use a global window.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[T] = ...

input
    .keyBy(<key selector>)
    .window(GlobalWindows.create())
    .<windowed transformation>(<window function>)
{% endhighlight %}
</div>
</div>

## Window Functions

After defining the window assigner, we need to specify the computation that we want
to perform on each of these windows. This is the responsibility of the *window function*, which is used to process the
elements of each (possibly keyed) window once the system determines that a window is ready for processing
(see [triggers](#triggers) for how Flink determines when a window is ready).

The window function can be one of `ReduceFunction`, `FoldFunction` or `WindowFunction`. The first
two can be executed more efficiently (see [State Size](#state size) section) because Flink can incrementally aggregate
the elements for each window as they arrive. A `WindowFunction` gets an `Iterable` for all the elements contained in a
window and additional meta information about the window to which the elements belong.

A windowed transformation with a `WindowFunction` cannot be executed as efficiently as the other
cases because Flink has to buffer *all* elements for a window internally before invoking the function.
This can be mitigated by combining a `WindowFunction` with a `ReduceFunction` or `FoldFunction` to
get both incremental aggregation of window elements and the additional window metadata that the
`WindowFunction` receives. We will look at examples for each of these variants.

### ReduceFunction

A `ReduceFunction` specifies how two elements from the input are combined to produce
an output element of the same type. Flink uses a `ReduceFunction` to incrementally aggregate
the elements of a window.

A `ReduceFunction` can be defined and used like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce(new ReduceFunction<Tuple2<String, Long>> {
      public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
      }
    });
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
{% endhighlight %}
</div>
</div>

The above example sums up the second fields of the tuples for all elements in a window.

### FoldFunction

A `FoldFunction` specifies how an input element of the window is combined with an element of
the output type. The `FoldFunction` is incrementally called for each element that is added
to the window and the current output value. The first element is combined with a pre-defined initial value of the output type.

A `FoldFunction` can be defined and used like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .fold("", new FoldFunction<Tuple2<String, Long>, String>> {
       public String fold(String acc, Tuple2<String, Long> value) {
         return acc + value.f1;
       }
    });
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .fold("") { (acc, v) => acc + v._2 }
{% endhighlight %}
</div>
</div>

The above example appends all input `Long` values to an initially empty `String`.

<span class="label label-danger">Attention</span> `fold()` cannot be used with session windows or other mergeable windows.

### WindowFunction - The Generic Case

A `WindowFunction` gets an `Iterable` containing all the elements of the window and provides
the most flexibility of all window functions. This comes
at the cost of performance and resource consumption, because elements cannot be incrementally
aggregated but instead need to be buffered internally until the window is considered ready for processing.

The signature of a `WindowFunction` looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {

  /**
   * Evaluates the window and outputs none or several elements.
   *
   * @param key The key for which this window is evaluated.
   * @param window The window that is being evaluated.
   * @param input The elements in the window being evaluated.
   * @param out A collector for emitting elements.
   *
   * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
   */
  void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception;
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {

  /**
    * Evaluates the window and outputs none or several elements.
    *
    * @param key    The key for which this window is evaluated.
    * @param window The window that is being evaluated.
    * @param input  The elements in the window being evaluated.
    * @param out    A collector for emitting elements.
    * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
    */
  def apply(key: KEY, window: W, input: Iterable[IN], out: Collector[OUT])
}
{% endhighlight %}
</div>
</div>

A `WindowFunction` can be defined and used like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, Long>> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .apply(new MyWindowFunction());

/* ... */

public class MyWindowFunction implements WindowFunction<Tuple<String, Long>, String, String, TimeWindow> {

  void apply(String key, TimeWindow window, Iterable<Tuple<String, Long>> input, Collector<String> out) {
    long count = 0;
    for (Tuple<String, Long> in: input) {
      count++;
    }
    out.collect("Window: " + window + "count: " + count);
  }
}

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[(String, Long)] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .apply(new MyWindowFunction())

/* ... */

class MyWindowFunction extends WindowFunction[(String, Long), String, String, TimeWindow] {

  def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): () = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"Window $window count: $count")
  }
}
{% endhighlight %}
</div>
</div>

The example shows a `WindowFunction` to count the elements in a window. In addition, the window function adds information about the window to the output.

<span class="label label-danger">Attention</span> Note that using `WindowFunction` for simple aggregates such as count is quite inefficient. The next section shows how a `ReduceFunction` can be combined with a `WindowFunction` to get both incremental aggregation and the added information of a `WindowFunction`.

### WindowFunction with Incremental Aggregation

A `WindowFunction` can be combined with either a `ReduceFunction` or a `FoldFunction` to
incrementally aggregate elements as they arrive in the window.
When the window is closed, the `WindowFunction` will be provided with the aggregated result.
This allows to incrementally compute windows while having access to the
additional window meta information of the `WindowFunction`.

#### Incremental Window Aggregation with FoldFunction

The following example shows how an incremental `FoldFunction` can be combined with
a `WindowFunction` to extract the number of events in the window and return also
the key and end time of the window.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<SensorReading> input = ...;

input
  .keyBy(<key selector>)
  .timeWindow(<window assigner>)
  .fold(new Tuple3<String, Long, Integer>("",0L, 0), new MyFoldFunction(), new MyWindowFunction())

// Function definitions

private static class MyFoldFunction
    implements FoldFunction<SensorReading, Tuple3<String, Long, Integer> > {

  public Tuple3<String, Long, Integer> fold(Tuple3<String, Long, Integer> acc, SensorReading s) {
      Integer cur = acc.getField(2);
      acc.setField(2, cur + 1);
      return acc;
  }
}

private static class MyWindowFunction
    implements WindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow> {

  public void apply(String key,
                    TimeWindow window,
                    Iterable<Tuple3<String, Long, Integer>> counts,
                    Collector<Tuple3<String, Long, Integer>> out) {
    Integer count = counts.iterator().next().getField(2);
    out.collect(new Tuple3<String, Long, Integer>(key, window.getEnd(),count));
  }
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

val input: DataStream[SensorReading] = ...

input
 .keyBy(<key selector>)
 .timeWindow(<window assigner>)
 .fold (
    ("", 0L, 0),
    (acc: (String, Long, Int), r: SensorReading) => { ("", 0L, acc._3 + 1) },
    ( key: String,
      window: TimeWindow,
      counts: Iterable[(String, Long, Int)],
      out: Collector[(String, Long, Int)] ) =>
      {
        val count = counts.iterator.next()
        out.collect((key, window.getEnd, count._3))
      }
  )

{% endhighlight %}
</div>
</div>

#### Incremental Window Aggregation with ReduceFunction

The following example shows how an incremental `ReduceFunction` can be combined with
a `WindowFunction` to return the smallest event in a window along
with the start time of the window.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<SensorReading> input = ...;

input
  .keyBy(<key selector>)
  .timeWindow(<window assigner>)
  .reduce(new MyReduceFunction(), new MyWindowFunction());

// Function definitions

private static class MyReduceFunction implements ReduceFunction<SensorReading> {

  public SensorReading reduce(SensorReading r1, SensorReading r2) {
      return r1.value() > r2.value() ? r2 : r1;
  }
}

private static class MyWindowFunction
    implements WindowFunction<SensorReading, Tuple2<Long, SensorReading>, String, TimeWindow> {

  public void apply(String key,
                    TimeWindow window,
                    Iterable<SensorReading> minReadings,
                    Collector<Tuple2<Long, SensorReading>> out) {
      SensorReading min = minReadings.iterator().next();
      out.collect(new Tuple2<Long, SensorReading>(window.getStart(), min));
  }
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

val input: DataStream[SensorReading] = ...

input
  .keyBy(<key selector>)
  .timeWindow(<window assigner>)
  .reduce(
    (r1: SensorReading, r2: SensorReading) => { if (r1.value > r2.value) r2 else r1 },
    ( key: String,
      window: TimeWindow,
      minReadings: Iterable[SensorReading],
      out: Collector[(Long, SensorReading)] ) =>
      {
        val min = minReadings.iterator.next()
        out.collect((window.getStart, min))
      }
  )

{% endhighlight %}
</div>
</div>

## Triggers

A `Trigger` determines when a window (as formed by the *window assigner*) is ready to be
processed by the *window function*. Each `WindowAssigner` comes with a default `Trigger`.
If the default trigger does not fit your needs, you can specify a custom trigger using `trigger(...)`.

The trigger interface has five methods that allow a `Trigger` to react to different events:

* The `onElement()` method is called for each element that is added to a window.
* The `onEventTime()` method is called when  a registered event-time timer fires.
* The `onProcessingTime()` method is called when a registered processing-time timer fires.
* The `onMerge()` method is relevant for stateful triggers and merges the states of two triggers when their corresponding windows merge, *e.g.* when using session windows.
* Finally the `clear()` method performs any action needed upon removal of the corresponding window.

Two things to notice about the above methods are:

1) The first three decide how to act on their invocation event by returning a `TriggerResult`. The action can be one of the following:

* `CONTINUE`: do nothing,
* `FIRE`: trigger the computation,
* `PURGE`: clear the elements in the window, and
* `FIRE_AND_PURGE`: trigger the computation and clear the elements in the window afterwards.

2) Any of these methods can be used to register processing- or event-time timers for future actions.

### Fire and Purge

Once a trigger determines that a window is ready for processing, it fires, *i.e.*, it returns `FIRE` or `FIRE_AND_PURGE`. This is the signal for the window operator
to emit the result of the current window. Given a window with a `WindowFunction`
all elements are passed to the `WindowFunction` (possibly after passing them to an evictor).
Windows with `ReduceFunction` of `FoldFunction` simply emit their eagerly aggregated result.

When a trigger fires, it can either `FIRE` or `FIRE_AND_PURGE`. While `FIRE` keeps the contents of the window, `FIRE_AND_PURGE` removes its content.
By default, the pre-implemented triggers simply `FIRE` without purging the window state.

<span class="label label-danger">Attention</span> Purging will simply remove the contents of the window and will leave any potential meta-information about the window and any trigger state intact.

### Default Triggers of WindowAssigners

The default `Trigger` of a `WindowAssigner` is appropriate for many use cases. For example, all the event-time window assigners have an `EventTimeTrigger` as
default trigger. This trigger simply fires once the watermark passes the end of a window.

<span class="label label-danger">Attention</span> The default trigger of the `GlobalWindow` is the `NeverTrigger` which does never fire. Consequently, you always have to define a custom trigger when using a `GlobalWindow`.

<span class="label label-danger">Attention</span> By specifying a trigger using `trigger()` you
are overwriting the default trigger of a `WindowAssigner`. For example, if you specify a
`CountTrigger` for `TumblingEventTimeWindows` you will no longer get window firings based on the
progress of time but only by count. Right now, you have to write your own custom trigger if
you want to react based on both time and count.

### Built-in and Custom Triggers

Flink comes with a few built-in triggers.

* The (already mentioned) `EventTimeTrigger` fires based on the progress of event-time as measured by watermarks.
* The `ProcessingTimeTrigger` fires based on processing time.
* The `CountTrigger` fires once the number of elements in a window exceeds the given limit.
* The `PurgingTrigger` takes as argument another trigger and transforms it into a purging one.

If you need to implement a custom trigger, you should check out the abstract
{% gh_link /flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java "Trigger" %} class.
Please note that the API is still evolving and might change in future versions of Flink.

## Evictors

Flink’s windowing model allows specifying an optional `Evictor` in addition to the `WindowAssigner` and the `Trigger`.
This can be done using the `evictor(...)` method (shown in the beginning of this document). The evictor has the ability
to remove elements from a window *after* the trigger fires and *before and/or after* the window function is applied.
To do so, the `Evictor` interface has two methods:

    /**
     * Optionally evicts elements. Called before windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

    /**
     * Optionally evicts elements. Called after windowing function.
     *
     * @param elements The elements currently in the pane.
     * @param size The current number of elements in the pane.
     * @param window The {@link Window}
     * @param evictorContext The context for the Evictor
     */
    void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

The `evictBefore()` contains the eviction logic to be applied before the window function, while the `evictAfter()`
contains the one to be applied after the window function. Elements evicted before the application of the window
function will not be processed by it.

Flink comes with three pre-implemented evictors. These are:

* `CountEvictor`: keeps up to a user-specified number of elements from the window and discards the remaining ones from
the beginning of the window buffer.
* `DeltaEvictor`: takes a `DeltaFunction` and a `threshold`, computes the delta between the last element in the
window buffer and each of the remaining ones, and removes the ones with a delta greater or equal to the threshold.
* `TimeEvictor`: takes as argument an `interval` in milliseconds and for a given window, it finds the maximum
timestamp `max_ts` among its elements and removes all the elements with timestamps smaller than `max_ts - interval`.

<span class="label label-info">Default</span> By default, all the pre-implemented evictors apply their logic before the
window function.

<span class="label label-danger">Attention</span> Specifying an evictor prevents any pre-aggregation, as all the
elements of a window have to be passed to the evictor before applying the computation.

<span class="label label-danger">Attention</span> Flink provides no guarantees about the order of the elements within
a window. This implies that although an evictor may remove elements from the beginning of the window, these are not
necessarily the ones that arrive first or last.


## Allowed Lateness

When working with *event-time* windowing, it can happen that elements arrive late, *i.e.* the watermark that Flink uses to
keep track of the progress of event-time is already past the end timestamp of a window to which an element belongs. See
[event time](./event_time.html) and especially [late elements](./event_time.html#late-elements) for a more thorough
discussion of how Flink deals with event time.

By default, late elements are dropped when the watermark is past the end of the window. However,
Flink allows to specify a maximum *allowed lateness* for window operators. Allowed lateness
specifies by how much time elements can be late before they are dropped, and its default value is 0.
Elements that arrive after the watermark has passed the end of the window but before it passes the end of
the window plus the allowed lateness, are still added to the window. Depending on the trigger used,
a late but not dropped element may cause the window to fire again. This is the case for the `EventTimeTrigger`.

In order to make this work, Flink keeps the state of windows until their allowed lateness expires. Once this happens, Flink removes the window and deletes its state, as
also described in the [Window Lifecycle](#window-lifecycle) section.

<span class="label label-info">Default</span> By default, the allowed lateness is set to
`0`. That is, elements that arrive behind the watermark will be dropped.

You can specify an allowed lateness like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<T> input = ...;

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .<windowed transformation>(<window function>);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[T] = ...

input
    .keyBy(<key selector>)
    .window(<window assigner>)
    .allowedLateness(<time>)
    .<windowed transformation>(<window function>)
{% endhighlight %}
</div>
</div>

<span class="label label-info">Note</span> When using the `GlobalWindows` window assigner no
data is ever considered late because the end timestamp of the global window is `Long.MAX_VALUE`.

### Late elements considerations

When specifying an allowed lateness greater than 0, the window along with its content is kept after the watermark passes
the end of the window. In these cases, when a late but not dropped element arrives, it could trigger another firing for the
window. These firings are called `late firings`, as they are triggered by late events and in contrast to the `main firing`
which is the first firing of the window. In case of session windows, late firings can further lead to merging of windows,
as they may "bridge" the gap between two pre-existing, unmerged windows.

<span class="label label-info">Attention</span> You should be aware that the elements emitted by a late firing should be treated as updated results of a previous computation, i.e., your data stream will contain multiple results for the same computation. Depending on your application, you need to take these duplicated results into account or deduplicate them.

## Useful state size considerations

Windows can be defined over long periods of time (such as days, weeks, or months) and therefore accumulate very large state. There are a couple of rules to keep in mind when estimating the storage requirements of your windowing computation:

1. Flink creates one copy of each element per window to which it belongs. Given this, tumbling windows keep one copy of each element (an element belongs to exactly window unless it is dropped late). In contrast, sliding windows create several of each element, as explained in the [Window Assigners](#window-assigners) section. Hence, a sliding window of size 1 day and slide 1 second might not be a good idea.

2. `FoldFunction` and `ReduceFunction` can significantly reduce the storage requirements, as they eagerly aggregate elements and store only one value per window. In contrast, just using a `WindowFunction` requires accumulating all elements.

3. Using an `Evictor` prevents any pre-aggregation, as all the elements of a window have to be passed through the evictor before applying the computation (see [Evictors](#evictors)).
