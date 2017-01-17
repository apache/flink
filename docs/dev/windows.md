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

The general structure of a windowed Flink program is presented below. This is also going to serve as a roadmap for 
the rest of the page.

    stream
           .keyBy(...)          <-  keyed versus non-keyed windows
           .window(...)         <-  required: "assigner"
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

## Window Assigner

After specifying whether your stream is keyed or not, the next step is to specify a *windowing strategy*. 
This will dictate how your elements will be assigned into windows and the general way to do so, is by specifying the 
`WindowAssigner` that corresponds to the windowing strategy of your choice in the `window(...)` call. 

The `WindowAssigner` is responsible for assigning each incoming element to one or more windows. Flink comes with 
pre-implemented window assigners for the most typical use cases, namely *tumbling windows*, *sliding windows*, 
*session windows* and *global windows*, but you can implement your own by extending the `WindowAssigner` class. All the 
built-in window assigners, except for the *global windows* one, assign elements to windows based on time, which can 
either be *processing* time or *event* time. A list of the pre-implemented `WindowAssigners` follows:

* **TumblingTimeWindows**: non-overlapping windows of a user-specified *duration* `d`. Each element belongs to a single 
            window based on its timestamp. As an example, an element with timestamp 12.01 and a tumbling windowing 
            strategy with `d = 5 min` will be assigned to the window `[start=12.00, end=12.05]`.

* **SlidingTimeWindows**: overlapping windows of a user-specified *duration* `d` and a *slide* `s`. Each element belongs to 
            a `d/s` windows based on its timestamp. Our element with timestamp 12.01 and a sliding windowing 
            strategy with `d = 5 min` and `s = 1 min` will be assigned to the windows `[start=12.00, end=12.05]`, 
            `[start=12.01, end=12.06]`, `[start=12.02, end=12.07]`, `[start=12.03, end=12.08]`, `[start=12.04, end=12.09]`.

* **TimeSessionWindows**: contrary to the previous assigners, here the window boundaries depend on the incoming data. The user 
            specifies a gap `g` and if there is a period of inactivity for the stream of more than `g` time units, then the 
            open window closes and a new, clean one opens to receive new events. In case we are operating in `event time`, two 
            consecutive session windows can be *merged* if an element arrives and makes the gap between the two windows less than 
            `g`. This is the reason why session windows belong to the category of `MergingWindowAssigners`. To illustrate
            the latter, if `g = 5` and we have two windows `w1=[start=12.00, end=12.05]` and `w2=[start=12.10, end=12.15]`,
            then if element `e` arrives with timestamp `t=12.03`, the two previous windows will be merged into one, 
            `w3=[start=12.00, end=12.15]`.

* **GlobalWindows**: this is a way of specifying that we don’t want to subdivide our elements into windows. All elements are 
            assigned to the same per-key global window. This implies that this strategy is only meaningful when combined 
            with a custom trigger, *e.g.* a `CountTrigger`, which will tell the window when to fire.

## Window Function

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

A reduce function specifies how two values can be combined to form one element. Flink can use this
to incrementally aggregate the elements in a window.

A `ReduceFunction` can be used in a program like this:

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

A `ReduceFunction` specifies how two elements from the input can be combined to produce
an output element. This example will sum up the second field of the tuple for all elements
in a window.

### FoldFunction

A fold function can be specified like this:

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

A `FoldFunction` specifies how elements from the input will be added to an initial
accumulator value (`""`, the empty string, in our example). This example will compute
a concatenation of all the `Long` fields of the input.

<span class="label label-danger">Attention</span> `fold()` cannot be used with session windows or other mergable windows.

### WindowFunction - The Generic Case

Using a `WindowFunction` provides the most flexibility, at the cost of performance. The reason for this
is that elements cannot be incrementally aggregated for a window and instead need to be buffered
internally until the window is considered ready for processing. A `WindowFunction` gets an
`Iterable` containing all the elements of the window being processed. The signature of
`WindowFunction` is this:

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

Here we show an example that uses a `WindowFunction` to count the elements in a window. We do this
because we want to access information about the window itself to emit it along with the count.
This is very inefficient, however, and should be implemented with a
`ReduceFunction` in practice. Below, we will see an example of how a `ReduceFunction` can
be combined with a `WindowFunction` to get both incremental aggregation and the added
information of a `WindowFunction`.

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

The next step is optional and consists in specifying the trigger that fits your needs. 
A `Trigger` determines when a window (as formed by the `WindowAssigner`) is ready to be
processed by the *window function*. 

The trigger interface provides five methods that react to different events. These are the `onElement()` which
observes and can react to the event of an element being added to a window, the `onEventTime()` which reacts to the 
event of a registered event-time timer firing, the `onProcessingTime()` which reacts to the event of a registered 
processing-time timer firing, the `onMerge()` which is relevant for stateful triggers and merges the states of 
two triggers when their corresponding windows merge, *e.g.* when using session windows, and the `clear()` which 
performs any action needed upon removal of the corresponding window. On all the previous occasions, the trigger can
register processing- or event-time timers for future actions. 

Once a trigger determines that a window is ready for processing, it fires. This is the signal for the window operator to 
take the elements that are currently in the window and pass them to the window function (or the evictor if one is 
specified) to produce the output for the firing window. When a trigger fires, it can either `FIRE` or `FIRE_AND_PURGE`. 
The difference is that the latter cleans up the elements in the fired window after firing, while the former keeps them. 
By default, the pre-implemented triggers simply `FIRE` without purging the window state.

<span class="label label-danger">Attention</span> When purging, only the contents of the window are cleared and not the
window metadata.

Each `WindowAssigner` (except `GlobalWindows`) comes with a default trigger that should be
appropriate for most use cases. For example, all the event-time window assigners have an `EventTimeTrigger` as
default trigger. This trigger simply fires once the watermark passes the end of a window. If the default 
trigger of your window assigner is enough, then you do not need to explicitly specify it. In other cases 
you can specify a trigger using the `trigger(...)` method with a given `Trigger`.

Flink comes with a few triggers out-of-box: there is the already mentioned `EventTimeTrigger` that
fires based on the progress of event-time as measured by watermarks, the `ProcessingTimeTrigger`
which does the same but based on processing time, the `CountTrigger` which fires once the number of elements
in a window exceeds the given limit, and the `PurgingTrigger` which takes as argument another trigger and 
transforms it into a purging one. 

<span class="label label-danger">Attention</span> By specifying a trigger using `trigger()` you
are overwriting the default trigger of a `WindowAssigner`. For example, if you specify a
`CountTrigger` for `TumblingEventTimeWindows` you will no longer get window firings based on the
progress of time but only by count. Right now, you have to write your own custom trigger if
you want to react based on both time and count.

The internal `Trigger` API is still considered experimental but you can check out the code
if you want to write your own custom trigger:
{% gh_link /flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java "Trigger.java" %}.

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

When working with *event-time* windowing it can happen that elements arrive late, *i.e.* the watermark that Flink uses to 
keep track of the progress of event-time is already past the end timestamp of a window to which an element belongs. See 
[event time](./event_time.html) and especially [late elements](./event_time.html#late-elements) for a more thorough 
discussion of how Flink deals with event time.

By default, late data is dropped from a window and is *not* considered when evaluating the window function. For cases 
that this is too strict, Flink allows you to specify a maximum *allowed lateness*. This specifies by how much time
elements can be late while not being dropped. Elements that arrive within the allowed lateness are still put into windows
and are considered when computing window results. Flink will also make sure that for time-based windows, any state held 
by the windowing operation is cleaned up once the watermark passes the end of a window plus the allowed lateness, as 
also stated in the section describing the [Window Lifecycle](#window-lifecycle).

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
the end of the window. In these cases, when a late but not dropped element arrives, it will trigger another firing for the 
window. These firings are called `late firings`, as they are triggered by late events and in contrast to the `main firing` 
which is the first firing of the window. In case of session windows, late firings can further lead to merging of windows,
as they may "bridge" the gap between two pre-existing, unmerged windows.

## Useful state size considerations

The main rules to keep in mind when estimating the storage needs of your windowing computation are:
 
1. Flink creates one copy of each element per window to which it belongs. Given this, tumbling windows keep one copy of each 
element, as each element belongs to at most one window (0 if the element is late), while sliding windows create `d/s`
copies of each element, as explained in the [Window Assigners](#window-assigners) section.

2. If `fold()` or `reduce()` functions are used, there is only one value kept per window, while `apply()` keeps all the 
elements. 

3. Using an `Evictor` voids any pre-aggregation, as all the elements of a window have to be passed through the evictor 
before applying the computation (see [Evictors](#evictors)).
