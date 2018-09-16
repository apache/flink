---
title: "Process Function (Low-level Operations)"
nav-title: "Process Function"
nav-parent_id: streaming_operators
nav-pos: 35
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

## The ProcessFunction

The `ProcessFunction` is a low-level stream processing operation, giving access to the basic building blocks of
all (acyclic) streaming applications:

  - events (stream elements)
  - state (fault-tolerant, consistent, only on keyed stream)
  - timers (event time and processing time, only on keyed stream)

The `ProcessFunction` can be thought of as a `FlatMapFunction` with access to keyed state and timers. It handles events
by being invoked for each event received in the input stream(s).

For fault-tolerant state, the `ProcessFunction` gives access to Flink's [keyed state]({{ site.baseurl }}/dev/stream/state/state.html), accessible via the
`RuntimeContext`, similar to the way other stateful functions can access keyed state.

The timers allow applications to react to changes in processing time and in [event time]({{ site.baseurl }}/dev/event_time.html).
Every call to the function `processElement(...)` gets a `Context` object which gives access to the element's
event time timestamp, and to the *TimerService*. The `TimerService` can be used to register callbacks for future
event-/processing-time instants. When a timer's particular time is reached, the `onTimer(...)` method is
called. During that call, all states are again scoped to the key with which the timer was created, allowing
timers to manipulate keyed state.

<span class="label label-info">Note</span> If you want to access keyed state and timers you have
to apply the `ProcessFunction` on a keyed stream:

{% highlight java %}
stream.keyBy(...).process(new MyProcessFunction())
{% endhighlight %}


## Low-level Joins

To realize low-level operations on two inputs, applications can use `CoProcessFunction`. This
function is bound to two different inputs and gets individual calls to `processElement1(...)` and
`processElement2(...)` for records from the two different inputs.

Implementing a low level join typically follows this pattern:

  - Create a state object for one input (or both)
  - Update the state upon receiving elements from its input
  - Upon receiving elements from the other input, probe the state and produce the joined result

For example, you might be joining customer data to financial trades,
while keeping state for the customer data. If you care about having
complete and deterministic joins in the face of out-of-order events,
you can use a timer to evaluate and emit the join for a trade when the
watermark for the customer data stream has passed the time of that
trade.

## Example

The following example maintains counts per key, and emits a key/count pair whenever a minute passes (in event time) without an update for that key:

  - The count, key, and last-modification-timestamp are stored in a `ValueState`, which is implicitly scoped by key.
  - For each record, the `ProcessFunction` increments the counter and sets the last-modification timestamp
  - The function also schedules a callback one minute into the future (in event time)
  - Upon each callback, it checks the callback's event time timestamp against the last-modification time of the stored count
    and emits the key/count if they match (i.e., no further update occurred during that minute)

<span class="label label-info">Note</span> This simple example could have been implemented with
session windows. We use `ProcessFunction` here to illustrate the basic pattern it provides.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


// the source data stream
DataStream<Tuple2<String, String>> stream = ...;

// apply the process function onto a keyed stream
DataStream<Tuple2<String, Long>> result = stream
    .keyBy(0)
    .process(new CountWithTimeoutFunction());

/**
 * The data type stored in the state
 */
public class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out)
            throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction.Context
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext
import org.apache.flink.util.Collector

// the source data stream
val stream: DataStream[Tuple2[String, String]] = ...

// apply the process function onto a keyed stream
val result: DataStream[Tuple2[String, Long]] = stream
  .keyBy(0)
  .process(new CountWithTimeoutFunction())

/**
  * The data type stored in the state
  */
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

/**
  * The implementation of the ProcessFunction that maintains the count and timeouts
  */
class CountWithTimeoutFunction extends ProcessFunction[(String, String), (String, Long)] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))


  override def processElement(value: (String, String), ctx: Context, out: Collector[(String, Long)]): Unit = {
    // initialize or retrieve/update the state

    val current: CountWithTimestamp = state.value match {
      case null =>
        CountWithTimestamp(value._1, 1, ctx.timestamp)
      case CountWithTimestamp(key, count, lastModified) =>
        CountWithTimestamp(key, count + 1, ctx.timestamp)
    }

    // write the state back
    state.update(current)

    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[(String, Long)]): Unit = {
    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
        out.collect((key, count))
      case _ =>
    }
  }
}
{% endhighlight %}
</div>
</div>

{% top %}


**NOTE:** Before Flink 1.4.0, when called from a processing-time timer, the `ProcessFunction.onTimer()` method sets
the current processing time as event-time timestamp. This behavior is very subtle and might not be noticed by users. Well, it's
harmful because processing-time timestamps are indeterministic and not aligned with watermarks. Besides, user-implemented logic
depends on this wrong timestamp highly likely is unintendedly faulty. So we've decided to fix it. Upon upgrading to 1.4.0, Flink jobs
that are using this incorrect event-time timestamp will fail, and users should adapt their jobs to the correct logic.

## The KeyedProcessFunction

`KeyedProcessFunction`, as an extension of `ProcessFunction`, gives access to the key of timers in its `onTimer(...)`
method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT]): Unit = {
  var key = ctx.getCurrentKey
  // ...
}
{% endhighlight %}
</div>
</div>

## Timers

Both types of timers (processing-time and event-time) are internally maintained by the `TimerService` and enqueued for execution.

The `TimerService` deduplicates timers per key and timestamp, i.e., there is at most one timer per key and timestamp. If multiple timers are registered for the same timestamp, the `onTimer()` method will be called just once.

<span class="label label-info">Note</span> Flink synchronizes invocations of `onTimer()` and `processElement()`. Hence, users do not have to worry about concurrent modification of state.

### Fault Tolerance

Timers are fault tolerant and checkpointed along with the state of the application. 
In case of a failure recovery or when starting an application from a savepoint, the timers are restored.

<span class="label label-info">Note</span> Checkpointed processing-time timers that were supposed to fire before their restoration, will fire immediately.
This might happen when an application recovers from a failure or when it is started from a savepoint.

<span class="label label-info">Note</span> Timers are always asynchronously checkpointed, except for the combination of RocksDB backend / with incremental snapshots / with heap-based timers (will be resolved with `FLINK-10026`).
Notice that large numbers of timers can increase the checkpointing time because timers are part of the checkpointed state. See the "Timer Coalescing" section for advice on how to reduce the number of timers.

### Timer Coalescing

Since Flink maintains only one timer per key and timestamp, you can reduce the number of timers by reducing the timer resolution to coalesce them.

For a timer resolution of 1 second (event or processing time), you
can round down the target time to full seconds. Timers will fire at most 1 second earlier but not later than requested with millisecond accuracy. 
As a result, there are at most one timer per key and second.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val coalescedTime = ((ctx.timestamp + timeout) / 1000) * 1000
ctx.timerService.registerProcessingTimeTimer(coalescedTime)
{% endhighlight %}
</div>
</div>

Since event-time timers only fire with watermarks coming in, you may also schedule and coalesce
these timers with the next watermark by using the current one:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val coalescedTime = ctx.timerService.currentWatermark + 1
ctx.timerService.registerEventTimeTimer(coalescedTime)
{% endhighlight %}
</div>
</div>

Timers can also be stopped and removed as follows:

Stopping a processing-time timer:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long timestampOfTimerToStop = ...
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val timestampOfTimerToStop = ...
ctx.timerService.deleteProcessingTimeTimer(timestampOfTimerToStop)
{% endhighlight %}
</div>
</div>

Stopping an event-time timer:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
long timestampOfTimerToStop = ...
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val timestampOfTimerToStop = ...
ctx.timerService.deleteEventTimeTimer(timestampOfTimerToStop)
{% endhighlight %}
</div>
</div>

<span class="label label-info">Note</span> Stopping a timer has no effect if no such timer with the given timestamp is registered.
