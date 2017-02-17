---
title: "Process Function (Low-level Operations)"
nav-title: "Process Function"
nav-parent_id: streaming
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
  - state (fault tolerant, consistent)
  - timers (event time and processing time)

The `ProcessFunction` can be thought of as a `FlatMapFunction` with access to keyed state and timers. It handles events
by being invoked for each event received in the input stream(s).

For fault tolerant state, the `ProcessFunction` gives access to Flink's [keyed state](state.html), accessible via the
`RuntimeContext`, similar to the way other stateful functions can access keyed state. Like all functions with keyed state,
the `ProcessFunction` needs to be applied onto a `KeyedStream`:
```java
stream.keyBy("id").process(new MyProcessFunction())
```

The timers allow applications to react to changes in processing time and in [event time](../event_time.html).
Every call to the function `processElement(...)` gets a `Context` object with gives access to the element's
event time timestamp, and to the *TimerService*. The `TimerService` can be used to register callbacks for future
event-/processing- time instants. When a timer's particular time is reached, the `onTimer(...)` method is
called. During that call, all states are again scoped to the key with which the timer was created, allowing
timers to perform keyed state manipulation as well.


## Low-level Joins

To realize low-level operations on two inputs, applications can use `CoProcessFunction`. It relates to `ProcessFunction`
in the same way that `CoFlatMapFunction` relates to `FlatMapFunction`: the function is bound to two different inputs and
gets individual calls to `processElement1(...)` and `processElement2(...)` for records from the two different inputs.

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

*Note:* This simple example could have been implemented with session windows. We use `ProcessFunction` here to illustrate
the basic pattern it provides.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
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
public class CountWithTimeoutFunction extends RichProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out)
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
        ctx.timerService().registerEventTimeTimer(current.timestamp + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified) {
            // emit the state
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
class TimeoutStateFunction extends ProcessFunction[(String, Long), (String, Long)] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext()
      .getState(new ValueStateDescriptor<>("myState", clasOf[CountWithTimestamp]))


  override def processElement(value: (String, Long), ctx: Context, out: Collector[(String, Long)]): Unit = {
    // initialize or retrieve/update the state

    val current: CountWithTimestamp = state.value match {
      case null => 
        CountWithTimestamp(key, 1, ctx.timestamp)
      case CountWithTimestamp(key, count, time) =>
        CountWithTimestamp(key, count + 1, ctx.timestamp)
    }

    // write the state back
    state.update(current)

    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.timestamp + 60000)
  }

  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[(String, Long)]): Unit = {
    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (lastModified == timestamp) => 
        out.collect((key, count))
      case _ =>
    }
  }
}
{% endhighlight %}
</div>
</div>

{% top %}
