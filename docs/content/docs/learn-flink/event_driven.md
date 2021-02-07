---
title: Event-driven Applications
weight: 6
type: docs
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

# Event-driven Applications

## Process Functions

### Introduction

A `ProcessFunction` combines event processing with timers and state, making it a powerful building block for stream processing applications. This is the basis for creating event-driven applications with Flink. It is very similar to a `RichFlatMapFunction`, but with the addition of timers.

### Example

If you've done the
[hands-on exercise]({{< ref "docs/learn-flink/streaming_analytics" >}}#hands-on)
in the [Streaming Analytics training]({{< ref "docs/learn-flink/streaming_analytics" >}}),
you will recall that it uses a `TumblingEventTimeWindow` to compute the sum of the tips for
each driver during each hour, like this:

```java
// compute the sum of the tips per hour for each driver
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .process(new AddTips());
```

It is reasonably straightforward, and educational, to do the same thing with a
`KeyedProcessFunction`. Let us begin by replacing the code above with this:

```java
// compute the sum of the tips per hour for each driver
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));
```

In this code snippet a `KeyedProcessFunction` called `PseudoWindow` is being applied to a keyed
stream, the result of which is a `DataStream<Tuple3<Long, Long, Float>>` (the same kind of stream produced by the implementation that uses Flink's built-in time windows).

The overall outline of `PseudoWindow` has this shape:

```java
// Compute the sum of the tips for each driver in hour-long windows.
// The keys are driverIds.
public static class PseudoWindow extends 
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private final long durationMsec;

    public PseudoWindow(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    // Called once during initialization.
    public void open(Configuration conf) {
        . . .
    }

    @Override
    // Called as each fare arrives to be processed.
    public void processElement(
            TaxiFare fare,
            Context ctx,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }

    @Override
    // Called when the current watermark indicates that a window is now complete.
    public void onTimer(long timestamp, 
            OnTimerContext context, 
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }
}
```

Things to be aware of:

* There are several types of ProcessFunctions -- this is a `KeyedProcessFunction`, but there are also `CoProcessFunctions`, `BroadcastProcessFunctions`, etc. 

* A `KeyedProcessFunction` is a kind of `RichFunction`. Being a `RichFunction`, it has access to the `open` and `getRuntimeContext` methods needed for working with managed keyed state.

* There are two callbacks to implement: `processElement` and `onTimer`. `processElement` is called with each incoming event; `onTimer` is called when timers fire. These can be either event time or processing time timers. Both `processElement` and `onTimer` are provided with a context object that can be used to interact with a `TimerService` (among other things). Both callbacks are also passed a `Collector` that can be used to emit results.

#### The `open()` method

```java
// Keyed, managed state, with an entry for each window, keyed by the window's end time.
// There is a separate MapState object for each driver.
private transient MapState<Long, Float> sumOfTips;

@Override
public void open(Configuration conf) {

    MapStateDescriptor<Long, Float> sumDesc =
            new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
    sumOfTips = getRuntimeContext().getMapState(sumDesc);
}
```

Because the fare events can arrive out of order, it will sometimes be necessary to process events
for one hour before having finished computing the results for the previous hour. In fact, if the
watermarking delay is much longer than the window length, then there may be many windows open
simultaneously, rather than just two. This implementation supports this by using a `MapState` that
maps the timestamp for the end of each window to the sum of the tips for that window.

#### The `processElement()` method

```java
public void processElement(
        TaxiFare fare,
        Context ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long eventTime = fare.getEventTime();
    TimerService timerService = ctx.timerService();

    if (eventTime <= timerService.currentWatermark()) {
        // This event is late; its window has already been triggered.
    } else {
        // Round up eventTime to the end of the window containing this event.
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        // Schedule a callback for when the window has been completed.
        timerService.registerEventTimeTimer(endOfWindow);

        // Add this fare's tip to the running total for that window.
        Float sum = sumOfTips.get(endOfWindow);
        if (sum == null) {
            sum = 0.0F;
        }
        sum += fare.tip;
        sumOfTips.put(endOfWindow, sum);
    }
}
```

Things to consider:

* What happens with late events? Events that are behind the watermark (i.e., late) are being
  dropped. If you want to do something better than this, consider using a side output, which is
  explained in the [next section]({{< ref "docs/learn-flink/event_driven" >}}#side-outputs).

* This example uses a `MapState` where the keys are timestamps, and sets a `Timer` for that same
  timestamp. This is a common pattern; it makes it easy and efficient to lookup relevant information when the timer fires.

#### The `onTimer()` method

```java
public void onTimer(
        long timestamp, 
        OnTimerContext context, 
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long driverId = context.getCurrentKey();
    // Look up the result for the hour that just ended.
    Float sumOfTips = this.sumOfTips.get(timestamp);

    Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
    out.collect(result);
    this.sumOfTips.remove(timestamp);
}
```

Observations:

* The `OnTimerContext context` passed in to `onTimer` can be used to determine the current key.

* Our pseudo-windows are being triggered when the current watermark reaches the end of each hour, at
  which point `onTimer` is called. This onTimer method removes the related entry from `sumOfTips`,
  which has the effect of making it impossible to accommodate late events. This is the equivalent of
  setting the allowedLateness to zero when working with Flink's time windows.

### Performance Considerations

Flink provides `MapState` and `ListState` types that are optimized for RocksDB. Where possible,
these should be used instead of a `ValueState` object holding some sort of collection. The RocksDB
state backend can append to `ListState` without going through (de)serialization, and for `MapState`, each
key/value pair is a separate RocksDB object, so `MapState` can be efficiently accessed and updated.

{{< top >}}

## Side Outputs

### Introduction

There are several good reasons to want to have more than one output stream from a Flink operator, such as reporting:

* exceptions
* malformed events
* late events
* operational alerts, such as timed-out connections to external services

Side outputs are a convenient way to do this. Beyond error reporting, side outputs are also
a good way to implement an n-way split of a stream.

### Example

You are now in a position to do something with the late events that were ignored in the previous
section.

A side output channel is associated with an `OutputTag<T>`. These tags have generic types that
correspond to the type of the side output's `DataStream`, and they have names.

```java
private static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {};
```

Shown above is a static `OutputTag<TaxiFare>` that can be referenced both when emitting
late events in the `processElement` method of the `PseudoWindow`:

```java
if (eventTime <= timerService.currentWatermark()) {
    // This event is late; its window has already been triggered.
    ctx.output(lateFares, fare);
} else {
    . . .
}
```

and when accessing the stream from this side output in the `main` method of the job:

```java
// compute the sum of the tips per hour for each driver
SingleOutputStreamOperator hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));

hourlyTips.getSideOutput(lateFares).print();
```

Alternatively, you can use two OutputTags with the
same name to refer to the same side output, but if you do, they must have the same type.

{{< top >}}

## Closing Remarks

In this example you have seen how a `ProcessFunction` can be used to reimplement a straightforward time
window. Of course, if Flink's built-in windowing API meets your needs, by all means, go ahead and
use it. But if you find yourself considering doing something contorted with Flink's windows, don't
be afraid to roll your own.

Also, `ProcessFunctions` are useful for many other use cases beyond computing analytics. The hands-on
exercise below provides an example of something completely different.

Another common use case for ProcessFunctions is for expiring stale state. If you think back to the {{< training_link file="/rides-and-fares" name="Rides and Fares Exercise" >}},
where a `RichCoFlatMapFunction` is used to compute a simple join, the sample solution assumes that
the TaxiRides and TaxiFares are perfectly matched, one-to-one for each `rideId`. If an event is lost,
the other event for the same `rideId` will be held in state forever. This could instead be implemented
as a `KeyedCoProcessFunction`, and a timer could be used to detect and clear any stale state.

{{< top >}}

## Hands-on

The hands-on exercise that goes with this section is the {{< training_link file="/long-ride-alerts" name="Long Ride Alerts Exercise" >}}.

{{< top >}}

## Further Reading

- [ProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}})
- [Side Outputs]({{< ref "docs/dev/datastream/side_output" >}})

{{< top >}}
