---
title: "Windows"
weight: 8 
type: docs
aliases:
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Windows

Windows are at the heart of processing infinite streams. Windows split the stream into "buckets" 
of finite size, over which we can apply computations. This document focuses on how windowing is 
performed in Flink DataStream and how the programmer can benefit to the maximum from its offered 
functionality.

To utilize the Window functionality provided by DataStream API, users need to complete the following three steps:
1. Declare Window: Specify the type of window you wish to use.
2. Define `WindowProcessFunction`: Outline the logic that should be executed at various stages of the Window's lifecycle.
3. Combine the Window Declaration and `WindowProcessFunction` to build a `ProcessFunction`
   - Encapsulate the window declaration and the `WindowProcessFunction` into a single 
   `ProcessFunction`, which can then be leveraged within the DataStream API.

This section will provide a comprehensive overview of these three steps and include an example 
illustrating how to use Window in DataStream API.

## Declare Window

Users should first determine which type of window to use for their applications. We currently offer
three built-in window types: Time Window, Session Window and Global Window.

### Time Window

Time Windows are divided into multiple windows based on time ranges, and data is allocated to the 
corresponding window according to its timestamp. We support two types of Time Windows: 
tumbling windows and sliding windows. The time semantics within these windows can be classified 
as event time or processing time.

Please note that Time Windows are currently supported only in Keyed Partition Stream.

#### Tumbling Window

*Tumbling Window* assigns each element to a window of a specified *window size*.
Tumbling windows have a fixed size and do not overlap. For example, if you specify a tumbling
window with a size of 5 minutes, the current window will be evaluated and a new window will be
started every five minutes as illustrated by the following figure.

{{< img src="/fig/tumbling-windows.svg" alt="Tumbling Windows" >}}

The following code snippets show how to use tumbling windows.

```java
// create a tumbling window strategy with a window size of 60 seconds
WindowStrategy windowStrategy = WindowStrategy.tumbling(Duration.ofSeconds(60), WindowStrategy.EVENT_TIME);
```

Time intervals can be specified by using one of `Duration.ofMillis(x)`, `Duration.ofSeconds(x)`,
`Duration.ofMinutes(x)`, and so on.

#### Sliding Window

*Sliding Window* assigns elements to windows of fixed length. Similar to a tumbling
window, the size of the windows is configured by the *window size* parameter.
An additional *window slide* parameter controls how frequently a sliding window is started. Hence,
sliding windows can be overlapping if the slide is smaller than the window size. 
In this case elements are assigned to multiple windows.

For example, you could have windows of size 10 minutes that slides by 5 minutes. With this you 
get every 5 minutes a window that contains the events that arrived during the last 10 minutes 
as depicted by the following figure.

{{< img src="/fig/sliding-windows.svg" alt="sliding windows" >}}

The following code snippets show how to use sliding windows.

```java
// create a sliding window strategy with a window size of 60 seconds and a slide of 30 seconds
WindowStrategy windowStrategy = WindowStrategy.sliding(Duration.ofSeconds(60), Duration.ofSeconds(30), WindowStrategy.PROCESSING_TIME);
```

Time intervals can be specified by using one of `Duration.ofMillis(x)`, `Duration.ofSeconds(x)`,
`Duration.ofMinutes(x)`, and so on.

#### Allowed Lateness

When working with *event-time* windowing, it can happen that elements arrive late, *i.e.* the 
event time watermark that Flink uses to keep track of the progress of event-time is already past 
the end timestamp of a window to which an element belongs. See 
[event time]({{< ref "docs/dev/datastream-v2/time-processing/event_timer_service" >}}) for 
a more thorough discussion of how Flink deals with event time.

By default, late elements are dropped when the event time watermark is past the end of the window. 
However, Flink allows to specify a maximum *allowed lateness* for window operators. Allowed lateness
specifies by how much time elements can be late before they are dropped, and its default value is 0.

Elements that arrive after the event time watermark has passed the end of the window, but before 
it passes the end of the window plus the allowed lateness, are still added to the window.
The late but not dropped element will cause the window to fire again, and the late and dropped 
element will be processed by the [WindowProcessFunction#onLateRecord](#introduction-to-windowprocessfunction).

In order to make this work, Flink keeps the state of windows until their allowed lateness expires. 
Once this happens, Flink removes the window and deletes its state, as also described in 
the [Window Lifecycle](#window-lifecycle) section.

By default, the allowed lateness is set to `0`. That is, elements that arrive behind the watermark 
will be dropped.

You can specify an allowed lateness like this:

```java
// create a sliding window strategy with a window size of 60 seconds and a slide of 30 seconds and an allowed lateness of 10 seconds
WindowStrategy windowStrategy = WindowStrategy.sliding(Duration.ofSeconds(60), Duration.ofSeconds(30), WindowStrategy.PROCESSING_TIME, Duration.ofSeconds(10));
```

### Session Window

The *session windows* groups elements by sessions of activity. Session windows do not overlap and
do not have a fixed start and end time, in contrast to *tumbling windows* and *sliding windows*. 
Instead, a session window closes when it does not receive elements for a certain period of 
time, *i.e.*, when a gap of inactivity occurred. A session window can be configured 
with *session gap*. When this period expires, the current session closes and subsequent elements 
are assigned to a new session window.

{{< img src="/fig/session-windows.svg" alt="session windows" >}}

The following code snippets show how to declare session windows.

```java
// create a session window strategy with a session gap of 60 seconds
WindowStrategy windowStrategy = WindowStrategy.session(Duration.ofSeconds(60), WindowStrategy.EVENT_TIME);
```

Please note that Session Windows are supported only in Global Stream and Keyed Partition Stream.

### Global Window

*Global Window* refers to a scenario where all elements are assigned to a single, unified Window.
This windowing scheme is particularly useful in bounded stream scenarios, as it can be triggered
once all inputs have concluded. Note that it is compatible with Global Stream, Keyed Partition Stream, 
and Non-Keyed Partition Stream.

{{< img src="/fig/non-windowed.svg" alt="global windows" >}}

The following code snippets show how to declare a global window.

```java
// create a global window strategy
WindowStrategy windowStrategy = WindowStrategy.global();
```

## Define WindowProcessFunction
### Window Lifecycle
In a nutshell, a window is **created** as soon as the first element that should belong to this 
window arrives, and the window is **completely removed** when the time (event or processing time) 
passes its end timestamp plus the user-specified `allowed lateness` (see [Allowed Lateness](#allowed-lateness)). 
For example, with an event-time-based windowing that creates non-overlapping (or tumbling) windows 
every 5 minutes and has an allowed lateness of 1 min, Flink will create a new window for the 
interval between `12:00` and `12:05` when the first element with a timestamp that falls into this 
interval arrives, and it will remove it when the event time watermark passes the `12:06` timestamp.

Flink abstracts the essential actions within the window lifecycle into methods in 
the [WindowProcessFunction](#introduction-to-windowprocessfunction). Users are required to implement 
their own *WindowProcessFunction* to define their specific windowing computation logic.

### Introduction to WindowProcessFunction

The `WindowProcessFunction` is the main component that users need to implement to process the 
data in the window. After declaring a window, users need to define the operational logic for various 
stages of the window's lifecycle, called *WindowProcessFunction*.

Here is the interface of `OneInputWindowStreamProcessFunction`:

```java
/**
 * A type of {@link WindowProcessFunction} for one-input window processing.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 */
@Experimental
public interface OneInputWindowStreamProcessFunction<IN, OUT> extends WindowProcessFunction {

    /**
     * This method will be invoked when a record is received. Its default behaviors to store data in
     * built-in window state by {@link OneInputWindowContext#putRecord}. If the user overrides this
     * method, they have to take care of the input data themselves.
     */
    default void onRecord(
            IN record,
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception {
        windowContext.putRecord(record);
    }

    /**
     * This method will be invoked when the Window is triggered, you can obtain all the input
     * records in the Window by {@link OneInputWindowContext#getAllRecords()}.
     */
    void onTrigger(
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception;

    /**
     * Callback when a window is about to be cleaned up. It is the time to deletes any state in the
     * {@code windowContext} when the Window expires (the event time or processing time passes its
     * {@code maxTimestamp} + {@code allowedLateness}).
     */
    default void onClear(
            Collector<OUT> output,
            PartitionedContext<OUT> ctx,
            OneInputWindowContext<IN> windowContext)
            throws Exception {}

    /** This method will be invoked when a record is received after the window has been cleaned. */
    default void onLateRecord(IN record, Collector<OUT> output, PartitionedContext<OUT> ctx)
            throws Exception {}
}
```

There are four key methods in the WindowProcessFunction. The names and meanings of these methods are as follows:
1. `onRecord`: `onRecord` indicates that the window has received a record.
2. `onTrigger`: `onTrigger` indicates that the window has been triggered.
3. `onClear`: `onClear` indicates that the window has been cleared.
4. `onLateRecord`: `onLateRecord` indicates that the window has received record after the window is cleared.

There are some important points to consider regarding these methods.
1. Windows can be triggered multiple times. Therefore, `onRecord` may be called after `onTrigger`.
2. GlobalWindow is cleared when the data stream ends, while time/session windows are cleared after the window boundary is reached and the allowedLateness (see [Allowed Lateness](#allowed-lateness)) has elapsed.
3. `onLateRecord` method is not possible to access the window state since the window has been cleared.

Users should implement `WindowProcessFunction` to complete their own calculation logic. 
In this step, they can use [Window State](#declare-and-use-window-state) to store Window-related data and [access the built-in state](#access-built-state-of-window) used to store Window data.

### Declare and Use Window State
In a window, there are two types of states: partitioned state and window state.

1. Partitioned State: We refer to the partition-related state as partitioned state. 

    For NonKeyedStream, this state is shared among a specific task. For KeyedStream, this state is shared among data with the same key. 

    User can declare partitioned state through `ProcessFunction#usesStates` and use partitioned state through `PartitionedContext#getStateManager`. 
It's users' responsibility to clear data in partitioned state that are no longer needed in `WindowProcessFunction#onClear`.
2. Window State: We refer to the window-related state as window state.

    Window state is bound to a specific window. For example, the window state declared and used for the same key in the 10:00-11:00 window is different from that in the 11:00-12:00 window. 

    User can declare window state through `WindowProcessFunction#usesWindowStates` and use window state through `WindowContext#getWindowState`. 
All window state will eventually be cleared by framework, whether or not the user clears it manually in `WindowProcessFunction#onClear`.

### Access Built State of Window
We provide built-in window state for each window to store the input data. Users can access this through `WindowContext#putRecord` and `WindowContext#getAllRecords`. 
This state will be cleared when the window is cleared.

By default, `WindowProcessFunction#onRecord` stores the received data in the window's built-in state by `WindowContext#putRecord`, and users can retrieve all the data within the Window using `WindowContext#getAllRecords` when the window is triggered.

Therefore, when overriding `WindowProcessFunction#onRecord`, users should consider whether they need to write the input data into the built-in state. 
A typical example is if users want to do pre-aggregation, they can declare a Window state, perform aggregation in `WindowProcessFunction#onRecord`, update the aggregated window state, and output the final result in `WindowProcessFunction#onTrigger`. 
Therefore, unnecessary cost of caching all data are eliminated.

## Build a ProcessFunction
After declaring the Window and defining the *WindowProcessFunction*, users should utilize the *BuiltinFuncs.window* method to encapsulate these two components into a *ProcessFunction*, which can then be integrated into the data processing stream. 
An example of this is as follows:

```java
KeyedPartitionStream stream = ...;
OneInputStreamProcessFunction wrappedWindowProcessFunction = BuiltinFuncs.window(windowStrategy, new CustomWindowProcessFunction());
stream.process(wrappedWindowProcessFunction)
      .process(...);
```

By doing so, Flink will automatically manage the state and timers required for caching window data, relieving users of the burden of handling these details on their own.

## Example: Count the sales of each product every hour

The following is an example of how to use Window to count the sales of each product in each hour.

In the following example, we first use the [event time extension]({{< ref "docs/dev/datastream-v2/time-processing/event_timer_service" >}}) to extract the event time from `orderSoucrce`, then declare a one-hour tumbling window and calculate the sales quantity of each product when the window is triggered.

```java
public class CountProductSalesEveryHour {

    public static class Order {
        public long orderId;
        public long productId;
        public long salesQuantity;
        public long orderTime;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // create order source stream
        NonKeyedPartitionStream<Order> orderSource = ...;

        // extract and propagate event time from order
        NonKeyedPartitionStream<Order> orderStream = orderSource.process(
                EventTimeExtension
                        .<Order>newWatermarkGeneratorBuilder(order -> order.orderTime)
                        .periodicWatermark(Duration.ofMillis(200))
                        .buildAsProcessFunction()
        );

        NonKeyedPartitionStream<Tuple2<Long, Long>> productSalesQuantityStream = orderStream
                // key by productId
                .keyBy(order -> order.productId)
                .process(BuiltinFuncs.window(
                                // declare tumbling window with window size 10 seconds
                                WindowStrategy.tumbling(
                                        Duration.ofHours(1),
                                        WindowStrategy.EVENT_TIME),
                                // define window process function to calculate total sales quantity per product per window
                                new CountSalesQuantity()
                        )
                );

        // print result
        productSalesQuantityStream.toSink(new WrappedSink<>(new PrintSink<>()));

        env.execute("CountSalesQuantifyOfEachProductEveryHour");
    }

    public static class CountSalesQuantity implements OneInputWindowStreamProcessFunction<Order, Tuple2<Long, Long>>  {

        @Override
        public void onTrigger(
                Collector<Tuple2<Long, Long>> output,
                PartitionedContext<Tuple2<Long, Long>> ctx,
                OneInputWindowContext<Order> windowContext) throws Exception {
            // get current productId
            long productId = ctx.getStateManager().getCurrentKey();
            // calculate total sales quantity
            long totalSalesQuantity = 0;
            for (Order order : windowContext.getAllRecords()) {
                totalSalesQuantity += order.salesQuantity;
            }
            // emit result
            output.collect(Tuple2.of(productId, totalSalesQuantity));
        }
    }
}
```

In the scenario of this example, users can reduce the cost of Window calculation by performing pre-aggregation in the Window.
We rewrote the `CountSalesQuantity` WindowProcessFunction into the `CountSalesQuantityWithPreAggregation` WindowProcessFunction that can perform pre-aggregation when the input data arrives.

```java

public static class CountSalesQuantityWithPreAggregation implements OneInputWindowStreamProcessFunction<Order, Tuple2<Long, Long>>  {

    private final ValueStateDeclaration<Long> salesQuantityStateDeclaration = 
            StateDeclarations.valueState("totalSalesQuantity", TypeDescriptors.LONG);

    @Override
    public Set<StateDeclaration> useWindowStates() {
        return Set.of(salesQuantityStateDeclaration);
    }

    @Override
    public void onRecord(
            Order record,
            Collector<Tuple2<Long, Long>> output,
            PartitionedContext<Tuple2<Long, Long>> ctx,
            OneInputWindowContext<Order> windowContext) throws Exception {
        // get sales quantity from state
        ValueState<Long> salesQuantityState = windowContext.getWindowState(salesQuantityStateDeclaration).get();
        long salesQuantity = 0;
        if (salesQuantityState.value() != null) {
            salesQuantity = salesQuantityState.value();
        }

        // update sales quantity in state
        salesQuantity += record.salesQuantity;
        salesQuantityState.update(salesQuantity);
    }

    @Override
    public void onTrigger(
            Collector<Tuple2<Long, Long>> output,
            PartitionedContext<Tuple2<Long, Long>> ctx,
            OneInputWindowContext<Order> windowContext) throws Exception {
        // get current productId
        long productId = ctx.getStateManager().getCurrentKey();
        // get sales quantity from state
        ValueState<Long> salesQuantityState = windowContext.getWindowState(salesQuantityStateDeclaration).get();
        long salesQuantity = salesQuantityState.value() == null ? 0 : salesQuantityState.value();
        // emit result
        output.collect(Tuple2.of(productId, salesQuantity));
    }
}
```

In the `CountSalesQuantityWithPreAggregation` function, we first declare a `ValueState` to store the total sales quantity for each product within the window.
When input data arrives, we update this state accordingly, and we output the final result once the window is triggered.
This approach allows us to avoid storing all input data in the window; instead, we only need to maintain a `ValueState` for each product within the window.


{{< top >}}
