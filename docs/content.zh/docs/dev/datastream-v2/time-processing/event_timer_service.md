---
title: "Event Timer Service"
weight: 9
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

# Event Timer Service

The event timer service is a high-level extension of the Flink DataStream API, provided by Flink. 
It enables users to register timers for executing calculations at specific event time points and helps determine when to trigger windows within the Flink framework.

For a comprehensive explanation of event time, please refer to the section on [Notions of Time: Event Time and Processing Time]({{< ref "docs/concepts/time" >}}#notions-of-time-event-time-and-processing-time).

In this section, we will introduce how to utilize the event timer service within the Flink DataStream API.

We use a special type of [Watermark]({{< ref "docs/dev/datastream-v2/watermark" >}}) to denote the progression of event time in the stream, 
which we refer to as the event time watermark. Additionally, to dealing idle inputs or sources, we implement another type of 
[Watermark]({{< ref "docs/dev/datastream-v2/watermark" >}}) to indicate that the input or source 
is idle. 
This is known as the idle status watermark. For more information, please refer to [Dealing With Idle Inputs / Sources](#dealing-with-idle-inputs--sources). 
In this document, we collectively refer to the event time watermark and the idle status watermark as event-time related watermarks.

The core of the event timer service lies in generating and propagating event-time related watermarks through the streams. 
To achieve this, we need to address two aspects:
1. How to generate event-time related watermarks.
2. How to handle event-time related watermarks.

The following will introduce these two aspects.

## Generate Event-Time related Watermarks

In order to work with *event time*, Flink needs to know the events
*timestamps*, meaning each element in the stream needs to have its event
timestamp *assigned*. This is usually done by accessing/extracting the
timestamp from some field in the element.

Once the timestamps have been extracted, Flink generates event time watermarks. 
There are two methods for doing so: one is to use the `EventTimeWatermarkGeneratorBuilder` provided by Flink, 
and the other is to implement a custom `ProcessFunction`.

Users should select one of these two approaches to suit their needs.

### Generate Watermarks by WatermarkGeneratorBuilder

Users can utilize the `EventTimeWatermarkGeneratorBuilder` to generate event-time related watermarks.
There are four aspects of the `EventTimeWatermarkGeneratorBuilder` that users can configure:

1. [Required] `EventTimeExtractor`

   This function instructs Flink on how to extract the event time from each record.

3. [Optional] Input Idle Timeout

   If the input stream remains idle for a specified duration, Flink will ignore this input when 
combining event-time related watermarks to prevent stalling the progression of event time. The default value for this timeout is 0.

    For more information, please refer to the [Dealing With Idle Inputs / Sources](#dealing-with-idle-inputs--sources).

4. [Optional] Out-of-Order Time

    To accommodate the disorder of input records, user can set a maximum out-of-order time for the event time watermark. 
The default value for this setting is also 0.

   For more information, please refer to the [Fixed Amount of Lateness](#fixed-amount-of-lateness).

5. [Optional] Generation Frequency

   Flink offers three scenarios for generating event-time related watermarks:
     - No EventTimeWatermarks are generated and emitted.
     - EventTimeWatermarks are generated and emitted periodically.
     - EventTimeWatermarks are generated and emitted for each event.

   By default, Flink adopts the second approach, which involves periodically generating and emitting 
event-time related watermarks. The interval for this periodic generation is determined by the configuration 
"pipeline.auto-watermark-interval."

After configuring and obtaining an instance of the `EventTimeWatermarkGeneratorBuilder`, 
users can utilize it to build a `ProcessFunction`. This `ProcessFunction` will extract the event time 
from each record and generates the corresponding event-time related watermark.

A typical example of using `EventTimeWatermarkGeneratorBuilder` is shown in below:

```java
NonKeyedPartitionStream stream = ...;

EventTimeWatermarkGeneratorBuilder<POJO> builder = EventTimeExtension
        .newWatermarkGeneratorBuilder(pojo -> pojo.getTimeStamp())  // set event time extractor
        .withIdleness(Duration.ofSeconds(10))           // set input idle timeout
        .withMaxOutOfOrderTime(Duration.ofSeconds(30))  // set max out-of-order time
        .periodicWatermark(Duration.ofMillis(200));      // set periodic watermark generation interval

stream.process(builder.buildAsProcessFunction())
      .process(...);
```

{{< hint warning >}}
**Attention**: Both timestamps and event time watermarks
are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
{{< /hint >}}

#### Fixed Amount of Lateness

There are some scenarios where the maximum lateness that can be
encountered in a stream is known in advance, e.g. when creating a custom source
containing elements with timestamps spread within a fixed period of time for
testing.

Users can deal these cases with `EventTimeWatermarkGeneratorBuilder#withMaxOutOfOrderTime`,
i.e. the maximum amount of time an element is allowed to be late before being ignored when
computing the final result for the given window. Lateness corresponds to the
result of `t - t_w`, where `t` is the (event-time) timestamp of an element, and
`t_w` that of the previous event time watermark.  If `lateness > 0` then the element is
considered late and is, by default, ignored when computing the result of the
job for its corresponding window.

#### Dealing With Idle Inputs / Sources

If one of the input splits/partitions/shards does not carry events for a while
this means that the `EventTimeWatermarkGeneratorBuilder` also does not get any new information
on which to base a watermark. We call this an *idle input* or an *idle source*.
This is a problem because it can happen that some of your partitions do still
carry events. In that case, the event time watermark will be held back, because it is
computed as the minimum over all the different parallel event time watermarks.

To deal with this, you can configure an idleness timeout with
`EventTimeWatermarkGeneratorBuilder#withIdleness` that will detect idleness
and mark an input as idle.

If an input remains idle, the `EventTimeWatermarkGeneratorBuilder` will emit an idle status watermark 
to indicate that the input is inactive. Consequently, downstream `ProcessFunction` instances will 
disregard this input when combining event time watermarks.

### Generate Watermarks by Custom ProcessFunction

Users can create and send event-time related watermarks by customizing the `ProcessFunction`. 
To do this, they need to follow these steps:
1. Declare the `EventTimeWatermarkDeclaration` in the custom `ProcessFunction`, and the `IdleStatusWatermarkDeclaration` if support for input idle is required.
2. Create the event-time related watermark using the `EventTimeWatermarkDeclaration`.
3. (Optional) Create the idle status watermark using the `IdleStatusWatermarkDeclaration`.
4. Send the watermarks through the `WatermarkManager`.

The following shows an example of this approach.

```java
public static class CustomProcessFunction
        implements OneInputStreamProcessFunction<Integer, Integer> {
 
 
    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return Set.of(
                  EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION,
                  EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION
               );
    }
 
    @Override
    public void processRecord(Integer record, Collector<Integer> output, PartitionedContext ctx)
            throws Exception {
        // do something as needed
         
        long eventTime = ...  // get event time from record
        // generate event time watermark and send to downstrea
        LongWatermark eventTimeWatermark = EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(eventTime);
        ctx.getNonPartitionedContext()
                .getWatermarkManager()
                .emitWatermark(eventTimeWatermark);
    }
}
```

{{< hint warning >}}
**Attention**: Both timestamps and event time watermarks
are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
{{< /hint >}}

## Handle Event-Time related Watermarks

As described in [Generate Event-Time related Watermarks](#generate-event-time-related-watermarks),
Flink provides abstractions that enable programmers to assign their own timestamps and emit event-time related watermarks.

Once event-time related watermarks are generated and propagated, Flink can use them to determine the
event time of the program, for example triggering a window when the event time watermark
exceeds the end time of the Window.

In addition, users can use event-time related watermarks to implement their own business logic.
Currently, Flink provide users with two ways to use event-time related watermarks, one is to use the built-in
`EventTimeProcessFunction`, and the other is to use the most basic `ProcessFunction` to
process event-time related watermarks.

It should be noted that the first method allows users to quickly register and unregister
event timers, while the second method requires users to implement this function themselves.
Therefore, when you need to use or perceive event time in your program, we recommend using
the first method because it is simple and efficient.

Each approach is described in detail below, users should select one of these to suit their needs.

### Handle Event-Time related Watermarks by EventTimeProcessFunction

`EventTimeProcessFunction` is provided by Flink and is a wrapper for `ProcessFunction` that needs to use event time.

When utilizing the `EventTimeProcessFunction`, it's important for user's custom `ProcessFunction` 
to implement the `OneInputEventTimeStreamProcessFunction` / `TwoInputBroadcastEventTimeStreamProcessFunction`
/ `TwoInputNonBroadcastEventTimeStreamProcessFunction` / `TwoOutputEventTimeStreamProcessFunction` interface 
instead of the `OneInputStreamProcessFunction` / `TwoInputBroadcastStreamProcessFunction` 
/ `TwoInputNonBroadcastStreamProcessFunction` / `TwoOutputStreamProcessFunction` interface.

Below is the interface of the `EventTimeProcessFunction` and `OneInputEventTimeStreamProcessFunction`:

```java
@Experimental
public interface EventTimeProcessFunction extends ProcessFunction {
    /**
     * Initialize the {@link EventTimeProcessFunction} with an instance of {@link EventTimeManager}.
     * Note that this method should be invoked before the open method.
     */
    void initEventTimeProcessFunction(EventTimeManager eventTimeManager);
}

@Experimental
public interface OneInputEventTimeStreamProcessFunction<IN, OUT>
        extends EventTimeProcessFunction, OneInputStreamProcessFunction<IN, OUT> {

    /**
     * The {@code #onEventTimeWatermark} method signifies that the EventTimeProcessFunction has
     * received an EventTimeWatermark. Other types of watermarks will be processed by the {@code
     * ProcessFunction#onWatermark} method.
     */
    default void onEventTimeWatermark(
            long watermarkTimestamp, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {}

    /**
     * Invoked when an event-time timer fires. Note that it is only used in {@link
     * KeyedPartitionStream}.
     */
    default void onEventTimer(long timestamp, Collector<OUT> output, PartitionedContext<OUT> ctx) {}
}
```

There are three methods that should be noted:
- initEventTimeProcessFunction
    - This method allows the `EventTimeProcessFunction` to obtain an instance of `EventTimeManager`. 
  Users can use this instance to access the current event time and to create or delete event timers as needed. Note that the event timer is only used in Keyed Partition Stream.
  
- onEventTimeWatermark
    - This method signifies that the `EventTimeProcessFunction` has received an event time watermark.
    It is important to note in `EventTimeProcessFunction`, the event time watermarks will be processed
      by `EventTimeProcessFunction#onEventTimeWatermark`, whereas other types of watermarks will be processed by the `EventTimeProcessFunction#onWatermark`.
  
- onEventTimer
    - This callback method is triggered by the event timer. Within this method, users can access
      the key and event time associated with the event timer, perform necessary calculations, and output the results.

Here is an example of how to implement `EventTimeProcessFunction`:

```java
class CustomEventTimeProcessFunction
            implements OneInputEventTimeStreamProcessFunction<InputPojo, OutputPojo> {
    
    private EventTimeManager eventTimeManager;

    @Override
    public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
        // get event time manager instance
        this.eventTimeManager = eventTimeManager;
    }

    @Override
    public void processRecord(
            InputPojo record,
            Collector<OutputPojo> output,
            PartitionedContext<OutputPojo> ctx)
            throws Exception {
        ...
        
        // register event timer
        eventTimeManager.registerTimer(targetTimestamp);
    }

    @Override
    public void onEventTimeWatermark(
            long watermarkTimestamp,
            Collector<OutputPojo> output,
            NonPartitionedContext<OutputPojo> ctx)
            throws Exception {
        // sense event time watermark arrival 
    }

    @Override
    public void onEventTimer(
            long timestamp,
            Collector<OutputPojo> output,
            PartitionedContext<OutputPojo> ctx) {
        // write your event timer callback here
    }
}
```

After implementing the `EventTimeProcessFunction`, users should wrap their custom function using 
`EventTimeUtils#wrapProcessFunction`. This step is essential as it provides the necessary components, 
including an instance of `EventTimeManager`, and declares the built-in state required for timers 
and other functionalities.

Here is an example of how to wrap the `EventTimeProcessFunction`:

```java
NonKeyedPartitionStream stream = ...;
stream.keyBy(x -> x.getKey())
      .process(EventTimeExtension.wrapProcessFunction(new CustomEventTimeProcessFunction()))
      .process(...);
```


### Handle Event-Time related Watermarks by Custom ProcessFunction

Similarly, users can handle event-time related watermarks by implementing `ProcessFunction` instead of `EventTimeProcessFunction`.

In this approach, users must evaluate whether the current watermark received is an event time watermark 
or idle status watermark within the `ProcessFunction#onWatermark` method and execute the appropriate 
processing logic accordingly. 

An example is provided below.

```java
public static class CustomProcessFunction
        implements OneInputStreamProcessFunction<Integer, Integer> {
 
    @Override
    public WatermarkHandlingResult onWatermark(
            Watermark watermark,
            Collector<Integer> output,
            NonPartitionedContext<Integer> ctx) throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark)) {
            // do something as needed
            ...
            return WatermarkHandlingResult.PEEK;
        } else if (EventTimeExtension.isIdleStatusWatermark(watermark)) {
            // do something as needed
            ...
            return WatermarkHandlingResult.PEEK;
        } else {
            // do something as needed
            ...
        }
    }
}
```

It is important to note that when `ProcessFunction#onWatermark` handles event-time related watermarks, 
it should return `WatermarkHandlingResult#PEEK`. This indicates that the Flink framework will choose 
the processing logic based on the watermark definition. 
For event time watermark and idle status watermark, the Flink framework will forward the watermark downstream.

Conversely, if `ProcessFunction#onWatermark` returns `WatermarkHandlingResult#POP`, the watermarks 
will not be sent downstream by the Flink framework. Users should be aware that this may result in 
the loss of the watermark, or they may need to send the watermark manually.

## Example
Here is an example of how to use event timer service in Flink:

```java
NonKeyedPartitionStream<POJO> source = ...;

source.process(
        EventTimeExtension.<POJO>newWatermarkGeneratorBuilder(pojo -> pojo.getTimestamp())
                .periodicWatermark(Duration.ofMillis(200))
                .buildAsProcessFunction()
        )
        .keyBy(pojo -> pojo.getKey())
        .process(EventTimeExtension.wrapProcessFunction(
                new OneInputEventTimeStreamProcessFunction<POJO, String>() {

                    private EventTimeManager eventTimeManager;

                    @Override
                    public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
                        // get event time manager instance
                        this.eventTimeManager = eventTimeManager;
                    }

                    @Override
                    public void processRecord(
                            POJO record,
                            Collector<String> output,
                            PartitionedContext<String> ctx) throws Exception {
                        ...

                        // register event timer
                        eventTimeManager.registerTimer(targetTimestamp);
                    }

                    @Override
                    public void onEventTimer(
                            long timestamp,
                            Collector<String> output,
                            PartitionedContext<String> ctx) {
                        // write your event timer callback here
                    }

                }
            )
        )
        .toSink(...);
```
{{< top >}}
