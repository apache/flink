---
title: "Process Function"
weight: 4 
type: docs
aliases:
  - /dev/stream/operators/process_function.html
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

# Process Function

## The ProcessFunction

The `ProcessFunction` is a low-level stream processing operation, giving access to the basic building blocks of
all (acyclic) streaming applications:

  - events (stream elements)
  - state (fault-tolerant, consistent, only on keyed stream)
  - timers (event time and processing time, only on keyed stream)

The `ProcessFunction` can be thought of as a `FlatMapFunction` with access to keyed state and timers. It handles events
by being invoked for each event received in the input stream(s).

For fault-tolerant state, the `ProcessFunction` gives access to Flink's [keyed state]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}), accessible via the
`RuntimeContext`, similar to the way other stateful functions can access keyed state.

The timers allow applications to react to changes in processing time and in [event time]({{< ref "docs/concepts/time" >}}).
Every call to the function `processElement(...)` gets a `Context` object which gives access to the element's
event time timestamp, and to the *TimerService*. The `TimerService` can be used to register callbacks for future
event-/processing-time instants. With event-time timers, the `onTimer(...)` method is called when the current watermark is advanced up to or beyond the timestamp of the timer, while with processing-time timers, `onTimer(...)` is called when wall clock time reaches the specified time. During that call, all states are again scoped to the key with which the timer was created, allowing
timers to manipulate keyed state.

{{< hint info >}}
If you want to access keyed state and timers you have
to apply the `ProcessFunction` on a keyed stream:
{{< /hint >}}

```java
stream.keyBy(...).process(new MyProcessFunction());
```

## Low-level Joins

To realize low-level operations on two inputs, applications can use `CoProcessFunction` or `KeyedCoProcessFunction`. This
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

In the following example a `KeyedProcessFunction` maintains counts per key, and emits a key/count pair whenever a minute passes (in event time) without an update for that key:

  - The count, key, and last-modification-timestamp are stored in a `ValueState`, which is implicitly scoped by key.
  - For each record, the `KeyedProcessFunction` increments the counter and sets the last-modification timestamp
  - The function also schedules a callback one minute into the future (in event time)
  - Upon each callback, it checks the callback's event time timestamp against the last-modification time of the stored count
    and emits the key/count if they match (i.e., no further update occurred during that minute)

{{< hint info >}}
This simple example could have been implemented with
session windows. We use `KeyedProcessFunction` here to illustrate the basic pattern it provides.
{{< /hint >}}

{{< tabs "6c8c009c-4c12-4338-9eeb-3be83cfa9e37" >}}
{{< tab "Java" >}}

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.util.Collector;


// the source data stream
DataStream<Tuple2<String, String>> stream = ...;

// apply the process function onto a keyed stream
DataStream<Tuple2<String, Long>> result = stream
    .keyBy(value -> value.f0)
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
public class CountWithTimeoutFunction 
        extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(OpenContext openContext) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            Tuple2<String, String> value, 
            Context ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

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
    public void onTimer(
            long timestamp, 
            OnTimerContext ctx, 
            Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
import datetime

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment


class CountWithTimeoutFunction(KeyedProcessFunction):

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "my_state", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        current = self.state.value()
        if current is None:
            current = Row(value.f1, 0, 0)

        # update the state's count
        current[1] += 1

        # set the state's timestamp to the record's assigned event time timestamp
        current[2] = ctx.timestamp()

        # write the state back
        self.state.update(current)

        # schedule the next timer 60 seconds from the current event time
        ctx.timer_service().register_event_time_timer(current[2] + 60000)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        # get the state for the key that scheduled the timer
        result = self.state.value()

        # check if this is an outdated timer or the latest timer
        if timestamp == result[2] + 60000:
            # emit the state on timeout
            yield result[0], result[1]


class MyTimestampAssigner(TimestampAssigner):

    def __init__(self):
        self.epoch = datetime.datetime.utcfromtimestamp(0)

    def extract_timestamp(self, value, record_timestamp) -> int:
        return int((value[0] - self.epoch).total_seconds() * 1000)


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql("""
            CREATE TABLE my_source (
              a TIMESTAMP(3),
              b VARCHAR,
              c VARCHAR
            ) WITH (
              'connector' = 'datagen',
              'rows-per-second' = '10'
            )
        """)

    stream = t_env.to_append_stream(
        t_env.from_path('my_source'),
        Types.ROW([Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING()]))
    watermarked_stream = stream.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps()
                         .with_timestamp_assigner(MyTimestampAssigner()))

    # apply the process function onto a keyed stream
    result = watermarked_stream.key_by(lambda value: value[1]) \
                               .process(CountWithTimeoutFunction()) \
                               .print()
    env.execute()
```
{{< /tab >}}
{{< /tabs >}}


{{< hint warning >}}
Before Flink 1.4.0, when called from a processing-time timer, the `ProcessFunction.onTimer()` method sets
the current processing time as event-time timestamp. This behavior is very subtle and might not be noticed by users. Well, it's
harmful because processing-time timestamps are indeterministic and not aligned with watermarks. Besides, user-implemented logic
depends on this wrong timestamp highly likely is unintendedly faulty. So we've decided to fix it. Upon upgrading to 1.4.0, Flink jobs
that are using this incorrect event-time timestamp will fail, and users should adapt their jobs to the correct logic.
{{< /hint >}}

## The KeyedProcessFunction

`KeyedProcessFunction`, as an extension of `ProcessFunction`, gives access to the key of timers in its `onTimer(...)`
method.

{{< tabs "f8b6791f-023f-4e56-a6e4-8541dd0b3e1b" >}}
{{< tab "Java" >}}
```java
@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
    K key = ctx.getCurrentKey();
    // ...
}

```
{{< /tab >}}
{{< tab "Python" >}}
```python
def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
    key = ctx.get_current_key()
    # ...
```
{{< /tab >}}
{{< /tabs >}}

## Timers

Both types of timers (processing-time and event-time) are internally maintained by the `TimerService` and enqueued for execution.

The `TimerService` deduplicates timers per key and timestamp, i.e., there is at most one timer per key and timestamp. If multiple timers are registered for the same timestamp, the `onTimer()` method will be called just once.

Flink synchronizes invocations of `onTimer()` and `processElement()`. Hence, users do not have to worry about concurrent modification of state.

### Fault Tolerance

Timers are fault tolerant and checkpointed along with the state of the application. 
In case of a failure recovery or when starting an application from a savepoint, the timers are restored.

{{< hint info >}}
Checkpointed processing-time timers that were supposed to fire before their restoration, will fire immediately.
This might happen when an application recovers from a failure or when it is started from a savepoint.
{{< /hint >}}

{{< hint info >}}
Timers are always asynchronously checkpointed, except for the combination of RocksDB backend / with incremental snapshots / with heap-based timers (will be resolved with `FLINK-10026`).
Notice that large numbers of timers can increase the checkpointing time because timers are part of the checkpointed state. See the "Timer Coalescing" section for advice on how to reduce the number of timers.
{{< /hint >}}

### Timer Coalescing

Since Flink maintains only one timer per key and timestamp, you can reduce the number of timers by reducing the timer resolution to coalesce them.

For a timer resolution of 1 second (event or processing time), you
can round down the target time to full seconds. Timers will fire at most 1 second earlier but not later than requested with millisecond accuracy. 
As a result, there are at most one timer per key and second.

{{< tabs "aa23eeb6-d15f-44f2-85ab-d130a4202d57" >}}
{{< tab "Java" >}}
```java
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
coalesced_time = ((ctx.timestamp() + timeout) // 1000) * 1000
ctx.timer_service().register_processing_time_timer(coalesced_time)
```
{{< /tab >}}
{{< /tabs >}}

Since event-time timers only fire with watermarks coming in, you may also schedule and coalesce
these timers with the next watermark by using the current one:

{{< tabs "ef74a1da-c4cd-4fab-8035-d29ffd7039d4" >}}
{{< tab "Java" >}}
```java
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
coalesced_time = ctx.timer_service().current_watermark() + 1
ctx.timer_service().register_event_time_timer(coalesced_time)
```
{{< /tab >}}
{{< /tabs >}}

Timers can also be stopped and removed as follows:

Stopping a processing-time timer:

{{< tabs "5d0d1344-6f51-44f8-b500-ebe863cedba4" >}}
{{< tab "Java" >}}
```java
long timestampOfTimerToStop = ...;
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
timestamp_of_timer_to_stop = ...
ctx.timer_service().delete_processing_time_timer(timestamp_of_timer_to_stop)
```
{{< /tab >}}
{{< /tabs >}}

Stopping an event-time timer:

{{< tabs "581e5996-503c-452e-8b2a-a4daeaf4ac88" >}}
{{< tab "Java" >}}
```java
long timestampOfTimerToStop = ...;
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
timestamp_of_timer_to_stop = ...
ctx.timer_service().delete_event_time_timer(timestamp_of_timer_to_stop)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
Stopping a timer has no effect if no such timer with the given timestamp is registered.
{{< /hint >}}

{{< top >}}
