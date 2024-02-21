---
title: "Generating Watermarks"
weight: 2
type: docs
aliases:
  - /dev/event_timestamps_watermarks.html
  - /apis/streaming/event_time.html
  - /apis/streaming/event_timestamps_watermarks.html
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

# Generating Watermarks

In this section you will learn about the APIs that Flink provides for working
with **event time** timestamps and watermarks.  For an introduction to *event
time*, *processing time*, and *ingestion time*, please refer to the
[introduction to event time]({{< ref "docs/concepts/time" >}}).

## Introduction to Watermark Strategies

In order to work with *event time*, Flink needs to know the events
*timestamps*, meaning each element in the stream needs to have its event
timestamp *assigned*. This is usually done by accessing/extracting the
timestamp from some field in the element by using a `TimestampAssigner`.

Timestamp assignment goes hand-in-hand with generating watermarks, which tell
the system about progress in event time. You can configure this by specifying a
`WatermarkGenerator`.

The Flink API expects a `WatermarkStrategy` that contains both a
`TimestampAssigner` and `WatermarkGenerator`.  A number of common strategies
are available out of the box as static methods on `WatermarkStrategy`, but
users can also build their own strategies when required. 

Here is the interface for completeness' sake:

```java
public interface WatermarkStrategy<T> 
    extends TimestampAssignerSupplier<T>,
            WatermarkGeneratorSupplier<T>{

    /**
     * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this
     * strategy.
     */
    @Override
    TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context);

    /**
     * Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
     */
    @Override
    WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);
}
```

As mentioned, you usually don't implement this interface yourself but use the
static helper methods on `WatermarkStrategy` for common watermark strategies or
to bundle together a custom `TimestampAssigner` with a `WatermarkGenerator`.
For example, to use bounded-out-of-orderness watermarks and a lambda function as a
timestamp assigner you use this:

{{< tabs "ff4b2dcc-9487-4ba8-98d6-5b112050801d" >}}
{{< tab "Java" >}}
```java
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withTimestampAssigner((event, timestamp) -> event.f0);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
  .withTimestampAssigner(new SerializableTimestampAssigner[(Long, String)] {
    override def extractTimestamp(element: (Long, String), recordTimestamp: Long): Long = element._1
  })
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class FirstElementTimestampAssigner(TimestampAssigner):
   
    def extract_timestamp(self, value, record_timestamp):
        return value[0]


WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(20)) \
    .with_timestamp_assigner(FirstElementTimestampAssigner())
```
{{< /tab >}}
{{< /tabs >}}

Specifying a `TimestampAssigner` is optional and in most cases you don't
actually want to specify one. For example, when using Kafka or Kinesis you
would get timestamps directly from the Kafka/Kinesis records.

We will look at the `WatermarkGenerator` interface later in [Writing
WatermarkGenerators](#writing-watermarkgenerators).

{{< hint warning >}}
**Attention**: Both timestamps and watermarks
are specified as milliseconds since the Java epoch of 1970-01-01T00:00:00Z.
{{< /hint >}}

## Using Watermark Strategies

There are two places in Flink applications where a `WatermarkStrategy` can be
used: 1) directly on sources and 2) after non-source operation.

The first option is preferable, because it allows sources to exploit knowledge
about shards/partitions/splits in the watermarking logic. Sources can usually
then track watermarks at a finer level and the overall watermark produced by a
source will be more accurate. Specifying a `WatermarkStrategy` directly on the
source usually means you have to use a source specific interface/ Refer to
[Watermark Strategies and the Kafka
Connector](#watermark-strategies-and-the-kafka-connector) for how this works on
a Kafka Connector and for more details about how per-partition watermarking
works there.

The second option (setting a `WatermarkStrategy` after arbitrary operations)
should only be used if you cannot set a strategy directly on the source:

{{< tabs "b9fa8499-faf5-462f-b9b9-22832aeb360f" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<MyEvent> stream = env.readFile(
        myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
        FilePathFilter.createDefaultFilter(), typeInfo);

DataStream<MyEvent> withTimestampsAndWatermarks = stream
        .filter( event -> event.severity() == WARNING )
        .assignTimestampsAndWatermarks(<watermark strategy>);

withTimestampsAndWatermarks
        .keyBy( (event) -> event.getGroup() )
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce( (a, b) -> a.add(b) )
        .addSink(...);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

val stream: DataStream[MyEvent] = env.readFile(
         myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
         FilePathFilter.createDefaultFilter())

val withTimestampsAndWatermarks: DataStream[MyEvent] = stream
        .filter( _.severity == WARNING )
        .assignTimestampsAndWatermarks(<watermark strategy>)

withTimestampsAndWatermarks
        .keyBy( _.getGroup )
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce( (a, b) => a.add(b) )
        .addSink(...)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

# currently read_file is not supported in PyFlink
stream = env \
    .read_text_file(my_file_path, charset) \
    .map(lambda s: MyEvent.from_string(s))

with_timestamp_and_watermarks = stream \
    .filter(lambda e: e.severity() == WARNING) \
    .assign_timestamp_and_watermarks(<watermark strategy>)

with_timestamp_and_watermarks \
    .key_by(lambda e: e.get_group()) \
    .window(TumblingEventTimeWindows.of(Time.seconds(10))) \
    .reduce(lambda a, b: a.add(b)) \
    .add_sink(...)
```
{{< /tab >}}
{{< /tabs >}}

Using a `WatermarkStrategy` this way takes a stream and produce a new stream
with timestamped elements and watermarks. If the original stream had timestamps
and/or watermarks already, the timestamp assigner overwrites them.

## Dealing With Idle Sources

If one of the input splits/partitions/shards does not carry events for a while
this means that the `WatermarkGenerator` also does not get any new information
on which to base a watermark. We call this an *idle input* or an *idle source*.
This is a problem because it can happen that some of your partitions do still
carry events. In that case, the watermark will be held back, because it is
computed as the minimum over all the different parallel watermarks.

To deal with this, you can use a `WatermarkStrategy` that will detect idleness
and mark an input as idle. `WatermarkStrategy` provides a convenience helper
for this:

{{< tabs "dbdf635d-705e-446b-aba9-65c374b571d1" >}}
{{< tab "Java" >}}
```java
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withIdleness(Duration.ofMinutes(1));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
  .withIdleness(Duration.ofMinutes(1))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(20)) \
    .with_idleness(Duration.of_minutes(1))
```
{{< /tab >}}
{{< /tabs >}}

## Watermark alignment

In the previous paragraph we discussed a situation when splits/partitions/shards or sources are idle
and can stall increasing watermarks. On the other side of the spectrum, a split/partition/shard or
source may process records very fast and in turn increase its watermark relatively faster than the
others. This on its own is not a problem per se. However, for downstream operators that are using
watermarks to emit some data it can actually become a problem. 

In this case, contrary to idle sources, the watermark of such downstream operator (like windowed
joins on aggregations) can progress. However, such operator might need to buffer excessive amount of
data coming from the fast inputs, as the minimal watermark from all of its inputs is held back by
the lagging input. All records emitted by the fast input will hence have to be buffered
in the said downstream operator state, which can lead into uncontrollable growth of the operator's
state.

In order to address the issue, you can enable watermark alignment, which will make sure no
sources/splits/shards/partitions increase their watermarks too far ahead of the rest. You can enable
alignment for every source separately:


{{< tabs >}}
{{< tab "Java" >}}
```java
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
WatermarkStrategy
  .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
  .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(20)) \
    .with_watermark_alignment("alignment-group-1", Duration.of_seconds(20), Duration.of_seconds(1))
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
**Note:** You can enable watermark alignment only for [FLIP-27]({{< ref "docs/dev/datastream/sources" >}}) 
sources. It does not work for legacy or if applied after the source via 
[DataStream#assignTimestampsAndWatermarks](#using-watermark-strategies).
{{< /hint >}}

When enabling the alignment, you need to tell Flink, which group should the source belong. You do
that by providing a label (e.g. `alignment-group-1`) which bind together all sources that share it.
Moreover, you have to tell the maximal drift from the current minimal watermarks across all sources
belonging to that group. The third parameter describes how often the current maximal watermark
should be updated. The downside of frequent updates is that there will be more RPC messages
travelling between TMs and the JM.

In order to achieve the alignment Flink will pause consuming from the source/task, which generated
watermark that is too far into the future. In the meantime it will continue reading records from
other sources/tasks which can move the combined watermark forward and that way unblock the faster
one.

{{< hint warning >}}
**Note:** As of Flink 1.17, split level watermark alignment is supported by the FLIP-27 source framework. 
Source connectors have to implement an interface to resume and pause splits so that splits/partitions/shards 
can be aligned in the same task. More detail on the pause and resume interfaces can found in the [Source API]({{< ref "docs/dev/datastream/sources" >}}#split-level-watermark-alignment).

If you are upgrading from a Flink version between 1.15.x and 1.16.x inclusive, you can disable split level alignment by setting
`pipeline.watermark-alignment.allow-unaligned-source-splits` to true. Moreover, you can tell if your source supports split level alignment
by checking if it throws an `UnsupportedOperationException` at runtime or by reading the javadocs. In this case, it would be desirable to 
to disable split level watermark alignment to avoid fatal exceptions.

When setting the flag to true, watermark alignment will be only working properly when the number of splits/shards/partitions is equal to the
parallelism of the source operator. This results in every subtask being assigned a single unit of work. On the other hand, if there are two Kafka partitions, which produce watermarks at different paces and
get assigned to the same task, then watermarks might not behave as expected. Fortunately, even in the worst case, the basic alignment should not perform worse than having no alignment at all.

Furthermore, Flink also supports aligning across tasks of the same sources and/or different
sources, which is useful when you have two different sources (e.g. Kafka and File) that produce watermarks at different speeds.
{{< /hint >}}

## Writing WatermarkGenerators

A `TimestampAssigner` is a simple function that extracts a field from an event, we therefore don't need to look at them in detail. A `WatermarkGenerator`, on the other hand, is a bit more complicated to write and we will look at how you can do that in the next two sections. This is the `WatermarkGenerator` interface:

```java
/**
 * The {@code WatermarkGenerator} generates watermarks either based on events or
 * periodically (in a fixed interval).
 *
 * <p><b>Note:</b> This WatermarkGenerator subsumes the previous distinction between the
 * {@code AssignerWithPunctuatedWatermarks} and the {@code AssignerWithPeriodicWatermarks}.
 */
@Public
public interface WatermarkGenerator<T> {

    /**
     * Called for every event, allows the watermark generator to examine 
     * and remember the event timestamps, or to emit a watermark based on
     * the event itself.
     */
    void onEvent(T event, long eventTimestamp, WatermarkOutput output);

    /**
     * Called periodically, and might emit a new watermark, or not.
     *
     * <p>The interval in which this method is called and Watermarks 
     * are generated depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
     */
    void onPeriodicEmit(WatermarkOutput output);
}
```

There are two different styles of watermark generation: *periodic* and
*punctuated*.

A periodic generator usually observes the incoming events via `onEvent()`
and then emits a watermark when the framework calls `onPeriodicEmit()`.

A puncutated generator will look at events in `onEvent()` and wait for special
*marker events* or *punctuations* that carry watermark information in the
stream. When it sees one of these events it emits a watermark immediately.
Usually, punctuated generators don't emit a watermark from `onPeriodicEmit()`.

We will look at how to implement generators for each style next.

### Writing a Periodic WatermarkGenerator

A periodic generator observes stream events and generates
watermarks periodically (possibly depending on the stream elements, or purely
based on processing time).

The interval (every *n* milliseconds) in which the watermark will be generated
is defined via `ExecutionConfig.setAutoWatermarkInterval(...)`. The
generators's `onPeriodicEmit()` method will be called each time, and a new
watermark will be emitted if the returned watermark is non-null and larger than
the previous watermark.

Here we show two simple examples of watermark generators that use periodic
watermark generation. Note that Flink ships with
`BoundedOutOfOrdernessWatermarks`, which is a `WatermarkGenerator` that works
similarly to the `BoundedOutOfOrdernessGenerator` shown below. You can read
about using that [here]({{< ref "docs/dev/datastream/event-time/built_in" >}}).

{{< tabs "0e9b4b32-60dd-4626-a987-63966ccae260" >}}
{{< tab "Java" >}}
```java
/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<MyEvent> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // emit the watermark as current highest timestamp minus the out-of-orderness bound
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }

}

/**
 * This generator generates watermarks that are lagging behind processing time 
 * by a fixed amount. It assumes that elements arrive in Flink after a bounded delay.
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<MyEvent> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
        // don't need to do anything because we work on processing time
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
class BoundedOutOfOrdernessGenerator extends WatermarkGenerator[MyEvent] {

    val maxOutOfOrderness = 3500L // 3.5 seconds

    var currentMaxTimestamp: Long = _

    override def onEvent(element: MyEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
        currentMaxTimestamp = max(eventTimestamp, currentMaxTimestamp)
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
        // emit the watermark as current highest timestamp minus the out-of-orderness bound
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
    }
}

/**
 * This generator generates watermarks that are lagging behind processing 
 * time by a fixed amount. It assumes that elements arrive in Flink after 
 * a bounded delay.
 */
class TimeLagWatermarkGenerator extends WatermarkGenerator[MyEvent] {

    val maxTimeLag = 5000L // 5 seconds

    override def onEvent(element: MyEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
        // don't need to do anything because we work on processing time
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag))
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

### Writing a Punctuated WatermarkGenerator

A punctuated watermark generator will observe the stream of
events and emit a watermark whenever it sees a special element that carries
watermark information.

This is how you can implement a punctuated generator that emits a watermark
whenever an event indicates that it carries a certain marker:

{{< tabs "e2ea017d-ce00-4473-b941-4027ed4ce32d" >}}
{{< tab "Java" >}}
```java
public class PunctuatedAssigner implements WatermarkGenerator<MyEvent> {

    @Override
    public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
        if (event.hasWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // don't need to do anything because we emit in reaction to events above
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class PunctuatedAssigner extends WatermarkGenerator[MyEvent] {

    override def onEvent(element: MyEvent, eventTimestamp: Long): Unit = {
        if (event.hasWatermarkMarker()) {
            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()))
        }
    }

    override def onPeriodicEmit(): Unit = {
        // don't need to do anything because we emit in reaction to events above
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
**Note:** It is possible to
generate a watermark on every single event. However, because each watermark
causes some computation downstream, an excessive number of watermarks degrades
performance.
{{< /hint >}}

## Watermark Strategies and the Kafka Connector

When using [Apache Kafka]({{< ref "docs/connectors/datastream/kafka" >}}) as a data source, each Kafka
partition may have a simple event time pattern (ascending timestamps or bounded
out-of-orderness). However, when consuming streams from Kafka, multiple
partitions often get consumed in parallel, interleaving the events from the
partitions and destroying the per-partition patterns (this is inherent in how
Kafka's consumer clients work).

In that case, you can use Flink's Kafka-partition-aware watermark generation.
Using that feature, watermarks are generated inside the Kafka consumer, per
Kafka partition, and the per-partition watermarks are merged in the same way as
watermarks are merged on stream shuffles.

For example, if event timestamps are strictly ascending per Kafka partition,
generating per-partition watermarks with the [ascending timestamps watermark
generator]({{< ref "docs/dev/datastream/event-time/built_in">}}#monotonously-increasing-timestamps)
will result in perfect overall watermarks. Note, that we don't provide a
`TimestampAssigner` in the example, the timestamps of the Kafka records
themselves will be used instead.

The illustrations below show how to use the per-Kafka-partition watermark
generation, and how watermarks propagate through the streaming dataflow in that
case.

{{< tabs "8c79e7ba-e4c4-4892-9aab-d2e958b75c0e" >}}
{{< tab "Java" >}}
```java
KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
    .setBootstrapServers(brokers)
    .setTopics("my-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> stream = env.fromSource(
    kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "mySource");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
    .setBootstrapServers("brokers")
    .setTopics("my-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema)
    .build()

val stream = env.fromSource(
    kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)), "mySource")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
kafka_source = KafkaSource.builder()
    .set_bootstrap_servers(brokers)
    .set_topics("my-topic")
    .set_group_id("my-group")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()

stream = env.from_source(
    source=kafka_source,
    watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(20)),
    source_name="kafka_source")
```
{{< /tab >}}
{{< /tabs >}}

{{< img src="/fig/parallel_kafka_watermarks.svg" alt="Generating Watermarks with awareness for Kafka-partitions" class="center" width="80%" >}}

## How Operators Process Watermarks

As a general rule, operators are required to completely process a given
watermark before forwarding it downstream. For example, `WindowOperator` will
first evaluate all windows that should be fired, and only after producing all of
the output triggered by the watermark will the watermark itself be sent
downstream. In other words, all elements produced due to occurrence of a
watermark will be emitted before the watermark.

The same rule applies to `TwoInputStreamOperator`. However, in this case the
current watermark of the operator is defined as the minimum of both of its
inputs.

The details of this behavior are defined by the implementations of the
`OneInputStreamOperator#processWatermark`,
`TwoInputStreamOperator#processWatermark1` and
`TwoInputStreamOperator#processWatermark2` methods.

## The Deprecated AssignerWithPeriodicWatermarks and AssignerWithPunctuatedWatermarks

Prior to introducing the current abstraction of `WatermarkStrategy`,
`TimestampAssigner`, and `WatermarkGenerator`, Flink used
`AssignerWithPeriodicWatermarks` and `AssignerWithPunctuatedWatermarks`. You will
still see them in the API but it is recommended to use the new interfaces
because they offer a clearer separation of concerns and also unify periodic and
punctuated styles of watermark generation.

{{< top >}}
