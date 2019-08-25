---
title: "Generating Timestamps / Watermarks"
nav-parent_id: event_time
nav-pos: 1
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

* toc
{:toc}


This section is relevant for programs running on **event time**. For an introduction to *event time*,
*processing time*, and *ingestion time*, please refer to the [introduction to event time]({{ site.baseurl }}/dev/event_time.html).

To work with *event time*, streaming programs need to set the *time characteristic* accordingly.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

## Assigning Timestamps

In order to work with *event time*, Flink needs to know the events' *timestamps*, meaning each element in the
stream needs to have its event timestamp *assigned*. This is usually done by accessing/extracting the
timestamp from some field in the element.

Timestamp assignment goes hand-in-hand with generating watermarks, which tell the system about
progress in event time.

There are two ways to assign timestamps and generate watermarks:

  1. Directly in the data stream source
  2. Via a timestamp assigner / watermark generator: in Flink, timestamp assigners also define the watermarks to be emitted

<span class="label label-danger">Attention</span> Both timestamps and watermarks are specified as
milliseconds since the Java epoch of 1970-01-01T00:00:00Z.

### Source Functions with Timestamps and Watermarks

Stream sources can directly assign timestamps to the elements they produce, and they can also emit watermarks.
When this is done, no timestamp assigner is needed.
Note that if a timestamp assigner is used, any timestamps and watermarks provided by the source will be overwritten.

To assign a timestamp to an element in the source directly, the source must use the `collectWithTimestamp(...)`
method on the `SourceContext`. To generate watermarks, the source must call the `emitWatermark(Watermark)` function.

Below is a simple example of a *(non-checkpointed)* source that assigns timestamps and generates watermarks:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@Override
public void run(SourceContext<MyType> ctx) throws Exception {
	while (/* condition */) {
		MyType next = getNext();
		ctx.collectWithTimestamp(next, next.getEventTimestamp());

		if (next.hasWatermarkTime()) {
			ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
		}
	}
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
override def run(ctx: SourceContext[MyType]): Unit = {
	while (/* condition */) {
		val next: MyType = getNext()
		ctx.collectWithTimestamp(next, next.eventTimestamp)

		if (next.hasWatermarkTime) {
			ctx.emitWatermark(new Watermark(next.getWatermarkTime))
		}
	}
}
{% endhighlight %}
</div>
</div>


### Timestamp Assigners / Watermark Generators

Timestamp assigners take a stream and produce a new stream with timestamped elements and watermarks. If the
original stream had timestamps and/or watermarks already, the timestamp assigner overwrites them.

Timestamp assigners are usually specified immediately after the data source, but it is not strictly required to do so.
A common pattern, for example, is to parse (*MapFunction*) and filter (*FilterFunction*) before the timestamp assigner.
In any case, the timestamp assigner needs to be specified before the first operation on event time
(such as the first window operation). As a special case, when using Kafka as the source of a streaming job,
Flink allows the specification of a timestamp assigner / watermark emitter inside
the source (or consumer) itself. More information on how to do so can be found in the
[Kafka Connector documentation]({{ site.baseurl }}/dev/connectors/kafka.html).


**NOTE:** The remainder of this section presents the main interfaces a programmer has
to implement in order to create her own timestamp extractors/watermark emitters.
To see the pre-implemented extractors that ship with Flink, please refer to the
[Pre-defined Timestamp Extractors / Watermark Emitters]({{ site.baseurl }}/dev/event_timestamp_extractors.html) page.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<MyEvent> stream = env.readFile(
        myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
        FilePathFilter.createDefaultFilter(), typeInfo);

DataStream<MyEvent> withTimestampsAndWatermarks = stream
        .filter( event -> event.severity() == WARNING )
        .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks());

withTimestampsAndWatermarks
        .keyBy( (event) -> event.getGroup() )
        .timeWindow(Time.seconds(10))
        .reduce( (a, b) -> a.add(b) )
        .addSink(...);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val stream: DataStream[MyEvent] = env.readFile(
         myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
         FilePathFilter.createDefaultFilter())

val withTimestampsAndWatermarks: DataStream[MyEvent] = stream
        .filter( _.severity == WARNING )
        .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks())

withTimestampsAndWatermarks
        .keyBy( _.getGroup )
        .timeWindow(Time.seconds(10))
        .reduce( (a, b) => a.add(b) )
        .addSink(...)
{% endhighlight %}
</div>
</div>


#### **With Periodic Watermarks**

`AssignerWithPeriodicWatermarks` assigns timestamps and generates watermarks periodically (possibly depending
on the stream elements, or purely based on processing time).

The interval (every *n* milliseconds) in which the watermark will be generated is defined via
`ExecutionConfig.setAutoWatermarkInterval(...)`. The assigner's `getCurrentWatermark()` method will be
called each time, and a new watermark will be emitted if the returned watermark is non-null and larger than the previous
watermark.

Here we show two simple examples of timestamp assigners that use periodic watermark generation. Note that Flink ships with a `BoundedOutOfOrdernessTimestampExtractor` similar to the `BoundedOutOfOrdernessGenerator` shown below, which you can read about [here]({{ site.baseurl }}/dev/event_timestamp_extractors.html#assigners-allowing-a-fixed-amount-of-lateness).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
        long timestamp = element.getCreationTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

/**
 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
 * It assumes that elements arrive in Flink after a bounded delay.
 */
public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {

	private final long maxTimeLag = 5000; // 5 seconds

	@Override
	public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
		return element.getCreationTime();
	}

	@Override
	public Watermark getCurrentWatermark() {
		// return the watermark as current time minus the maximum time lag
		return new Watermark(System.currentTimeMillis() - maxTimeLag);
	}
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[MyEvent] {

    val maxOutOfOrderness = 3500L // 3.5 seconds

    var currentMaxTimestamp: Long = _

    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {
        val timestamp = element.getCreationTime()
        currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
        timestamp
    }

    override def getCurrentWatermark(): Watermark = {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }
}

/**
 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
 * It assumes that elements arrive in Flink after a bounded delay.
 */
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[MyEvent] {

    val maxTimeLag = 5000L // 5 seconds

    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {
        element.getCreationTime
    }

    override def getCurrentWatermark(): Watermark = {
        // return the watermark as current time minus the maximum time lag
        new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
}
{% endhighlight %}
</div>
</div>

#### **With Punctuated Watermarks**

To generate watermarks whenever a certain event indicates that a new watermark might be generated, use
`AssignerWithPunctuatedWatermarks`. For this class Flink will first call the `extractTimestamp(...)` method
to assign the element a timestamp, and then immediately call the
`checkAndGetNextWatermark(...)` method on that element.

The `checkAndGetNextWatermark(...)` method is passed the timestamp that was assigned in the `extractTimestamp(...)`
method, and can decide whether it wants to generate a watermark. Whenever the `checkAndGetNextWatermark(...)`
method returns a non-null watermark, and that watermark is larger than the latest previous watermark, that
new watermark will be emitted.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<MyEvent> {

	@Override
	public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
		return element.getCreationTime();
	}

	@Override
	public Watermark checkAndGetNextWatermark(MyEvent lastElement, long extractedTimestamp) {
		return lastElement.hasWatermarkMarker() ? new Watermark(extractedTimestamp) : null;
	}
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[MyEvent] {

	override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {
		element.getCreationTime
	}

	override def checkAndGetNextWatermark(lastElement: MyEvent, extractedTimestamp: Long): Watermark = {
		if (lastElement.hasWatermarkMarker()) new Watermark(extractedTimestamp) else null
	}
}
{% endhighlight %}
</div>
</div>

*Note:* It is possible to generate a watermark on every single event. However, because each watermark causes some
computation downstream, an excessive number of watermarks degrades performance.


## Timestamps per Kafka Partition

When using [Apache Kafka](connectors/kafka.html) as a data source, each Kafka partition may have a simple event time pattern (ascending
timestamps or bounded out-of-orderness). However, when consuming streams from Kafka, multiple partitions often get consumed in parallel,
interleaving the events from the partitions and destroying the per-partition patterns (this is inherent in how Kafka's consumer clients work).

In that case, you can use Flink's Kafka-partition-aware watermark generation. Using that feature, watermarks are generated inside the
Kafka consumer, per Kafka partition, and the per-partition watermarks are merged in the same way as watermarks are merged on stream shuffles.

For example, if event timestamps are strictly ascending per Kafka partition, generating per-partition watermarks with the
[ascending timestamps watermark generator](event_timestamp_extractors.html#assigners-with-ascending-timestamps) will result in perfect overall watermarks.

The illustrations below show how to use the per-Kafka-partition watermark generation, and how watermarks propagate through the
streaming dataflow in that case.


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
FlinkKafkaConsumer09<MyType> kafkaSource = new FlinkKafkaConsumer09<>("myTopic", schema, props);
kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyType>() {

    @Override
    public long extractAscendingTimestamp(MyType element) {
        return element.eventTimestamp();
    }
});

DataStream<MyType> stream = env.addSource(kafkaSource);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val kafkaSource = new FlinkKafkaConsumer09[MyType]("myTopic", schema, props)
kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[MyType] {
    def extractAscendingTimestamp(element: MyType): Long = element.eventTimestamp
})

val stream: DataStream[MyType] = env.addSource(kafkaSource)
{% endhighlight %}
</div>
</div>

<img src="{{ site.baseurl }}/fig/parallel_kafka_watermarks.svg" alt="Generating Watermarks with awareness for Kafka-partitions" class="center" width="80%" />

{% top %}
