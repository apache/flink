---
title: "Generating Timestamps / Watermarks"

sub-nav-group: streaming
sub-nav-pos: 1
sub-nav-parent: eventtime
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


This section is relevant for program running on **Event Time**. For an introduction to *Event Time*,
*Processing Time*, and *Ingestion Time*, please refer to the [event time introduction]({{ site.baseurl }}/apis/streaming/event_time.html)

To work with *Event Time*, streaming programs need to set the *time characteristic* accordingly.

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
</div>


## Assigning Timestamps

In order to work with *Event Time*, Flink needs to know the events' *timestamps*, meaning each element in the
stream needs to get its event timestamp *assigned*. That happens usually by accessing/extracting the
timestamp from some field in the element.

Timestamp assignment goes hand-in-hand with generating watermarks, which tell the system about 
the progress in event time.

There are two ways to assign timestamps and generate Watermarks:

  1. Directly in the data stream source
  2. Via a *TimestampAssigner / WatermarkGenerator*


### Source Functions with Timestamps and Watermarks

Stream sources can also directly assign timestamps to the elements they produce and emit Watermarks. In that case,
no Timestamp Assigner is needed.

To assign a timestamp to an element in the soruce directly, the source must use the `collectWithTimestamp(...)`
method on the `SourceContext`. To generate Watermarks, the source must call the `emitWatermark(Watermark)` function.

Below is a simple example of a source *(non-checkpointed)* that assigns timestamps and generates Watermarks
depending on special events:

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

*Note:* If the streaming program uses a TimestampAssigner on a stream where elements have a timestamp already,
those timestamps will be overwritten by the TimestampAssigner. Similarly, Watermarks will be overwritten as well.


### Timestamp Assigners / Watermark Generators

Timestamp Assigners take a stream and produce a new stream with timestamped elements and watermarks. If the
original stream had timestamps or watermarks already, the timestamp assigner overwrites those.

The timestamp assigners occur usually direct after the data source, but it is not strictly required to. A
common pattern is for example to parse (*MapFunction*) and filter (*FilterFunction*) before the timestamp assigner.
In any case, the timestamp assigner needs to occur before the first operation on event time
(such as the first window operation).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<MyEvent> stream = env.addSource(new FlinkKafkaConsumer09<MyEvent>(topic, schema, props));

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

val stream: DataStream[MyEvent] = env.addSource(new FlinkKafkaConsumer09[MyEvent](topic, schema, props))

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


#### **With Ascending timestamps**

The simplest case for generating watermarks is the case where timestamps within one source occur in ascending order.
In that case, the current timestamp can always act as a watermark, because no lower timestamps will occur any more.

Note that it is only necessary that timestamps are ascending *per parallel data source instance*. For example, if
in a specific setup one Kafka partition is read by one parallel data source instance, then it is only necessary that
timestamps are ascending within each Kafka partition. Flink's Watermark merging mechanism will generate correct
whenever parallel streams are shuffled, unioned, connected, or merged.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<MyEvent> stream = ...

DataStream<MyEvent> withTimestampsAndWatermarks = 
    stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyEvent>() {

        @Override
        public long extractAscendingTimestamp(MyEvent element) {
            return element.getCreationTime();
        }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[MyEvent] = ...

val withTimestampsAndWatermarks = stream.assignAscendingTimestamps( _.getCreationTime )
{% endhighlight %}
</div>
</div>


*Note:* The generation of watermarks on ascending timestamps is a special case of the periodic watermark
generation described in the next section.


#### **With Periodic Watermarks**

The `AssignerWithPeriodicWatermarks` assigns timestamps and generate watermarks periodically (possibly depending
the stream elements, or purely based on processing time).

The interval (every *n* milliseconds) in which the watermark will be generated is defined via
`ExecutionConfig.setAutoWatermarkInterval(...)`. Each time, the assigner's `getCurrentWatermark()` method will be
called, and a new Watermark will be emitted, if the returned Watermark is non-null and larger than the previous
Watermark.

Two simple examples of timestamp assigners with periodic watermark generation are below.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
 * This generator generates watermarks assuming that elements come out of order to a certain degree only.
 * The latest elements for a certain timestamp t will arrive at most n milliseconds after the earliest
 * elements for timestamp t.
 */
public class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks<MyEvent> {

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
 * This generator generates watermarks that are lagging behind processing time by a certain amount.
 * It assumes that elements arrive in Flink after at most a certain time.
 */
public class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks<MyEvent> {

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
 * This generator generates watermarks assuming that elements come out of order to a certain degree only.
 * The latest elements for a certain timestamp t will arrive at most n milliseconds after the earliest
 * elements for timestamp t.
 */
class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[MyEvent] {

    val maxOutOfOrderness = 3500L; // 3.5 seconds

    var currentMaxTimestamp: Long;

    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {
        val timestamp = element.getCreationTime() 
        currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
        timestamp;
    }

    override def getCurrentWatermark(): Watermark = {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

/**
 * This generator generates watermarks that are lagging behind processing time by a certain amount.
 * It assumes that elements arrive in Flink after at most a certain time.
 */
class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[MyEvent] {

    val maxTimeLag = 5000L; // 5 seconds

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

To generate Watermarks whenever a certain event indicates that a new watermark can be generated, use the
`AssignerWithPunctuatedWatermarks`. For this class, Flink will first call the `extractTimestamp(...)` method
to assign the element a timestamp, and then immediately call for that element the
`checkAndGetNextWatermark(...)` method.

The `checkAndGetNextWatermark(...)` method gets the timestamp that was assigned in the `extractTimestamp(...)`
method, and can decide whether it wants to generate a Watermark. Whenever the `checkAndGetNextWatermark(...)`
method returns a non-null Watermark, and that Watermark is larger than the latest previous Watermark, that
new Watermark will be emitted.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks<MyEvent> {

	@Override
	public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
		return element.getCreationTime();
	}

	@Override
	public Watermark checkAndGetNextWatermark(MyEvent lastElement, long extractedTimestamp) {
		return element.hasWatermarkMarker() ? new Watermark(extractedTimestamp) : null;
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
		if (element.hasWatermarkMarker()) new Watermark(extractedTimestamp) else null
	}
}
{% endhighlight %}
</div>
</div>

*Note:* It is possible to generate a watermark on every single event. However, because each watermark causes some
computation downstream, an excessive number of watermarks slows down performance.

