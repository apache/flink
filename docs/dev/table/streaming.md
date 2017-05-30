---
title: "Streaming Concepts"
nav-parent_id: tableapi
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

**TODO: has to be completely written**

* This will be replaced by the TOC
{:toc}

Dynamic table
-------------

* Stream -> Table
* Table -> Stream
* update changes / retraction

{% top %}

Time Attributes
---------------

Flink supports different notions of *time* in streaming programs.

- *Processing time* refers to the system time of the machine (also known as "wall-clock time") that is executing the respective operation.
- *Event time* is the time that each individual event occurred on its producing device.
- *Ingestion time* is the time that events enter Flink, internally, it is treated similar to event time.

For more information about time handling in Flink, see the introduction about [Event Time and Watermarks]({{ site.baseurl }}/dev/event_time.html).

Table programs assume that a corresponding time characteristic has been specified for the streaming environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default

// alternatively:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) //default

// alternatively:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

Time-based operations such as [windows]({{ site.baseurl }}/dev/table/tableApi.html) in both the Table API and SQL require information about the notion of time and its origin. Therefore, tables can offer *logical time attributes* for indicating time and accessing corresponding timestamps in table programs.

Time attributes can be part of every table schema. They are defined when creating a table from a `DataStream` or pre-defined when using a `TableSource`. Once a time attribute is defined at the beginning, it can be referenced as field and used in time-based operations.

As long as a time attribute is not modified and simply forwarded from one part of the query to another, it remains a valid time attribute. Time attributes behave like regular timestamps and can be accessed for calculations. If a time attribute is used in a calculation, it will be materialized and becomes a regular timestamp. Regular timestamps do not cooperate with Flink's time and watermarking system and can thus not be used for time-based operations anymore.

### Processing time

Processing time allows a table program to produce results based on the time of the local machine. It is the simplest notion of time but does not provide determinism. It does neither require timestamp extraction nor watermark generation.

There are two ways to define a processing time attribute.

#### During DataStream-to-Table Conversion

The processing time attribute is defined with the `.proctime` property during schema definition. The time attribute must only extend the physical schema by an additional logical field. Thus, it can only be defined at the end of the schema definition.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, String>> stream = ...;

// declare an additional logical field as processing time attribute
Table table = tEnv.fromDataStream(stream, "Username, Data, UserActionTime.proctime");

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[(String, String)] = ...

// declare an additional logical field as processing time attribute
val table = tEnv.fromDataStream(stream, 'UserActionTimestamp, 'Username, 'Data, 'UserActionTime.proctime)

val windowedTable = table.window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)
{% endhighlight %}
</div>
</div>

#### Using a TableSource

The processing time attribute is defined by a `TableSource` that implements the `DefinedProctimeAttribute` interface. The logical time attribute is appended to the physical schema defined by the return type of the `TableSource`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// define table source with processing attribute
public class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"Username" , "Data"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// create stream 
		DataStream<Row> stream = ...;
		return stream;
	}

	@Override
	public String getProctimeAttribute() {
		// field with this name will be appended as a third field 
		return "UserActionTime";
	}
}

// register table source
tEnv.registerTableSource("UserActions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.scan("UserActions")
	.window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// define table source with processing attribute
class UserActionSource extends StreamTableSource[Row] with DefinedProctimeAttribute {

	override def getReturnType = {
		val names = Array[String]("Username" , "Data")
		val types = Array[TypeInformation[_]](Types.STRING, Types.STRING)
		Types.ROW(names, types)
	}

	override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
		// create stream
		val stream = ...
		stream
	}

	override def getProctimeAttribute = {
		// field with this name will be appended as a third field 
		"UserActionTime"
	}
}

// register table source
tEnv.registerTableSource("UserActions", new UserActionSource)

val windowedTable = tEnv
	.scan("UserActions")
	.window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)
{% endhighlight %}
</div>
</div>

### Event time

Event time allows a table program to produce results based on the time that is contained in every record. This allows for consistent results even in case of out-of-order events or late events. It also ensures replayable results of table program when reading records from persistent storage. 

Additionally, event time allows for unified syntax for table programs in both a batch and streaming environment. A time attribute in streaming can be a regular field of a record in a batch environment.

In order to handle out-of-order events and distinguish between on-time and late events in streaming, Flink needs to extract timestamps from events and make some kind of progress in time (so-called [watermarks]({{ site.baseurl }}/dev/event_time.html)).

The Table API & SQL assumes that timestamps and watermarks have been generated in the [underlying DataStream API]({{ site.baseurl }}/dev/event_timestamps_watermarks.html) before. Ideally, this happens within a TableSource with knowledge about the incoming data's characteristics and hidden from the API end user.

After timestamp and watermarks are generated, an event time attribute can be defined in two ways:

#### During DataStream-to-Table Conversion

The event time attribute is defined with the `.rowtime` property during schema definition. 

There are two ways of defining the time attribute when converting a `DataStream` into a `Table`:

- Extending the physical schema by an additional logical field
- Replacing a physical field by a logical field (e.g. because it is not needed anymore after timestamp extraction).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// extract timestamp from first field and assign watermarks based on knownledge about stream
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// Option 1:

// the first field has still some value and should be kept
// declare an additional logical field as event time attribute
Table table = tEnv.fromDataStream(stream, "UserActionTimestamp, Username, Data, UserActionTime.rowtime");


// Option 2:

// the first field has been used for timestamp extraction and is not necessary anymore
// replace first field as logical event time attribute
Table table = tEnv.fromDataStream(stream, "UserActionTime.rowtime, Username, Data");

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// extract timestamp from first field and assign watermarks based on knownledge about stream
val stream: DataStream[(Long, String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// Option 1:

// the first field has still some value and should be kept
// declare an additional logical field as event time attribute
val table = tEnv.fromDataStream(stream, 'UserActionTimestamp, 'Username, 'Data, 'UserActionTime.rowtime)


// Option 2:

// the first field has been used for timestamp extraction and is not necessary anymore
// replace first field as logical event time attribute
val table = tEnv.fromDataStream(stream, 'UserActionTime.rowtime, 'Username, 'Data)

val windowedTable = table.window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)
{% endhighlight %}
</div>
</div>

#### Using a TableSource

The event time attribute is defined by a `TableSource` that implements the `DefinedRowtimeAttribute` interface. The logical time attribute is appended to the physical schema defined by the return type of the `TableSource`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// define table source with rowtime attribute
public class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttribute {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"Username" , "Data"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// create stream 
		// ...
		// extract timestamp and assign watermarks based on knownledge about stream
		DataStream<Row> stream = inputStream.assignTimestampsAndWatermarks(...);
		return stream;
	}

	@Override
	public String getRowtimeAttribute() {
		// field with this name will be appended as a third field 
		return "UserActionTime";
	}
}

// register table source
tEnv.registerTableSource("UserActions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.scan("UserActions")
	.window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// define table source with rowtime attribute
class UserActionSource extends StreamTableSource[Row] with DefinedRowtimeAttribute {

	override def getReturnType = {
		val names = Array[String]("Username" , "Data")
		val types = Array[TypeInformation[_]](Types.STRING, Types.STRING)
		Types.ROW(names, types)
	}

	override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
		// create stream 
		// ...
		// extract timestamp and assign watermarks based on knownledge about stream
		val stream = inputStream.assignTimestampsAndWatermarks(...)
		stream
	}

	override def getRowtimeAttribute = {
		// field with this name will be appended as a third field 
		"UserActionTime"
	}
}

// register table source
tEnv.registerTableSource("UserActions", new UserActionSource)

val windowedTable = tEnv
	.scan("UserActions")
	.window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)
{% endhighlight %}
</div>
</div>

{% top %}

Query Configuration
-------------------

In stream processing, compuations are constantly happening and there are many use cases that require to update previously emitted results. There are many ways in which a query can compute and emit updates. These do not affect the semantics of the query but might lead to approximated results. 

Flink's Table API and SQL interface use a `QueryConfig` to control the computation and emission of results and updates.

### State Retention

{% top %}


