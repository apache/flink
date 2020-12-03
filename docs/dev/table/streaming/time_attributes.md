---
title: "Time Attributes"
nav-parent_id: streaming_tableapi
nav-pos: 2
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

Flink can process data based on different notions of time. 

- *Processing time* refers to the machine's system time (also known as "wall-clock time") that is executing the respective operation.
- *Event time* refers to the processing of streaming data based on timestamps that are attached to each row. The timestamps can encode when an event happened.

For more information about time handling in Flink, see the introduction about [event time and watermarks]({% link dev/event_time.md %}).


* This will be replaced by the TOC
{:toc}

Introduction to Time Attributes
-------------------------------

Time attributes can be part of every table schema.
They are defined when creating a table from a `CREATE TABLE DDL` or a `DataStream`. 
Once a time attribute is defined, it can be referenced as a field and used in time-based operations.
As long as a time attribute is not modified, and is simply forwarded from one part of a query to another, it remains a valid time attribute. 
Time attributes behave like regular timestamps, and are accessible for calculations.
When used in calculations, time attributes are materialized and act as standard timestamps. 
However, ordinary timestamps cannot be used in place of, or be converted to, time attributes.

Event Time
----------

Event time allows a table program to produce results based on timestamps in every record, allowing for consistent results despite out-of-order or late events. It also ensures the replayability of the results of the table program when reading records from persistent storage.

Additionally, event time allows for unified syntax for table programs in both batch and streaming environments. A time attribute in a streaming environment can be a regular column of a row in a batch environment.

To handle out-of-order events and to distinguish between on-time and late events in streaming, Flink needs to know the timestamp for each row, and it also needs regular indications of how far along in event time the processing has progressed so far (via so-called [watermarks]({% link dev/event_time.md %})).

Event time attributes can be defined in `CREATE` table DDL or during DataStream-to-Table conversion.

### Defining in DDL

The event time attribute is defined using a `WATERMARK` statement in `CREATE` table DDL. A watermark statement defines a watermark generation expression on an existing event time field, which marks the event time field as the event time attribute. Please see [CREATE TABLE DDL]({% link dev/table/sql/create.md %}#create-table) for more information about watermark statement and watermark strategies.

{% highlight sql %}

CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);

{% endhighlight %}


### During DataStream-to-Table Conversion

When converting a `DataStream` to a table, an event time attribute can be defined with the `.rowtime` property during schema definition. [Timestamps and watermarks]({% link dev/event_time.md %}) must have already been assigned in the `DataStream` being converted.

There are two ways of defining the time attribute when converting a `DataStream` into a `Table`. Depending on whether the specified `.rowtime` field name exists in the schema of the `DataStream`, the timestamp is either (1) appended as a new column, or it
(2) replaces an existing column.

In either case, the event time timestamp field will hold the value of the `DataStream` event time timestamp.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// Option 1:

// extract timestamp and assign watermarks based on knowledge of the stream
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// declare an additional logical field as an event time attribute
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").rowtime());


// Option 2:

// extract timestamp from first field, and assign watermarks based on knowledge of the stream
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// the first field has been used for timestamp extraction, and is no longer necessary
// replace first field with a logical event time attribute
Table table = tEnv.fromDataStream(stream, $("user_action_time").rowtime(), $("user_name"), $("data"));

// Usage:

WindowedTable windowedTable = table.window(Tumble
       .over(lit(10).minutes())
       .on($("user_action_time"))
       .as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Option 1:

// extract timestamp and assign watermarks based on knowledge of the stream
val stream: DataStream[(String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// declare an additional logical field as an event time attribute
val table = tEnv.fromDataStream(stream, $"user_name", $"data", $"user_action_time".rowtime)


// Option 2:

// extract timestamp from first field, and assign watermarks based on knowledge of the stream
val stream: DataStream[(Long, String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// the first field has been used for timestamp extraction, and is no longer necessary
// replace first field with a logical event time attribute
val table = tEnv.fromDataStream(stream, $"user_action_time".rowtime, $"user_name", $"data")

// Usage:

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
{% endhighlight %}
</div>
</div>


Processing Time
---------------

Processing time allows a table program to produce results based on the time of the local machine. It is the simplest notion of time, but it will generate non-deterministic results. Processing time does not require timestamp extraction or watermark generation.

There are three ways to define a processing time attribute.

### Defining in DDL

The processing time attribute is defined as a computed column in `CREATE` table DDL using the system `PROCTIME()` function. Please see [CREATE TABLE DDL]({% link dev/table/sql/create.md %}#create-table) for more information about computed column.

{% highlight sql %}

CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time AS PROCTIME() -- declare an additional field as a processing time attribute
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);

{% endhighlight %}


### During DataStream-to-Table Conversion

The processing time attribute is defined with the `.proctime` property during schema definition. The time attribute must only extend the physical schema by an additional logical field. Thus, it is only definable at the end of the schema definition.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, String>> stream = ...;

// declare an additional logical field as a processing time attribute
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").proctime());

WindowedTable windowedTable = table.window(
        Tumble.over(lit(10).minutes())
            .on($("user_action_time"))
            .as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[(String, String)] = ...

// declare an additional logical field as a processing time attribute
val table = tEnv.fromDataStream(stream, $"UserActionTimestamp", $"user_name", $"data", $"user_action_time".proctime)

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
{% endhighlight %}
</div>
</div>
