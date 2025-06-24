---
title: "Time Attributes"
weight: 3
type: docs
aliases:
  - /dev/table/streaming/time_attributes.html
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

# Time Attributes

Flink can process data based on different notions of time. 

- *Processing time* refers to the machine's system time (also known as epoch time, e.g. Java's `System.currentTimeMillis()`) that is executing the respective operation.
- *Event time* refers to the processing of streaming data based on timestamps that are attached to each row. The timestamps can encode when an event happened.

For more information about time handling in Flink, see the introduction about [event time and watermarks]({{< ref "docs/concepts/time" >}}).

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

To handle out-of-order events and to distinguish between on-time and late events in streaming, Flink needs to know the timestamp for each row, and it also needs regular indications of how far along in event time the processing has progressed so far (via so-called [watermarks]({{< ref "docs/concepts/time" >}})).

Event time attributes can be defined in `CREATE` table DDL or during DataStream-to-Table conversion.

### Defining in DDL

The event time attribute is defined using a `WATERMARK` statement in `CREATE` table DDL. A watermark statement defines a watermark generation expression on an existing event time field, which marks the event time field as the event time attribute. Please see [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table) for more information about watermark statement and watermark strategies.

Flink supports defining event time attribute on TIMESTAMP column and TIMESTAMP_LTZ column. 
If the timestamp data in the source is represented as year-month-day-hour-minute-second, usually a string value without time-zone information, e.g. `2020-04-15 20:13:40.564`, it's recommended to define the event time attribute as a `TIMESTAMP` column::
```sql

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

```

If the timestamp data in the source is represented as a epoch time, usually a long value, e.g. `1618989564564`, it's recommended to define event time attribute as a `TIMESTAMP_LTZ` column:
```sql

CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  ts BIGINT,
  time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  -- declare time_ltz as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(time_ltz, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(time_ltz, INTERVAL '10' MINUTE);

```

#### Advanced watermark features

In previous versions, many advanced features of watermark (such as watermark alignment) were easy to use through the datastream api, but not so easy to use in sql, so we have extended these features in version 1.18 to enable users to use them in sql as well.

{{< hint warning >}}
**Note:** Only source connectors that implement the `SupportsWatermarkPushDown` interface (e.g. kafka, pulsar) can use these advanced features. If a source does not implement the `SupportsWatermarkPushDown` interface, but the task is configured with these parameters, the task can run normally, but these parameters will not take effect.

These features all can be configured with dynamic table options or the 'OPTIONS' hint, If the user has configured these feature both in the dynamic table options and in the 'OPTIONS' hint, the options in the 'OPTIONS' hint are preferred. If the user uses 'OPTIONS' hint for the same source table in multiple places, the first hint will be used.
{{< /hint >}}

##### I. Configure watermark emit strategy
There are two strategies to emit watermark in flink:

- on-periodic: Emit watermark periodically.
- on-event: Emit watermark per events.

In the DataStream API, the user can choose to emit strategy through the WatermarkGenerator interface ([Writing WatermarkGenerators]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#writing-watermarkgenerators)). For sql tasks, watermark is emitted periodically by default, with a default period of 200ms, which can be changed by the parameter `pipeline.auto-watermark-interval`. If you need to emit watermark per events, you can configure it in the source table as follows:

```sql
-- configure in table options
CREATE TABLE user_actions (
  ...
  user_action_time TIMESTAMP(3),
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  'scan.watermark.emit.strategy'='on-event',
  ...
)
```

Of course, you can also use the `OPTIONS` hint:
```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.emit.strategy'='on-periodic') */
```

##### II. Configure the idle-timeout of source table

If a split/partition/shard in the source table does not send event data for some time, it means that `WatermarkGenerator` will not get any new data to generate watermark either, we call such data sources as idle inputs or idle sources. In this case, a problem occurs if some other partition is still sending event data, because the downstream operator's watermark is calculated by taking the minimum value of all upstream parallel data sources' watermarks, and since the idle split/partition/shard is not generating a new watermark, the downstream operator's watermark will not change. However, if the idle timeout is configured, the split/partition/shard will be marked as idle if no event data is sent in the timeout, and the downstream will ignore this idle source when calculating new watermark.

A global idle timeout can be defined in sql with the `table.exec.source.idle-timeout` parameter, which will take effect for each source table. However, if you want to set a different idle timeout for each source table, you can configure in the source table by parameter `scan.watermark.idle-timeout` like this:

```sql
-- configure in table options
CREATE TABLE user_actions (
  ...
  user_action_time TIMESTAMP(3),
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  'scan.watermark.idle-timeout'='1min',
  ...
).
```

Or you can use the `OPTIONS` hint:
```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.idle-timeout'='1min') */
```

If the user has configured source idle-timeout both with parameter `table.exec.source.idle-timeout` and parameter `scan.watermark.idle-timeout`, the parameter `scan.watermark.idle-timeout` is preferred.

##### III. watermark alignment
Affected by various factors such as data distribution or machine load, the consumption rate may be different between different splits/partitions/shards of the same data source or different data sources. If there are some stateful operators downstream, these operators may need to cache more data in the state for those that consume faster and wait for those that consume slower, the state may become very large; Inconsistent consumption rates may also cause more serious data disorder, which may affect the computational accuracy of the window. These scenarios can be avoided by using the watermark alignment feature to ensure that the watermark of fast splits/partitions/shards does not increase too fast compared to other splits/partitions/shards. Noted that the watermark alignment feature affects the performance of the task depending on how much the data consumption differs between different sources.

The watermark alignment can be configured in the source table as follows

```sql
-- configure in table options
CREATE TABLE user_actions (
...
user_action_time TIMESTAMP(3),
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
'scan.watermark.alignment.group'='alignment-group-1',
'scan.watermark.alignment.max-drift'='1min',
'scan.watermark.alignment.update-interval'='1s',
...
).
```

Of course, you can also still use the `OPTIONS` hint.

```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.alignment.group'='alignment-group-1', 'scan.watermark.alignment.max-drift'='1min', 'scan. watermark.alignment.update-interval'='1s') */
```

There are three parameters :

- `scan.watermark.alignment.group` configures the alignment group name, data sources in the same group will be aligned
- `scan.watermark.alignment.max-drift` configures the maximum range of deviations from the alignment time allowed for splits/partitions/shards
- `scan.watermark.alignment.update-interval` configure how often the alignment time is calculated, not required, default is 1s

{{< hint warning >}}
**Note:** connectors have to implement watermark alignment of source split in order to use the watermark alignment feature since 1.17 according [FLIP-217](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217%3A+Support+watermark+alignment+of+source+splits). If source connector does not implement FLIP-217, the task will run with an error, user could set `pipeline.watermark-alignment.allow-unaligned-source-splits: true` to disable watermark alignment of source split, and watermark alignment will be working properly only when your number of splits equals to the parallelism of the source operator.
{{< /hint >}}


### During DataStream-to-Table Conversion

When converting a `DataStream` to a table, an event time attribute can be defined with the `.rowtime` property during schema definition. [Timestamps and watermarks]({{< ref "docs/concepts/time" >}}) must have already been assigned in the `DataStream` being converted. During the conversion, Flink always derives rowtime attribute as TIMESTAMP WITHOUT TIME ZONE, because DataStream doesn't have time zone notion, and treats all event time values as in UTC.

There are two ways of defining the time attribute when converting a `DataStream` into a `Table`. Depending on whether the specified `.rowtime` field name exists in the schema of the `DataStream`, the timestamp is either (1) appended as a new column, or it
(2) replaces an existing column.

In either case, the event time timestamp field will hold the value of the `DataStream` event time timestamp.

{{< tabs "5c2b6c52-59ae-4da5-94ec-1dfa18eab4e7" >}}
{{< tab "Java" >}}
```java

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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

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
```
{{< /tab >}}
{{< tab "Python" >}}
```python

# Option 1:

# extract timestamp and assign watermarks based on knowledge of the stream
stream = input_stream.assign_timestamps_and_watermarks(...)

table = t_env.from_data_stream(stream, col('user_name'), col('data'), col('user_action_time').rowtime)

# Option 2:

# extract timestamp from first field, and assign watermarks based on knowledge of the stream
stream = input_stream.assign_timestamps_and_watermarks(...)

# the first field has been used for timestamp extraction, and is no longer necessary
# replace first field with a logical event time attribute
table = t_env.from_data_stream(stream, col("user_action_time").rowtime, col('user_name'), col('data'))

# Usage:

table.window(Tumble.over(lit(10).minutes).on(col("user_action_time")).alias("userActionWindow"))
```
{{< /tab >}}
{{< /tabs >}}


Processing Time
---------------

Processing time allows a table program to produce results based on the time of the local machine. It is the simplest notion of time, but it will generate non-deterministic results. Processing time does not require timestamp extraction or watermark generation.

There are two ways to define a processing time attribute.

### Defining in DDL

The processing time attribute is defined as a computed column in `CREATE` table DDL using the system `PROCTIME()` function, the function return type is TIMESTAMP_LTZ. Please see [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table) for more information about computed column.

```sql

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

```

### During DataStream-to-Table Conversion

The processing time attribute is defined with the `.proctime` property during schema definition. The time attribute must only extend the physical schema by an additional logical field. Thus, it is only definable at the end of the schema definition.

{{< tabs "8578fd8f-2b8c-43dc-ba11-a83f505ce7cf" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, String>> stream = ...;

// declare an additional logical field as a processing time attribute
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").proctime());

WindowedTable windowedTable = table.window(
        Tumble.over(lit(10).minutes())
            .on($("user_action_time"))
            .as("userActionWindow"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val stream: DataStream[(String, String)] = ...

// declare an additional logical field as a processing time attribute
val table = tEnv.fromDataStream(stream, $"UserActionTimestamp", $"user_name", $"data", $"user_action_time".proctime)

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
stream = ...

# declare an additional logical field as a processing time attribute
table = t_env.from_data_stream(stream, col("UserActionTimestamp"), col("user_name"), col("data"), col("user_action_time").proctime)

windowed_table = table.window(Tumble.over(lit(10).minutes).on(col("user_action_time")).alias("userActionWindow"))
```
{{< /tab >}}
{{< /tabs >}}
