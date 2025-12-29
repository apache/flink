---
title: "时间属性"
weight: 3
type: docs
aliases:
  - /zh/dev/table/streaming/time_attributes.html
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

# 时间属性

Flink 可以基于几种不同的 *时间* 概念来处理数据。

- *处理时间* 指的是执行具体操作时的机器时间（大家熟知的绝对时间, 例如 Java的 `System.currentTimeMillis()`) ）
- *事件时间* 指的是数据本身携带的时间。这个时间是在事件产生时的时间。
- *摄入时间* 指的是数据进入 Flink 的时间；在系统内部，会把它当做事件时间来处理。

对于时间相关的更多信息，可以参考 [事件时间和Watermark]({{< ref "docs/concepts/time" >}})。

本页面说明了如何在 Flink Table API & SQL 里面定义时间以及相关的操作。

时间属性介绍
-------------------------------

像窗口（在 [Table API]({{< ref "docs/dev/table/tableApi" >}}#group-windows) 和 [SQL]({{< ref "docs/dev/table/sql/queries/window-agg" >}}) ）这种基于时间的操作，需要有时间信息。因此，Table API 中的表就需要提供*逻辑时间属性*来表示时间，以及支持时间相关的操作。

每种类型的表都可以有时间属性，可以在用CREATE TABLE DDL创建表的时候指定、也可以在 `DataStream` 中指定、也可以在定义 `TableSource` 时指定。一旦时间属性定义好，它就可以像普通列一样使用，也可以在时间相关的操作中使用。

只要时间属性没有被修改，而是简单地从一个表传递到另一个表，它就仍然是一个有效的时间属性。时间属性可以像普通的时间戳的列一样被使用和计算。一旦时间属性被用在了计算中，它就会被物化，进而变成一个普通的时间戳。普通的时间戳是无法跟 Flink 的时间以及watermark等一起使用的，所以普通的时间戳就无法用在时间相关的操作中。

Table API 程序需要在 streaming environment 中指定时间属性：

{{< tabs "bc88319c-8c8f-4f9d-8972-a2d6efe478d7" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default

// 或者:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) // default

// 或者:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)  # default

# 或者:
# env.set_stream_time_characteristic(TimeCharacteristic.IngestionTime)
# env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
```
{{< /tab >}}
{{< /tabs >}}

<a name="processing-time"></a>

处理时间
---------------

处理时间是基于机器的本地时间来处理数据，它是最简单的一种时间概念，但是它不能提供确定性。它既不需要从数据里获取时间，也不需要生成 watermark。

共有三种方法可以定义处理时间。

### 在创建表的 DDL 中定义

处理时间属性可以在创建表的 DDL 中用计算列的方式定义，用 `PROCTIME()` 就可以定义处理时间，函数 `PROCTIME()` 的返回类型是 TIMESTAMP_LTZ 。关于计算列，更多信息可以参考：[CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table) 

```sql

CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time AS PROCTIME() -- 声明一个额外的列作为处理时间属性
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);

```


### 在 DataStream 到 Table 转换时定义

处理时间属性可以在 schema 定义的时候用 `.proctime` 后缀来定义。时间属性一定不能定义在一个已有字段上，所以它只能定义在 schema 定义的最后。

{{< tabs "ec1506ee-adf2-4668-a6f0-7944a5b24bc2" >}}
{{< tab "Java" >}}
```java
DataStream<Tuple2<String, String>> stream = ...;

// 声明一个额外的字段作为时间属性字段
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

// 声明一个额外的字段作为时间属性字段
val table = tEnv.fromDataStream(stream, $"UserActionTimestamp", $"user_name", $"data", $"user_action_time".proctime)

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```
{{< /tab >}}
{{< /tabs >}}

### 使用 TableSource 定义

处理时间属性可以在实现了 `DefinedProctimeAttribute` 的 `TableSource` 中定义。逻辑的时间属性会放在 `TableSource` 已有物理字段的最后

{{< tabs "4b87d0d7-487e-4b11-a01a-4812f6b71e2d" >}}
{{< tab "Java" >}}
```java
// 定义一个由处理时间属性的 table source
public class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name" , "data"};
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
		// 这个名字的列会被追加到最后，作为第三列
		return "user_action_time";
	}
}

// register table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble
	    .over(lit(10).minutes())
	    .on($("user_action_time"))
	    .as("userActionWindow"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 定义一个由处理时间属性的 table source
class UserActionSource extends StreamTableSource[Row] with DefinedProctimeAttribute {

	override def getReturnType = {
		val names = Array[String]("user_name" , "data")
		val types = Array[TypeInformation[_]](Types.STRING, Types.STRING)
		Types.ROW(names, types)
	}

	override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
		// create stream
		val stream = ...
		stream
	}

	override def getProctimeAttribute = {
		// 这个名字的列会被追加到最后，作为第三列
		"user_action_time"
	}
}

// register table source
tEnv.registerTableSource("user_actions", new UserActionSource)

val windowedTable = tEnv
	.from("user_actions")
	.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```
{{< /tab >}}
{{< /tabs >}}

事件时间
----------

事件时间允许程序按照数据中包含的时间来处理，这样可以在有乱序或者晚到的数据的情况下产生一致的处理结果。它可以保证从外部存储读取数据后产生可以复现（replayable）的结果。

除此之外，事件时间可以让程序在流式和批式作业中使用同样的语法。在流式程序中的事件时间属性，在批式程序中就是一个正常的时间字段。

为了能够处理乱序的事件，并且区分正常到达和晚到的事件，Flink 需要从事件中获取事件时间并且产生 watermark（[watermarks]({{< ref "docs/concepts/time" >}})）。

事件时间属性也有类似于处理时间的三种定义方式：在DDL中定义、在 DataStream 到 Table 转换时定义、用 TableSource 定义。

### 在 DDL 中定义

事件时间属性可以用 WATERMARK 语句在 CREATE TABLE DDL 中进行定义。WATERMARK 语句在一个已有字段上定义一个 watermark 生成表达式，同时标记这个已有字段为时间属性字段。更多信息可以参考：[CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table)

Flink 支持和在 TIMESTAMP 列和 TIMESTAMP_LTZ 列上定义事件时间。如果源数据中的时间戳数据表示为年-月-日-时-分-秒，则通常为不带时区信息的字符串值，例如 `2020-04-15 20:13:40.564`，建议将事件时间属性定义在 `TIMESTAMP` 列上:
```sql

CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- 声明 user_action_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);

```

源数据中的时间戳数据表示为一个纪元 (epoch) 时间，通常是一个 long 值，例如 `1618989564564`，建议将事件时间属性定义在 `TIMESTAMP_LTZ` 列上：
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

#### 在SQL中使用watermark进阶功能
之前的版本中，Watermark的很多进阶功能（比如watermark对齐）通过datastream api很容易使用，但想在sql中使用却不太容易，所以我们在1.18版本对这些功能进行了扩展，使用户也能够在sql中用到这些功能。

{{< hint warning >}}
**Note:** 只有实现了`SupportsWatermarkPushDown`接口的源连接器(source connector)（比如kafka、pulsar）才可以使用这些进阶功能。如果一个源连接器(source connector)没有实现`SupportsWatermarkPushDown`接口，但是任务配置了这些参数，任务可以正常运行，但是这些参数也不会生效。
这些进阶的功能都可以使用dynamic table options或`OPTIONS` hint进行配置，如果用户同时使用dynamic table options或`OPTIONS` hint进行配置，那么`OPTIONS` hint配置的值会优先生效。如果用户在sql的多个地方使用了`OPTIONS` hint，那么SQL中出现的第一个hint会优先生效
{{< /hint >}}

##### I. 配置Watermark发射方式
Flink中watermark有两种发射方式：

- on-periodic: 周期性发射
- on-event: 每条事件数据发射一次watermark

在DataStream API，用户可以通过WatermarkGenerator接口来决定选择哪种方式([自定义 WatermarkGenerator]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#自定义-watermarkgenerator))，而对于sql任务，watermark默认是周期性发射的方式，默认周期是200ms，这个周期可以通过参数`pipeline.auto-watermark-interval`来进行修改。如果需要每条事件数据都发射一次watermark，可以在source表中进行如下配置：

```sql
-- configure in table options
CREATE TABLE user_actions (
  ...
  user_action_time TIMESTAMP(3),
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  'scan.watermark.emit.strategy'='on-event',
  ...
);
```

当然，也可以使用`OPTIONS` hint来配置：
```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.emit.strategy'='on-periodic') */
```

##### II. 配置数据源(Source)的空闲超时时间

如果数据源中的某一个分区/分片在一段时间内未发送事件数据，则意味着`WatermarkGenerator`也不会获得任何新数据去生成watermark，我们称这类数据源为空闲输入或空闲源。在这种情况下，如果其他某些分区仍然在发送事件数据就会出现问题，因为下游算子watermark的计算方式是取所有上游并行数据源watermark的最小值，由于空闲的分片/分区没有计算新的watermark，任务的watermark将不会发生变化，如果配置了数据源的空闲超时时间，一个分区/分片在超时时间没有发送事件数据就会被标记为空闲，下游计算新的watermark的时候将会忽略这个空闲sourse，从而让watermark继续推进。

在sql中可以通过`table.exec.source.idle-timeout`参数来定义一个全局的超时时间，每个数据源都会生效。但如果你想为每个数据源设置不同的空闲超时时间，可以直接在源表中进行设置：

```sql
-- configure in table options
CREATE TABLE user_actions (
  ...
  user_action_time TIMESTAMP(3),
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  'scan.watermark.idle-timeout'='1min',
  ...
);
```

或者也可以使用`OPTIONS` hint：
```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.idle-timeout'='1min') */
```
如果用户同时使用`table.exec.source.idle-timeout`参数和`scan.watermark.idle-timeout`参数配置了数据源的空闲超时时间，`scan.watermark.idle-timeout`参数会优先生效。

##### III. Watermark对齐
受到数据分布或者机器负载等各种因素的影响，同一个数据源的不同分区/分片之间可能出现消费速度不一样的情况，不同数据源之间的消费速度也可能不一样，假如下游有一些有状态的算子，这些算子可能需要在状态中缓存更多那些消费更快的数据，等待那些消费慢的数据，状态可能会变得很大；消费速率不一致也可能造成更严重的数据乱序情况，可能会影响窗口的计算准确度。这些场景都可以使用watermark对齐功能，确保源表的某个分片/分块/分区的watermark不会比其他分片/分块/分区增加太快，从而避免上述问题，需要注意的是watermark对齐功能会影响任务的性能，这取决于不同源表之间数据消费差别有多大。

在sql任务中可以在源表中配置watermark对齐：

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
);
```

当然，你也依然可以用`OPTIONS` hint:

```sql
-- use 'OPTIONS' hint
select ... from source_table /*+ OPTIONS('scan.watermark.alignment.group'='alignment-group-1', 'scan.watermark.alignment.max-drift'='1min', 'scan.watermark.alignment.update-interval'='1s') */
```

这里有三个参数：

- `scan.watermark.alignment.group`配置对齐组名称，在同一个组的数据源将会对齐
- `scan.watermark.alignment.max-drift`配置分片/分块/分区允许偏离对齐时间的最大范围
- `scan.watermark.alignment.update-interval`配置计算对齐时间的频率，非必需，默认是1s

{{< hint warning >}}
**Note:** 如果源连接器(source connector)未实现[FLIP-217](https://cwiki.apache.org/confluence/display/FLINK/FLIP-217%3A+Support+watermark+alignment+of+source+splits)，并且使用了watermark对齐的功能，那么任务运行会抛出异常，用户可以设置`pipeline.watermark-alignment.allow-unaligned-source-splits`为`true`来禁用源分片的WaterMark对齐功能，此时，只有当分片数量等于源并行度的时候，watermark对齐功能才能正常工作。
{{< /hint >}}

### 在 DataStream 到 Table 转换时定义

事件时间属性可以用 `.rowtime` 后缀在定义 `DataStream` schema 的时候来定义。[时间戳和 watermark]({{< ref "docs/concepts/time" >}}) 在这之前一定是在 `DataStream` 上已经定义好了。 
在从 DataStream 转换到 Table 时，由于 `DataStream` 没有时区概念，因此 Flink 总是将 `rowtime` 属性解析成 `TIMESTAMP WITHOUT TIME ZONE` 类型，并且将所有事件时间的值都视为 UTC 时区的值。

在从 `DataStream` 到 `Table` 转换时定义事件时间属性有两种方式。取决于用 `.rowtime` 后缀修饰的字段名字是否是已有字段，事件时间字段可以是：

- 在 schema 的结尾追加一个新的字段
- 替换一个已经存在的字段。

不管在哪种情况下，事件时间字段都表示 `DataStream` 中定义的事件的时间戳。

{{< tabs "4ddfd970-e823-48c2-ae38-4e110ada0f9a" >}}
{{< tab "Java" >}}
```java

// Option 1:

// 基于 stream 中的事件产生时间戳和 watermark
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 声明一个额外的逻辑字段作为事件时间属性
Table table = tEnv.fromDataStream(stream, $("user_name"), $("data"), $("user_action_time").rowtime());


// Option 2:

// 从第一个字段获取事件时间，并且产生 watermark
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 第一个字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
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

// 基于 stream 中的事件产生时间戳和 watermark
val stream: DataStream[(String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// 声明一个额外的逻辑字段作为事件时间属性
val table = tEnv.fromDataStream(stream, $"user_name", $"data", $"user_action_time".rowtime)


// Option 2:

// 从第一个字段获取事件时间，并且产生 watermark
val stream: DataStream[(Long, String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// 第一个字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
val table = tEnv.fromDataStream(stream, $"user_action_time".rowtime, $"user_name", $"data")

// Usage:

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```
{{< /tab >}}
{{< /tabs >}}

### 使用 TableSource 定义

事件时间属性可以在实现了 `DefinedRowTimeAttributes` 的 `TableSource` 中定义。`getRowtimeAttributeDescriptors()` 方法返回 `RowtimeAttributeDescriptor` 的列表，包含了描述事件时间属性的字段名字、如何计算事件时间、以及 watermark 生成策略等信息。

同时需要确保 `getDataStream` 返回的 `DataStream` 已经定义好了时间属性。
只有在定义了 `StreamRecordTimestamp` 时间戳分配器的时候，才认为 `DataStream` 是有时间戳信息的。 
只有定义了 `PreserveWatermarks` watermark 生成策略的 `DataStream` 的 watermark 才会被保留。反之，则只有时间字段的值是生效的。

{{< tabs "bfc26d42-c602-402e-98fc-fe6eb787d283" >}}
{{< tab "Java" >}}
```java
// 定义一个有事件时间属性的 table source
public class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"user_name", "data", "user_action_time"};
		TypeInformation[] types =
		    new TypeInformation[] {Types.STRING(), Types.STRING(), Types.LONG()};
		return Types.ROW(names, types);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// 构造 DataStream
		// ...
		// 基于 "user_action_time" 定义 watermark
		DataStream<Row> stream = inputStream.assignTimestampsAndWatermarks(...);
		return stream;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		// 标记 "user_action_time" 字段是事件时间字段
		// 给 "user_action_time" 构造一个时间属性描述符
		RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
			"user_action_time",
			new ExistingField("user_action_time"),
			new AscendingTimestamps());
		List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
		return listRowtimeAttrDescr;
	}
}

// register the table source
tEnv.registerTableSource("user_actions", new UserActionSource());

WindowedTable windowedTable = tEnv
	.from("user_actions")
	.window(Tumble.over(lit(10).minutes()).on($("user_action_time")).as("userActionWindow"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 定义一个有事件时间属性的 table source
class UserActionSource extends StreamTableSource[Row] with DefinedRowtimeAttributes {

	override def getReturnType = {
		val names = Array[String]("user_name" , "data", "user_action_time")
		val types = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.LONG)
		Types.ROW(names, types)
	}

	override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
		// 构造 DataStream
		// ...
		// 基于 "user_action_time" 定义 watermark
		val stream = inputStream.assignTimestampsAndWatermarks(...)
		stream
	}

	override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
		// 标记 "user_action_time" 字段是事件时间字段
		// 给 "user_action_time" 构造一个时间属性描述符
		val rowtimeAttrDescr = new RowtimeAttributeDescriptor(
			"user_action_time",
			new ExistingField("user_action_time"),
			new AscendingTimestamps)
		val listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr)
		listRowtimeAttrDescr
	}
}

// register the table source
tEnv.registerTableSource("user_actions", new UserActionSource)

val windowedTable = tEnv
	.from("user_actions")
	.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
