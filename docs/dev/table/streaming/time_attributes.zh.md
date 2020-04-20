---
title: "时间属性"
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

Flink 可以基于几种不同的 *时间* 概念来处理数据。

- *处理时间* 指的是执行具体操作时的机器时间（也称作"挂钟时间"）
- *事件时间* 指的是数据本身携带的时间。这个时间是在事件产生时的时间。
- *摄入时间* 指的是数据进入 Flink 的时间；在系统内部，会把它当做事件时间来处理。

对于时间相关的更多信息，可以参考 [事件时间和Watermark]({{ site.baseurl }}/zh/dev/event_time.html)。

本页面说明了如何在 Flink Table API & SQL 里面定义时间以及相关的操作。

* This will be replaced by the TOC
{:toc}

时间属性介绍
-------------------------------

像窗口（在 [Table API]({{ site.baseurl }}/zh/dev/table/tableApi.html#group-windows) 和 [SQL]({{ site.baseurl }}/zh/dev/table/sql/queries.html#group-windows) ）这种基于时间的操作，需要有时间信息。因此，Table API 中的表就需要提供*逻辑时间属性*来表示时间，以及支持时间相关的操作。

每种类型的表都可以有时间属性，可以在用CREATE TABLE DDL创建表的时候指定、也可以在 `DataStream` 中指定、也可以在定义 `TableSource` 时指定。一旦时间属性定义好，它就可以像普通列一样使用，也可以在时间相关的操作中使用。

只要时间属性没有被修改，而是简单地从一个表传递到另一个表，它就仍然是一个有效的时间属性。时间属性可以像普通的时间戳的列一样被使用和计算。一旦时间属性被用在了计算中，它就会被物化，进而变成一个普通的时间戳。普通的时间戳是无法跟 Flink 的时间以及watermark等一起使用的，所以普通的时间戳就无法用在时间相关的操作中。

Table API 程序需要在 streaming environment 中指定时间属性：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // default

// 或者:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment

env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) // default

// 或者:
// env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()

env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)  # default

# 或者:
# env.set_stream_time_characteristic(TimeCharacteristic.IngestionTime)
# env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

处理时间
---------------

处理时间是基于机器的本地时间来处理数据，它是最简单的一种时间概念，但是它不能提供确定性。它既不需要从数据里获取时间，也不需要生成 watermark。

共有三种方法可以定义处理时间。

### 在创建表的 DDL 中定义

处理时间属性可以在创建表的 DDL 中用计算列的方式定义，用 `PROCTIME()` 就可以定义处理时间。关于计算列，更多信息可以参考：[CREATE TABLE DDL]({{ site.baseurl }}/zh/dev/table/sql/create.html#create-table) 

{% highlight sql %}

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

{% endhighlight %}


### 在 DataStream 到 Table 转换时定义

处理时间属性可以在 schema 定义的时候用 `.proctime` 后缀来定义。时间属性一定不能定义在一个已有字段上，所以它只能定义在 schem 定义的最后。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<String, String>> stream = ...;

// 声明一个额外的字段作为时间属性字段
Table table = tEnv.fromDataStream(stream, "user_name, data, user_action_time.proctime");

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[(String, String)] = ...

// 声明一个额外的字段作为时间属性字段
val table = tEnv.fromDataStream(stream, 'UserActionTimestamp, 'user_name, 'data, 'user_action_time.proctime)

val windowedTable = table.window(Tumble over 10.minutes on 'user_action_time as 'userActionWindow)
{% endhighlight %}
</div>
</div>

### 使用 TableSource 定义

处理时间属性可以在实现了 `DefinedProctimeAttribute` 的 `TableSource` 中定义。逻辑的时间属性会放在 `TableSource` 已有物理字段的最后

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
	.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
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
	.window(Tumble over 10.minutes on 'user_action_time as 'userActionWindow)
{% endhighlight %}
</div>
</div>

事件时间
----------

事件时间允许程序按照数据中包含的时间来处理，这样可以在有乱序或者晚到的数据的情况下产生一致的处理结果。它可以保证从外部存储读取数据后产生可以复现（replayable）的结果。

除此之外，事件时间可以让程序在流式和批式作业中使用同样的语法。在流式程序中的事件时间属性，在批式程序中就是一个正常的时间字段。

为了能够处理乱序的事件，并且区分正常到达和晚到的事件，Flink 需要从事件中获取事件时间并且产生 watermark（[watermarks]({{ site.baseurl }}/zh/dev/event_time.html)）。

事件时间属性也有类似于处理时间的三种定义方式：在DDL中定义、在 DataStream 到 Table 转换时定义、用 TableSource 定义。

### 在 DDL 中定义

事件时间属性可以用 WATERMARK 语句在 CREATE TABLE DDL 中进行定义。WATERMARK 语句在一个已有字段上定义一个 watermark 生成表达式，同时标记这个已有字段为时间属性字段。更多信息可以参考：[CREATE TABLE DDL]({{ site.baseurl }}/zh/dev/table/sql/create.html#create-table)

{% highlight sql %}

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

{% endhighlight %}


### 在 DataStream 到 Table 转换时定义

事件时间属性可以用 `.rowtime` 后缀在定义 `DataStream` schema 的时候来定义。[时间戳和 watermark]({{ site.baseurl }}/zh/dev/event_time.html) 在这之前一定是在 `DataStream` 上已经定义好了。

在从 `DataStream` 到 `Table` 转换时定义事件时间属性有两种方式。取决于用 `.rowtime` 后缀修饰的字段名字是否是已有字段，事件时间字段可以是：

- 在 schema 的结尾追加一个新的字段
- 替换一个已经存在的字段。

不管在哪种情况下，事件时间字段都表示 `DataStream` 中定义的事件的时间戳。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// Option 1:

// 基于 stream 中的事件产生时间戳和 watermark
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 声明一个额外的逻辑字段作为事件时间属性
Table table = tEnv.fromDataStream(stream, "user_name, data, user_action_time.rowtime");


// Option 2:

// 从第一个字段获取事件时间，并且产生 watermark
DataStream<Tuple3<Long, String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);

// 第一个字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
Table table = tEnv.fromDataStream(stream, "user_action_time.rowtime, user_name, data");

// Usage:

WindowedTable windowedTable = table.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

// Option 1:

// 基于 stream 中的事件产生时间戳和 watermark
val stream: DataStream[(String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// 声明一个额外的逻辑字段作为事件时间属性
val table = tEnv.fromDataStream(stream, 'user_name, 'data, 'user_action_time.rowtime)


// Option 2:

// 从第一个字段获取事件时间，并且产生 watermark
val stream: DataStream[(Long, String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// 第一个字段已经用作事件时间抽取了，不用再用一个新字段来表示事件时间了
val table = tEnv.fromDataStream(stream, 'user_action_time.rowtime, 'user_name, 'data)

// Usage:

val windowedTable = table.window(Tumble over 10.minutes on 'user_action_time as 'userActionWindow)
{% endhighlight %}
</div>
</div>

### 使用 TableSource 定义

事件时间属性可以在实现了 `DefinedRowTimeAttributes` 的 `TableSource` 中定义。`getRowtimeAttributeDescriptors()` 方法返回 `RowtimeAttributeDescriptor` 的列表，包含了描述事件时间属性的字段名字、如何计算事件时间、以及 watermark 生成策略等信息。

同时需要确保 `getDataStream` 返回的 `DataStream` 已经定义好了时间属性。
只有在定义了 `StreamRecordTimestamp` 时间戳分配器的时候，才认为 `DataStream` 是有时间戳信息的。 
只有定义了 `PreserveWatermarks` watermark 生成策略的 `DataStream` 的 watermark 才会被保留。反之，则只有时间字段的值是生效的。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
	.window(Tumble.over("10.minutes").on("user_action_time").as("userActionWindow"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
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
	.window(Tumble over 10.minutes on 'user_action_time as 'userActionWindow)
{% endhighlight %}
</div>
</div>

{% top %}
