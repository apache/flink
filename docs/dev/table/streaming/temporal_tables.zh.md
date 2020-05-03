---
title: "时态表（Temporal Tables）"
nav-parent_id: streaming_tableapi
nav-pos: 4
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

时态表（Temporal Table）表示一直在变化的表上一个（参数化）视图的概念，该视图返回表在某个特定时间点的内容。

变化的表可以是追踪变化的变化历史记录表（例如：数据库变更日志），也可以是有具体更改的维表（例如：数据库表）。

对于表的历史变化，Flink 可以追踪这些变化，并且允许查询这张表在某个特定时间点的内容。在 Flink 中，这类表由 *时态表函数（Temporal Table Function）* 表示。

对于变化的维表，Flink 允许查询这张表在处理时的内容，在 Flink 中，此类表由 *时态表（Temporal Table）* 表示。

* This will be replaced by the TOC
{:toc}

设计初衷
----------

### 与表的历史变化相关

假设我们有下表 `RatesHistory`。

{% highlight sql %}
SELECT * FROM RatesHistory;

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1
10:45   Euro        116
11:15   Euro        119
11:49   Pounds      108
{% endhighlight %}

`RatesHistory` 表示一个关于日元且不断增长的货币汇率 append-only 表（汇率为1）。例如，`欧元`兑`日元`从 `09:00` 到 `10:45` 的汇率为 `114`。从 `10:45` 到 `11:15` ，汇率为 `116`。

假设我们要输出在 `10:58` 的时间的所有当前汇率，则需要以下 SQL 查询来计算结果表：

{% highlight sql %}
SELECT *
FROM RatesHistory AS r
WHERE r.rowtime = (
  SELECT MAX(rowtime)
  FROM RatesHistory AS r2
  WHERE r2.currency = r.currency
  AND r2.rowtime <= TIME '10:58');
{% endhighlight %}

子查询确定对应货币的最大时间小于或等于所需时间。外部查询列出具有最大时间戳的汇率。 
 
下表显示了这种计算的结果。在我们的示例中，考虑了 `10:45` 时`欧元`的更新，但是在 `10:58` 时，该表的版本中未考虑对 `11:15` 处的`欧元`更新和`英镑`的新值。

{% highlight text %}
rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Yen           1
10:45   Euro        116
{% endhighlight %}

 *时态表* 的概念旨在简化此类查询，加快其执行速度，并减少 Flink 的状态使用率。*时态表* 是 append-only 表上的参数化视图，该视图将 append-only 表的行解释为表的变更日志，并在特定时间点提供该表的版本。将 append-only 表解释为变更日志需要指定主键属性和时间戳属性。主键确定哪些行将被覆盖，时间戳确定行有效的时间。

在上面的示例中，`currency` 是 `RatesHistory` 表的主键，而 `rowtime` 是时间戳属性。

在Flink中, 这由 [*时态表函数*](#temporal-table-function) 表示.

### 与维表变化相关

另一方面，某些用例需要连接变化的维表，该表是外部数据库表。

假设 `LatestRates` 是一个以最新汇率实现的表（例如，存储在其中）。`LatestRates` 是物化的 `RatesHistory` 历史。然后在时间 `10:58` 的 `LatestRates`表的内容将是：

{% highlight text %}
10:58> SELECT * FROM LatestRates;
currency   rate
======== ======
US Dollar   102
Yen           1
Euro        116
{% endhighlight %}

`12:00` 时 `LatestRates` 表的内容将是： 

{% highlight text %}
12:00> SELECT * FROM LatestRates;
currency   rate
======== ======
US Dollar   102
Yen           1
Euro        119
Pounds      108
{% endhighlight %}

在Flink中，这由 [*时态表*](#temporal-table)表示.

<a name="temporal-table-function"></a>

时态表函数
------------------------

为了访问时态表中的数据，必须传递一个[时间属性](time_attributes.html)，该属性确定将要返回的表的版本。
 Flink 使用[表函数]({{ site.baseurl }}/zh/dev/table/functions/udfs.html#table-functions) 的SQL语法提供一种表达它的方法。

定义后，*时态表函数*将使用单个时间参数 timeAttribute 并返回一个行集合。
该集合包含相对于给定时间属性的所有现有主键的行的最新版本。

假设我们基于 `RatesHistory` 表定义了一个时态表函数，我们可以通过以下方式查询该函数 `Rates(timeAttribute)` ： 

{% highlight sql %}
SELECT * FROM Rates('10:15');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1

SELECT * FROM Rates('11:00');

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
10:45   Euro        116
09:00   Yen           1
{% endhighlight %}

对 `Rates（timeAttribute）` 的每个查询都将返回给定 `timeAttribute` 的 `Rates` 状态。

**注意**： 当前 Flink 不支持使用常量时间属性参数直接查询时态表函数。目前，时态表函数只能在 join 中使用。上面的示例用于提供有关函数 `Rates(timeAttribute)` 返回内容的直观信息。

另请参阅有关[用于持续查询的 join ](joins.html)的页面，以获取有关如何与时态表 join 的更多信息。

### 定义时态表函数

以下代码段说明了如何从 append-only 表中创建时态表函数。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Provide a static data set of the rates history table.
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// Create and register an example table using above data set.
// In the real setup, you should replace this with your own table.
DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");

tEnv.createTemporaryView("RatesHistory", ratesHistory);

// Create and register a temporal table function.
// Define "r_proctime" as the time attribute and "r_currency" as the primary key.
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// Get the stream and table environments.
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

// Provide a static data set of the rates history table.
val ratesHistoryData = new mutable.MutableList[(String, Long)]
ratesHistoryData.+=(("US Dollar", 102L))
ratesHistoryData.+=(("Euro", 114L))
ratesHistoryData.+=(("Yen", 1L))
ratesHistoryData.+=(("Euro", 116L))
ratesHistoryData.+=(("Euro", 119L))

// Create and register an example table using above data set.
// In the real setup, you should replace this with your own table.
val ratesHistory = env
  .fromCollection(ratesHistoryData)
  .toTable(tEnv, 'r_currency, 'r_rate, 'r_proctime.proctime)

tEnv.createTemporaryView("RatesHistory", ratesHistory)

// Create and register TemporalTableFunction.
// Define "r_proctime" as the time attribute and "r_currency" as the primary key.
val rates = ratesHistory.createTemporalTableFunction('r_proctime, 'r_currency) // <==== (1)
tEnv.registerFunction("Rates", rates)                                          // <==== (2)
{% endhighlight %}
</div>
</div>

行 `(1)` 创建了一个 `rates` [时态表函数](#temporal-table-function),
这使我们可以在[ Table API ](../tableApi.html#joins)中使用 `rates` 函数。

行 `(2)` 在表环境中注册名称为 `Rates` 的函数，这使我们可以在[ SQL ]({{ site.baseurl }}/zh/dev/table/sql/queries.html#joins)中使用 `Rates` 函数。

<a name="temporal-table"></a>

## 时态表

<span class="label label-danger">注意</span> 仅 Blink planner 支持此功能。

为了访问时态表中的数据，当前必须使用 `LookupableTableSource` 定义一个 `TableSource` 。Flink使用 `FOR SYSTEM_TIME AS OF` 的SQL语法查询时态表，它在SQL:2011中被提出。

假设我们定义了一个时态表 `LatestRates` ，我们可以通过以下方式查询此类表：

{% highlight sql %}
SELECT * FROM LatestRates FOR SYSTEM_TIME AS OF TIME '10:15';

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1

SELECT * FROM LatestRates FOR SYSTEM_TIME AS OF TIME '11:00';

currency   rate
======== ======
US Dollar   102
Euro        116
Yen           1
{% endhighlight %}

**注意**： 当前，Flink 不支持以固定时间直接查询时态表。目前，时态表只能在 join 中使用。上面的示例用于提供有关时态表 `LatestRates` 返回内容的直观信息。

另请参阅有关[用于持续查询的 join ](joins.html)的页面，以获取有关如何与时态表 join 的更多信息。

### 定义时态表


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

// Create an HBaseTableSource as a temporal table which implements LookableTableSource
// In the real setup, you should replace this with your own table.
HBaseTableSource rates = new HBaseTableSource(conf, "Rates");
rates.setRowKey("currency", String.class);   // currency as the primary key
rates.addColumn("fam1", "rate", Double.class);

// register the temporal table into environment, then we can query it in sql
tEnv.registerTableSource("Rates", rates);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// Get the stream and table environments.
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

// Create an HBaseTableSource as a temporal table which implements LookableTableSource
// In the real setup, you should replace this with your own table.
val rates = new HBaseTableSource(conf, "Rates")
rates.setRowKey("currency", String.class)   // currency as the primary key
rates.addColumn("fam1", "rate", Double.class)

// register the temporal table into environment, then we can query it in sql
tEnv.registerTableSource("Rates", rates)
{% endhighlight %}
</div>
</div>

另请参阅有关[如何定义 LookupableTableSource ](../sourceSinks.html#defining-a-tablesource-for-lookups)的页面.

{% top %}
