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

时态表（Temporal Table）代表基于表的（参数化）视图概念，该表记录变更历史，该视图返回表在某个特定时间点的内容。

变更表可以是跟踪变化的历史记录表（例如数据库变更日志），也可以是有具体更改的维表（例如数据库表）。

对于记录变更历史的表，Flink 可以追踪这些变化，并且允许查询这张表在某个特定时间点的内容。在 Flink 中，这类表由*时态表函数（Temporal Table Function）*表示。

对于变化的维表，Flink 允许查询这张表在处理时的内容，在 Flink 中，此类表由*时态表（Temporal Table）*表示。

* This will be replaced by the TOC
{:toc}

设计初衷
----------

### 与记录变更历史的表相关

假设我们有表 `RatesHistory` 如下所示。

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

`RatesHistory` 代表一个兑换日元货币汇率表（日元汇率为1），该表是不断增长的 append-only 表。例如，`欧元`兑`日元`从 `09:00` 到 `10:45` 的汇率为 `114`。从 `10:45` 到 `11:15`，汇率为 `116`。

假设我们要输出 `10:58` 的所有当前汇率，则需要以下 SQL 查询来计算结果表：

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
 
下表显示了这种计算的结果。我们的示例中，在 `10:58` 时表的内容，考虑了 `10:45` 时`欧元`的更新，但未考虑 `11:15` 时的`欧元`更新和`英镑`的新值。

{% highlight text %}
rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Yen           1
10:45   Euro        116
{% endhighlight %}

 *时态表*的概念旨在简化此类查询，加快其执行速度，并减少 Flink 的状态使用。*时态表*是 append-only 表上的参数化视图，该视图将 append-only 表的行解释为表的变更日志，并在特定时间点提供该表的版本。将 append-only 表解释为变更日志需要指定主键属性和时间戳属性。主键确定哪些行将被覆盖，时间戳确定行有效的时间。

在上面的示例中，`currency` 是 `RatesHistory` 表的主键，而 `rowtime` 是时间戳属性。

在 Flink 中，这由[*时态表函数*](#temporal-table-function)表示。

### 与维表变化相关

另一方面，某些用例需要连接变化的维表，该表是外部数据库表。

假设 `LatestRates` 是一个被物化的最新汇率表。`LatestRates` 是物化的 `RatesHistory` 历史。那么 `LatestRates` 表在 `10:58` 的内容将是：

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

在 Flink 中，这由[*时态表*](#temporal-table)表示。

<a name="temporal-table-function"></a>

时态表函数
------------------------

为了访问时态表中的数据，必须传递一个[时间属性](time_attributes.html)，该属性确定将要返回的表的版本。
Flink 使用[表函数]({{ site.baseurl }}/zh/dev/table/functions/udfs.html#table-functions)的 SQL 语法提供一种表达它的方法。

定义后，*时态表函数*将使用单个时间参数 timeAttribute 并返回一个行集合。
该集合包含相对于给定时间属性的所有现有主键的行的最新版本。

假设我们基于 `RatesHistory` 表定义了一个时态表函数，我们可以通过以下方式查询该函数 `Rates(timeAttribute)`： 

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

对 `Rates(timeAttribute)` 的每个查询都将返回给定 `timeAttribute` 的 `Rates` 状态。

**注意**：当前 Flink 不支持使用常量时间属性参数直接查询时态表函数。目前，时态表函数只能在 join 中使用。上面的示例用于为函数 `Rates(timeAttribute)` 返回内容提供直观信息。

另请参阅有关[用于持续查询的 join ](joins.html)页面，以获取有关如何与时态表 join 的更多信息。

### 定义时态表函数

以下代码段说明了如何从 append-only 表中创建时态表函数。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// 获取 stream 和 table 环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 提供一个汇率历史记录表静态数据集
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// 用上面的数据集创建并注册一个示例表
// 在实际设置中，应使用自己的表替换它
DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

tEnv.createTemporaryView("RatesHistory", ratesHistory);

// 创建和注册时态表函数
// 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// 获取 stream 和 table 环境
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

// 提供一个汇率历史记录表静态数据集
val ratesHistoryData = new mutable.MutableList[(String, Long)]
ratesHistoryData.+=(("US Dollar", 102L))
ratesHistoryData.+=(("Euro", 114L))
ratesHistoryData.+=(("Yen", 1L))
ratesHistoryData.+=(("Euro", 116L))
ratesHistoryData.+=(("Euro", 119L))

// 用上面的数据集创建并注册一个示例表
// 在实际设置中，应使用自己的表替换它
val ratesHistory = env
  .fromCollection(ratesHistoryData)
  .toTable(tEnv, 'r_currency, 'r_rate, 'r_proctime.proctime)

tEnv.createTemporaryView("RatesHistory", ratesHistory)

// 创建和注册时态表函数
// 指定 "r_proctime" 为时间属性，指定 "r_currency" 为主键
val rates = ratesHistory.createTemporalTableFunction($"r_proctime", $"r_currency") // <==== (1)
tEnv.registerFunction("Rates", rates)                                          // <==== (2)
{% endhighlight %}
</div>
</div>

行`(1)`创建了一个 `rates` [时态表函数](#temporal-table-function)，
这使我们可以在[ Table API ](../tableApi.html#joins)中使用 `rates` 函数。

行`(2)`在表环境中注册名称为 `Rates` 的函数，这使我们可以在[ SQL ]({{ site.baseurl }}/zh/dev/table/sql/queries.html#joins)中使用 `Rates` 函数。

<a name="temporal-table"></a>

## 时态表

<span class="label label-danger">注意</span> 仅 Blink planner 支持此功能。

为了访问时态表中的数据，当前必须使用 `LookupableTableSource` 定义一个 `TableSource`。Flink 使用 SQL:2011 中提出的 `FOR SYSTEM_TIME AS OF` 的 SQL 语法查询时态表。

假设我们定义了一个时态表 `LatestRates`，我们可以通过以下方式查询此表：

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

**注意**：当前，Flink 不支持以固定时间直接查询时态表。目前，时态表只能在 join 中使用。上面的示例用于为时态表 `LatestRates` 返回内容提供直观信息。

另请参阅有关[用于持续查询的 join ](joins.html)页面，以获取有关如何与时态表 join 的更多信息。

### 定义时态表


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 获取 stream 和 table 环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
// or TableEnvironment tEnv = TableEnvironment.create(settings);

// 用 DDL 定义一张 HBase 表，然后我们可以在 SQL 中将其当作一张时态表使用
// 'currency' 列是 HBase 表中的 rowKey
tEnv.executeSql(
    "CREATE TABLE LatestRates (" +
    "   currency STRING," +
    "   fam1 ROW<rate DOUBLE>" +
    ") WITH (" +
    "   'connector' = 'hbase-1.4'," +
    "   'table-name' = 'Rates'," +
    "   'zookeeper.quorum' = 'localhost:2181'" +
    ")");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// 获取 stream 和 table 环境
val env = StreamExecutionEnvironment.getExecutionEnvironment
val settings = EnvironmentSettings.newInstance().build()
val tEnv = StreamTableEnvironment.create(env, settings)
// or val tEnv = TableEnvironment.create(settings)

// 用 DDL 定义一张 HBase 表，然后我们可以在 SQL 中将其当作一张时态表使用
// 'currency' 列是 HBase 表中的 rowKey
tEnv.executeSql(
    s"""
       |CREATE TABLE LatestRates (
       |    currency STRING,
       |    fam1 ROW<rate DOUBLE>
       |) WITH (
       |    'connector' = 'hbase-1.4',
       |    'table-name' = 'Rates',
       |    'zookeeper.quorum' = 'localhost:2181'
       |)
       |""".stripMargin)
{% endhighlight %}
</div>
</div>

另请参阅有关[如何定义 LookupableTableSource ](../sourceSinks.html#defining-a-tablesource-for-lookups)的页面。

{% top %}
