---
title: "流上的 Join"
nav-parent_id: streaming_tableapi
nav-pos: 3
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

Join 在批数据处理中是比较常见且广为人知的运算，一般用于连接两张关系表。然而在[动态表](dynamic_tables.html)中 Join 的语义会更难以理解甚至让人困惑。

因而，Flink 提供了几种基于 Table API 和 SQL 的 Join 方法。

欲获取更多关于 Join 语法的细节，请参考 [Table API](../tableApi.html#joins) 和 [SQL]({{ site.baseurl }}/dev/table/sql/queries.html#joins) 中的 Join 章节。

* This will be replaced by the TOC
{:toc}

常规 Join
-------------

常规 Join 是最常用的 Join 用法。在常规 Join 中，任何新记录或对 Join 两侧的表的任何更改都是可见的，并会影响最终整个 Join 的结果。例如，如果 Join 左侧插入了一条新的记录，那么它将会与 Join 右侧过去与将来的所有记录一起合并查询。

{% highlight sql %}
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
{% endhighlight %}

上述语意允许对输入表进行任意类型的更新操作（insert, update, delete）。

然而，常规 Join 隐含了一个重要的前提：即它需要在 Flink 的状态中永久保存 Join 两侧的数据。
因而，如果 Join 操作中的一方或双方输入表持续增长的话，资源消耗也将会随之无限增长。

时间窗口 Join
-------------------

如果一个 Join 限定输入[时间属性](time_attributes.html)必须在一定的时间限制中（即时间窗口），那么就称之为时间窗口 Join。

{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

与常规 Join 操作相比，时间窗口 Join 只支持带有时间属性的递增表。由于时间属性是单调递增的，Flink 可以从状态中移除过期的数据，而不会影响结果的正确性。

临时表函数 Join
--------------------------

临时表函数 Join 连接了一个递增表（左输入/探针侧）和一个临时表（右输入/构建侧），即一个随时间变化且不断追踪其改动的表。请参考[临时表](temporal_tables.html)的相关章节查看更多细节。

下方示例展示了一个递增表 `Orders` 与一个不断改变的汇率表 `RatesHistory` 的 Join 操作。

`Orders` 表示了包含支付数据（数量字段 `amount` 和货币字段 `currency`）的递增表。
例如 `10:15` 对应行的记录代表了一笔 2 欧元支付记录。

{% highlight sql %}
SELECT * FROM Orders;

rowtime amount currency
======= ====== =========
10:15        2 Euro
10:30        1 US Dollar
10:32       50 Yen
10:52        3 Euro
11:04        5 US Dollar
{% endhighlight %}

字段 `RatesHistory` 表示不断变化的汇率信息。汇率以日元为基准（即 `Yen` 永远为 1）。
例如，`09:00` 到 `10:45` 间欧元对日元的汇率是 `114`，`10:45` 到 `11:15` 间为 `116`。

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

基于上述信息，欲计算 `Orders` 表中所有交易量并全部转换成日元。

例如，若要转换下表中的交易，需要使用对应时间区间内的汇率（即 `114`）。

{% highlight text %}
rowtime amount currency
======= ====== =========
10:15        2 Euro
{% endhighlight %}

如果没有[临时表](temporal_tables.html)概念，则需要写一段这样的查询：

{% highlight sql %}
SELECT
  SUM(o.amount * r.rate) AS amount
FROM Orders AS o,
  RatesHistory AS r
WHERE r.currency = o.currency
AND r.rowtime = (
  SELECT MAX(rowtime)
  FROM RatesHistory AS r2
  WHERE r2.currency = o.currency
  AND r2.rowtime <= o.rowtime);
{% endhighlight %}

有了临时表函数 `Rates` 和 `RatesHistory` 的帮助，我们可以将上述查询写成：

{% highlight sql %}
SELECT
  o.amount * r.rate AS amount
FROM
  Orders AS o,
  LATERAL TABLE (Rates(o.rowtime)) AS r
WHERE r.currency = o.currency
{% endhighlight %}

探针侧的每条记录都将与构建侧的表执行 Join 运算，构建侧的表中与探针侧对应时间属性的记录将参与运算。
为了支持更新（包括覆盖）构建侧的表，该表必须定义主键。

在示例中，`Orders` 表中的每一条记录都与时间点 `o.rowtime` 的 `Rates` 进行 Join 运算。`currency` 字段已被定义为 `Rates` 表的主键，在示例中该字段也被用于连接两个表。如果该查询采用的是 processing-time，则在执行时新增的订单将始终与最新的 `Rates` 执行 Join。

与[常规 Join](#regular-joins)相反，临时表函数 Join 意味着如果在构建侧新增一行记录将不会影响之前的结果。这同时使得 Flink 能够限制必须保存在 state 中的元素数量（因为不再需要保存之前的状态）。

与[时间窗口 Join](#time-windowed-joins) 相比，临时表 Join 没有定义限制了每次参与 Join 运算的元素的时间范围。探针侧的记录总是会和构建侧中对应特定时间属性的数据进行 Join 操作。因而在构建侧的记录可以是任意时间之前的。随着时间流动，之前产生的不再需要的记录（已给定了主键）将从 state 中移除。

这种做法让临时表 Join 成为一个很好的用于表达不同流之间关联的方法。

### 用法

在 [定义临时表函数](temporal_tables.html#defining-temporal-table-function) 之后就可以使用了。
临时表函数可以和普通表函数一样使用。

接下来这段代码解决了我们一开始提出的问题，即从计算 `Orders` 表中交易量之和并转换为对应货币：

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
Table result = orders
    .joinLateral("rates(o_proctime)", "o_currency = r_currency")
    .select("(o_amount * r_rate).sum as amount");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val result = orders
    .joinLateral(rates('o_proctime), 'r_currency === 'o_currency)
    .select(('o_amount * 'r_rate).sum as 'amount)
{% endhighlight %}
</div>
</div>

**注意**: 临时 Join中的 State 保留（在 [查询配置](query_configuration.html) 中定义）还未实现。这意味着计算的查询结果所需的状态可能会无限增长，具体数量取决于历史记录表的不重复主键个数。

### 基于 Processing-time 临时 Join

如果将 processing-time 作为时间属性，将无法将 _past_ 时间属性作为参数传递给临时表函数。
根据定义，processing-time 总会是当前时间戳。因此，基于 processing-time 的临时表函数将始终返回基础表的最新已知版本，时态表函数的调用将始终返回基础表的最新已知版本，并且基础历史表中的任何更新也将立即覆盖当前值。

只有最新版本的构建侧记录（是否最新由所定义的主键所决定）会被保存在 state 中。
构建侧的更新不会对之前 Join 的结果产生影响。

可以将 processing-time 的临时 Join 视作简单的哈希Map `HashMap <K，V>`，HashMap 中存储来自构建侧的所有记录。
当来自构建侧的新插入的记录与旧值具有相同的 Key 时，旧值仅会被覆盖。
探针侧的每条记录将总会根据 `HashMap` 的最新/当前状态来计算。

### 基于 Event-time 临时 Join

将 event-time 作为时间属性时，可将 _past_ 时间属性作为参数传递给临时表函数。
这允许对两个表中在相同时间点的记录执行 Join 操作。

与基于 processing-time 的临时 Join 相比，临时表不仅将构建侧记录的最新版本（是否最新由所定义的主键所决定）保存在 state 中，同时也会存储自上一个水印以来的所有版本（按时间区分）。

例如，在探针侧表新插入一条 event-time 时间为 `12:30:00` 的记录，它将和构建侧表时间点为 `12:30:00` 的版本根据[临时表的概念](temporal_tables.html)进行 Join 运算。
因此，新插入的记录仅与时间戳小于等于 `12:30:00` 的记录进行 Join 计算（由主键决定哪些时间点的数据将参与计算）。

通过定义事件时间（event time），[watermarks]({{ site.baseurl }}/dev/event_time.html) 允许 Join 运算不断向前滚动，丢弃不再需要的构建侧快照。因为不再需要时间戳更低或相等的记录。

临时表 Join
--------------------------

临时表 Join 意味着对任意表（左输入/探针侧）和一个临时表（右输入/构建侧）执行的 Join 操作，即随时间变化的的扩展表。请参考相应的页面以获取更多有关[临时表](temporal_tables.html#temporal-table)的信息。

<span class="label label-danger">注意</span> 不是任何表都能用作临时表，用户必须使用来自接口 `LookupableTableSource` 的表。接口 `LookupableTableSource` 的实例只能作为临时表用于临时 Join 。查看此页面获取更多关于[如何实现接口 `LookupableTableSource`](../sourceSinks.html#defining-a-tablesource-with-lookupable) 的详细内容。

接下来的示例展示了订单流 `Orders` 该如何与实时变化的汇率表 `Lates` 进行 Join 操作。

`LatestRates` 是实时更新的汇率表。在时间点 `10:15`, `10:30`, `10:52` 中，表 `LatestRates` 中的内容分别如下：

{% highlight sql %}
10:15> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1

10:30> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1


10:52> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        116     <==== changed from 114 to 116
Yen           1
{% endhighlight %}

表 `LastestRates` 中的数据在时间点 `10:15` 和 `10:30` 时是相等的。欧元汇率在时间点 `10:52` 从 114 变化至 116 。

表 `Orders` 包含了金额字段 `amount` 和货币字段 `currency` 的支付记录数据。例如在 `10:15` 有一笔金额为 `2` 欧元的订单记录。

{% highlight sql %}
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
{% endhighlight %}

基于以上，我们想要计算所有 `Orders` 表的订单金额总和，并同时转换为对应成日元的金额。

例如，我们想要以表 `LatestRates` 中的汇率将以下订单转换，则结果将为：

{% highlight text %}
amount currency     rate   amout*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
{% endhighlight %}


通过临时表 Join，我们可以将上述操作表示为以下 SQL 查询：

{% highlight sql %}
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
{% endhighlight %}

探针侧表中的每个记录都将与构建侧表的当前版本所关联。 在此示例中，查询使用 `processing-time` 作为处理时间，因而新增订单将始终与表 `LatestRates` 的最新汇率执行 Join 操作。 注意，结果对于处理时间来说不是确定的。

与[常规 Join](#regular-joins) 相比，尽管构建侧表的数据发生了变化，但临时表 Join 的变化前结果不会随之变化。而且临时表 Join 运算非常轻量级且不会保留任何状态。

与[时间窗口 Join](#time-windowed-joins) 相比，临时表 Join 没有定义决定哪些记录将被 Join 的时间窗口。
探针侧的记录将总是与构建侧在对应 `processing time` 时间的最新数据执行 Join。因而构建侧的数据可能是任意旧的。

[临时表函数 Join](#join-with-a-temporal-table-function) 和临时表 Join都有类似的功能，但是有不同的 SQL 语法和 runtime 实现：

* 临时表函数 Join 的 SQL 语法是一种 Join 用户定义生成表函数(UDTF，User-Defined Table-Generating Functions)，而临时表 Join 使用了 SQL:2011 标准引入的标准临时表语法。
* 临时表函数 Join 的实现实际上是 Join 两个流并保存在 state 中，而临时表 Join 只接受唯一的输入流，并根据记录的键值查找外部数据库。
* 临时表函数 Join 通常用于与变更日志流执行 Join，而临时表 Join 通常与外部表（例如维度表）执行 Join 操作。

这种做法让临时表 Join 成为一个很好的用于表达不同流之间关联的方法。

将来，临时表 Join 将支持临时表函数 Join 的功能，即支持临时 Join 变更日志流。

### 用法

临时表 Join 的语法如下:

{% highlight sql %}
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.proctime [AS <alias2>]
ON table1.column-name1 = table2.column-name1
{% endhighlight %}

目前只支持 INNER JOIN 和 LEFT JOIN，`FOR SYSTEM_TIME AS OF table1.proctime` 应位于临时表之后. `proctime` 是 `table1` 的 [processing time 属性](time_attributes.html#processing-time).
这意味着在 Join 计算中连接左侧表中的每个记录时会为临时表产生快照。

例如在[定义临时表](temporal_tables.html#defining-temporal-table)之后，我们可以用以下方式使用：

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders
  JOIN LatestRates FOR SYSTEM_TIME AS OF o_proctime
  ON r_currency = o_currency
{% endhighlight %}
</div>
</div>

<span class="label label-danger">注意</span> 只在 Blink planner 中支持。

<span class="label label-danger">注意</span> 只在 SQL 中支持，Table API 暂不支持。

<span class="label label-danger">注意</span> Flink 目前不支持 event time 临时表的 Join 。

{% top %}
