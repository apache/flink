---
title: "流上的 Join"
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

Join 在批数据处理中是比较常见且广为人知的运算，一般用于连接两张关系表。然而在[动态表]({%link dev/table/streaming/dynamic_tables.zh.md %})中 Join 的语义会难以理解甚至让人困惑。

因而，Flink 提供了几种基于 Table API 和 SQL 的 Join 方法。

欲获取更多关于 Join 语法的细节，请参考 [Table API]({%link dev/table/tableApi.zh.md %}#joins) 和 [SQL]({%link dev/table/sql/queries.zh.md %}#joins) 中的 Join 章节。

* This will be replaced by the TOC
{:toc}

<a name="regular-joins"></a>

常规 Join
-------------

常规 Join 是最常用的 Join 用法。在常规 Join 中，任何新记录或对 Join 两侧表的任何更改都是可见的，并会影响最终整个 Join 的结果。例如，如果 Join 左侧插入了一条新的记录，那么它将会与 Join 右侧过去与将来的所有记录进行 Join 运算。

{% highlight sql %}
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
{% endhighlight %}

上述语意允许对输入表进行任意类型的更新操作（insert, update, delete）。

然而，常规 Join 隐含了一个重要的前提：即它需要在 Flink 的状态中永久保存 Join 两侧的数据。因而，如果 Join 操作中的一方或双方输入表持续增长的话，资源消耗也将会随之无限增长。

<a name="interval-joins"></a>

时间区间 Join
-------------------

如果一个 Join 限定输入[时间属性]({%link dev/table/streaming/time_attributes.zh.md %})必须在一定的时间限制中（即时间窗口），那么就称之为时间区间 Join。

{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

与常规 Join 操作相比，时间区间 Join 只支持带有时间属性的递增表。由于时间属性是单调递增的，Flink 可以从状态中移除过期的数据，而不会影响结果的正确性。

<a name="join-with-a-temporal-table-function"></a>

时态表 Join
--------------------------
<span class="label label-danger">注意</span> 只在 Blink planner 中支持。
<span class="label label-danger">注意</span> 时态表有两种方式去定义，即 [时态表函数]({% link dev/table/streaming/versioned_tables.zh.md %}#时态表函数) 和 [时态表 DDL]({% link dev/table/streaming/versioned_tables.zh.md %}#时态表)，使用时态表函数的时态表 join 只支持在 Table API 中使用，使用时态表 DDL 的时态表 join 只支持在 SQL 中使用。
请参考[时态表]({% link dev/table/streaming/versioned_tables.zh.md %})页面获取更多关于时态表和时态表函数的区别。

时态表 Join 意味着对任意表（左输入/探针侧）去关联一个时态表（右输入/构建侧）的版本，时态表可以是一张跟踪所有变更记录的表（例如数据库表的 changelog，包含多个表快照），也可以是物化所有变更之后的表（例如数据库表，只有最新表快照）。

Flink 使用了 SQL:2011 标准引入的时态表 Join 语法，时态表 Join 的语法如下:

{% highlight sql %}
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
{% endhighlight %}
 
<a name="processing-time-temporal-joins"></a>

### 基于事件时间的时态 Join
基于事件时间的时态表 join 使用(左侧输入/探针侧) 的 事件时间 去关联(右侧输入/构建侧) [版本表]({% link dev/table/streaming/versioned_tables.zh.md %}#声明版本表) 对应的版本。
基于事件时间的时态表 join 仅支持关版本表或版本视图，版本表或版本视图只能是一个 changelog 流。 但是，Flink 支持将 append-only 流转换成 changelog 流，因此版本表也可以来自一个 append-only 流。
查看[声明版本视图]({% link dev/table/streaming/versioned_tables.zh.md %}#声明版本视图) 获取更多的信息关于如何声明一张来自 append-only 流的版本表。

将事件时间作为时间属性时，可将 _过去_ 时间属性与时态表一起使用。这允许对两个表中在相同时间点的记录执行 Join 操作。
与基于处理时间的时态 Join 相比，时态表不仅将构建侧记录的最新版本（是否最新由所定义的主键所决定）保存在 state 中，同时也会存储自上一个 watermarks 以来的所有版本（按时间区分）。

例如，在探针侧表新插入一条事件时间时间为 `12:30:00` 的记录，它将和构建侧表时间点为 `12:30:00` 的版本根据[时态表的概念]({% link dev/table/streaming/versioned_tables.zh.md %})进行 Join 运算。
因此，新插入的记录仅与时间戳小于等于 `12:30:00` 的记录进行 Join 计算（由主键决定哪些时间点的数据将参与计算）。

通过定义事件时间，[watermarks]({% link  dev/event_time.zh.md %}) 允许 Join 运算不断向前滚动，丢弃不再需要的构建侧快照。因为不再需要时间戳更低或相等的记录。

下面的例子展示了订单流关联产品表这个场景举例，`orders` 表包含了来自 Kafka 的实时订单流，`product_changelog` 表来自数据库表 `products` 的 changelog , 产品的价格在数据库表 `products` 中是随时间实时变化的。

{% highlight sql %}
SELECT * FROM product_changelog;

(changelog kind)  update_time product_name price
================= =========== ============ ===== 
+(INSERT)         00:01:00    scooter      11.11
+(INSERT)         00:02:00    basketball   23.11
-(UPDATE_BEFORE)  12:00:00    scooter      11.11
+(UPDATE_AFTER)   12:00:00    scooter      12.99  <= 产品 `scooter` 在 `12:00:00` 时涨价到了 `12.99`
-(UPDATE_BEFORE)  12:00:00    basketball   23.11 
+(UPDATE_AFTER)   12:00:00    basketball   19.99  <= 产品 `basketball` 在 `12:00:00` 时降价到了 `19.99`
-(DELETE)         18:00:00    scooter      12.99  <= 产品 `scooter` 在 `18:00:00` 从数据库表中删除
{% endhighlight %}

如果我们想输出 `product_changelog` 表在 `10:00:00` 对应的版本，表的内容如下所示：
{% highlight sql %}
update_time  product_id product_name price
===========  ========== ============ ===== 
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
{% endhighlight %}

如果我们想输出 `product_changelog` 表在 `13:00:00` 对应的版本，表的内容如下所示：
{% highlight sql %}
update_time  product_id product_name price
===========  ========== ============ ===== 
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
{% endhighlight %}

通过基于事件时间的时态表 join, 我们可以 join 上版本表中的不同版本：
{% highlight sql %}
CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time  -- defines the necessary event time
) WITH (
...
);

-- 设置会话的时间区间, changelog 里的数据库操作时间是以 epoch 开始的毫秒数存储的，
-- 在从毫秒转化为时间戳时，Flink SQL 会使用会话的时间区间
-- 因此，请根据 changelog 中的数据库操作时间设置合适的时间区间
SET table.local-time-zone=UTC;

-- 声明一张版本表
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL, -- 注意：自动从毫秒数转为时间戳
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) defines the primary key constraint
  WATERMARK FOR update_time AS update_time   -- (2) defines the event time by watermark                               
) WITH (
  'connector' = 'kafka',
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);

-- 基于事件时间的时态表 Join
SELECT
  order_id,
  order_time,
  product_name,
  product_time,
  price
FROM orders AS O
LEFT JOIN product_changelog FOR SYSTEM_TIME AS OF O.order_time AS P
ON O.product_id = P.product_id;

order_id order_time product_name product_time price
======== ========== ============ ============ =====
o_001    00:01:00   scooter      00:01:00     11.11
o_002    00:03:00   basketball   00:02:00     23.11
o_003    12:00:00   scooter      12:00:00     12.99
o_004    12:00:00   basketball   12:00:00     19.99
o_005    18:00:00   NULL         NULL         NULL
{% endhighlight %}

基于事件时间的时态表 Join 通常用在通过 changelog 丰富流上数据的场景。 

**注意**: 基于事件时间的时态表 Join 是通过左右两侧的 watermark 触发，请确保为 join 两侧的表设置了合适的 watermark。

**注意**: 基于事件时间的时态表 Join 的 join key 必须包含时态表的主键，例如：表 `product_changelog` 的主键 `P.product_id` 必须包含在 join 条件 `O.product_id = P.product_id` 中。

### 基于处理时间的时态 Join

基于处理时间的时态表 join 使用任意表 (左侧输入/探针侧) 的 处理时间 去关联 (右侧输入/构建侧) [普通表]({% link dev/table/streaming/versioned_tables.zh.md %}#声明普通表)的最新版本.
基于处理时间的时态表 join 当前只支持关联普通表或普通视图，且支持普通表或普通视图当前只能是 append-only 流。

如果将处理时间作为时间属性，_过去_ 时间属性将无法与时态表一起使用。根据定义，处理时间总会是当前时间戳。
因此，关联时态表的调用将始终返回底层表的最新已知版本，并且底层表中的任何更新也将立即覆盖当前值。

可以将处理时间的时态 Join 视作简单的 `HashMap <K，V>`，HashMap 中存储来自构建侧的所有记录。
当来自构建侧的新插入的记录与旧值具有相同的 Key 时，旧值会被覆盖。
探针侧的每条记录将总会根据 `HashMap` 的最新/当前状态来计算。

接下来的示例展示了订单流 `Orders` 该如何与实时变化的汇率表 `Lates` 进行基于处理时间的时态 Join 操作，`LatestRates` 总是表示 HBase 表 `Rates` 的最新内容。

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

通过时态表 Join，我们可以将上述操作表示为以下 SQL 查询：

{% highlight sql %}
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
{% endhighlight %}

探针侧表中的每个记录都将与构建侧表的当前版本所关联。 在此示例中，查询使用`处理时间`作为处理时间，因而新增订单将始终与表 `LatestRates` 的最新汇率执行 Join 操作。注意，结果对于处理时间来说不是确定的。
基于处理时间的时态表 Join 通常用在通过外部表（例如维度表）丰富流上数据的场景。 

与[常规 Join](#常规-join) 相比，尽管构建侧表的数据发生了变化，但时态表 Join 的变化前结果不会随之变化。
* 对于基于事件时间的时态 Join， join 算子保留 Join 两侧流的状态并通过 watermark 清理。 
* 对于基于处理时间的时态 Join， join 算子保留仅保留右侧（构建侧）的状态，且构建侧的状态只包含数据的最新版本，右侧的状态是轻量级的; 对于在运行时有能力查询外部系统的时态表，join 算子还可以优化成不保留任何状态，此时算子是非常轻量级的。

与[时间区间 Join](#时间区间-join) 相比，时态表 Join 没有定义决定构建侧记录所属的将被 Join 时间窗口。
探针侧的记录将总是与构建侧在对应`处理时间`的最新数据执行 Join，因而构建侧的数据可能是任意旧的。
 
时态表函数 Join
--------------------------

时态表函数 Join 连接了一个递增表（左输入/探针侧）和一个时态表（右输入/构建侧），即一个随时间变化且不断追踪其改动的表。请参考[时态表]({% link dev/table/streaming/versioned_tables.zh.md %})的相关章节查看更多细节。

下方示例展示了一个递增表 `Orders` 与一个不断改变的汇率表 `RatesHistory` 的 Join 操作。

`Orders` 表示了包含支付数据（数量字段 `amount` 和货币字段 `currency`）的递增表。例如 `10:15` 对应行的记录代表了一笔 2 欧元支付记录。

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

字段 `RatesHistory` 表示不断变化的汇率信息。汇率以日元为基准（即 `Yen` 永远为 1）。例如，`09:00` 到 `10:45` 间欧元对日元的汇率是 `114`，`10:45` 到 `11:15` 间为 `116`。

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

基于上述信息，欲计算 `Orders` 表中所有交易量并全部转换成日元。例如，若要转换下表中的交易，需要使用对应时间区间内的汇率（即 `114`）。

{% highlight text %}
rowtime amount currency
======= ====== =========
10:15        2 Euro
{% endhighlight %}

如果没有[时态表]({% link dev/table/streaming/versioned_tables.zh.md %})概念，则需要写一段这样的查询：

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

有了时态表函数 `Rates` 和 `RatesHistory` 的帮助，我们可以将上述查询写成：

{% highlight sql %}
SELECT
  o.amount * r.rate AS amount
FROM
  Orders AS o,
  LATERAL TABLE (Rates(o.rowtime)) AS r
WHERE r.currency = o.currency
{% endhighlight %}

探针侧的每条记录都将与构建侧的表执行 Join 运算，构建侧的表中与探针侧对应时间属性的记录将参与运算。为了支持更新（包括覆盖）构建侧的表，该表必须定义主键。

在示例中，`Orders` 表中的每一条记录都与时间点 `o.rowtime` 的 `Rates` 进行 Join 运算。`currency` 字段已被定义为 `Rates` 表的主键，在示例中该字段也被用于连接两个表。如果该查询采用的是处理时间，则在执行时新增的订单将始终与最新的 `Rates` 执行 Join。

与[常规 Join](#常规-join) 相反，时态表函数 Join 意味着如果在构建侧新增一行记录将不会影响之前的结果。这同时使得 Flink 能够限制必须保存在 state 中的元素数量（因为不再需要保存之前的状态）。

与[时间区间 Join](#时间区间-join) 相比，时态表 Join 没有定义限制了每次参与 Join 运算的元素的时间范围。探针侧的记录总是会和构建侧中对应特定时间属性的数据进行 Join 操作。因而在构建侧的记录可以是任意时间之前的。随着时间流动，之前产生的和不再需要的给定 primary key 所对应的记录将从 state 中移除。

这种做法让时态表 Join 成为一个很好的用于表达不同流之间关联的方法。

[时态表函数 Join](#时态表函数-join) 和[时态表 Join](#时态表-join)都有类似的功能，但是有不同的 SQL 语法和 runtime 实现：

* 时态表函数 Join 的 SQL 语法是一种 Join 用户定义生成表函数(UDTF，User-Defined Table-Generating Functions)，而时态表 Join 使用了 SQL:2011 标准引入的标准时态表语法。
* 时态表 Join 的覆盖了时态表函数 Join 支持的所有功能，两者共享部分算子实现，基于事件时间的时态表 Join 从 Flink 1.12 开始支持。
* 时态表函数 Join 总是保留数据流的状态，但在一些情况下，时态表 Join 可以不用保留流的状态，即：基于处理时间的时态表 Join 中， join 算子可以在运行时根据记录的键值查找外部数据库而不是从状态中获取。
* 时态表函数 Join 在 legacy planer 和 Blink planer 中均支持，而时态表 Join 仅在 Blink planner 中支持，legacy planner 在将来会被废弃。。

**注意**: 基于处理时间的时态 Join 中， 如果右侧表不是可以直接查询外部系统的表而是普通的数据流，时态表函数 Join 和 时态表 Join 的语义都有问题，时态表函数 Join 仍然允许使用，但是时态表 Join 禁用了该功能。
语义问题的原因是 join 算子没办法知道右侧时态表（构建侧）的完整快照是否到齐，这可能导致左侧的流在启动时关联不到用户期待的数据, 在生产环境中可能误导用户。

Flink SQL 在未来可能需要引入新的机制去获取右侧时态表的完整快照。

### 用法

在[定义时态表函数]({%link dev/table/streaming/versioned_tables.zh.md %}#defining-temporal-table-function)之后就可以使用了。时态表函数可以和普通表函数一样使用。

接下来这段代码解决了我们一开始提出的问题，即从计算订单表 `Orders` 的交易量之和，并转换为对应货币：

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
{% top %}
