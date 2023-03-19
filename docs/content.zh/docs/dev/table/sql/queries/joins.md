---
title: "Join"
weight: 10
type: docs
aliases:
  - /zh/dev/table/streaming/joins.html
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

# Joins

{{< label Batch >}} {{< label Streaming >}}

Flink SQL支持对动态表进行复杂而灵活的连接操作。
为了处理不同的场景，需要多种查询语义，因此有几种不同类型的 Join。

默认情况下，joins 的顺序是没有优化的。表的 join 顺序是在 `FROM` 从句指定的。可以通过把更新频率最低的表放在第一个、频率最高的放在最后这种方式来微调 join 查询的性能。需要确保表的顺序不会产生笛卡尔积，因为不支持这样的操作并且会导致查询失败。

Regular Joins
-------------

Regular join 是最通用的 join 类型。在这种 join 下，join 两侧表的任何新记录或变更都是可见的，并会影响整个 join 的结果。
例如：如果左边有一条新纪录，在 `Product.id` 相等的情况下，它将和右边表的之前和之后的所有记录进行 join。

```sql
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
```

对于流式查询，regular join 的语法是最灵活的，允许任何类型的更新（插入、更新、删除）输入表。
然而，这种操作具有重要的操作意义：Flink 需要将 Join 输入的两边数据永远保持在状态中。
因此，计算查询结果所需的状态可能会无限增长，这取决于所有输入表的输入数据量。你可以提供一个合适的状态 time-to-live (TTL) 配置来防止状态过大。注意：这样做可能会影响查询的正确性。查看 [查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) 了解详情。

### INNER Equi-JOIN

根据 join 限制条件返回一个简单的笛卡尔积。目前只支持 equi-joins，即：至少有一个等值条件。不支持任意的 cross join 和 theta join。(cross join 指的是类似 `SELECT * FROM table_a CROSS JOIN table_b`，theta join 指的是类似 `SELECT * FROM table_a, table_b`）

```sql
SELECT *
FROM Orders
INNER JOIN Product
ON Orders.product_id = Product.id
```

### OUTER Equi-JOIN

返回所有符合条件的笛卡尔积（即：所有通过 join 条件连接的行），加上所有外表没有匹配到的行。Flink 支持 LEFT、RIGHT 和 FULL outer joins。目前只支持 equi-joins，即：至少有一个等值条件。不支持任意的 cross join 和 theta join。

```sql
SELECT *
FROM Orders
LEFT JOIN Product
ON Orders.product_id = Product.id

SELECT *
FROM Orders
RIGHT JOIN Product
ON Orders.product_id = Product.id

SELECT *
FROM Orders
FULL OUTER JOIN Product
ON Orders.product_id = Product.id
```

Interval Joins
--------------

返回一个符合 join 条件和时间限制的简单笛卡尔积。Interval join 需要至少一个 equi-join 条件和一个 join 两边都包含的时间限定 join 条件。范围判断可以定义成就像一个条件（<, <=, >=, >），也可以是一个 BETWEEN 条件，或者两边表的一个相同类型（即：处理时间 或 事件时间）的[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}) 的等式判断。

例如：如果订单是在被接收到4小时后发货，这个查询会把所有订单和它们相应的 shipments join 起来。

```sql
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time
```

下面列举了一些有效的 interval join 时间条件：

- `ltime = rtime`
- `ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE`
- `ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND`

对于流式查询，对比 regular join，interval join 只支持有时间属性的非更新表。
由于时间属性是递增的，Flink 从状态中移除旧值也不会影响结果的正确性。

Temporal Joins
--------------

时态表（Temporal table）是一个随时间变化的表：在 Flink 中被称为[动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}})。时态表中的行与一个或多个时间段相关联，所有 Flink 中的表都是时态的（Temporal）。
时态表包含一个或多个版本的表快照，它可以是一个变化的历史表，跟踪变化（例如，数据库变化日志，包含所有快照）或一个变化的维度表，也可以是一个将变更物化的维表（例如，存放最终快照的数据表）。

### 事件时间 Temporal Join

基于事件时间的 Temporal join 允许对[版本表]({{< ref "docs/dev/table/concepts/versioned_tables" >}})进行 join。
这意味着一个表可以使用变化的元数据来丰富，并在某个时间点检索其具体值。

Temporal Joins 使用任意表（左侧输入/探测端）的每一行与版本表中对应的行进行关联（右侧输入/构建端）。
Flink 使用 `SQL:2011标准` 中的 `FOR SYSTEM_TIME AS OF` 语法去执行操作。
Temporal join 的语法如下：

```sql
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
```

有了事件时间属性（即：rowtime 属性），就能检索到过去某个时间点的值。
这允许在一个共同的时间点上连接这两个表。
版本表将存储自最后一个 watermark 以来的所有版本（按时间标识）。

例如，假设我们有一个订单表，每个订单都有不同货币的价格。
为了正确地将该表统一为单一货币（如美元）,每个订单都需要与下单时相应的汇率相关联。

```sql
-- Create a table of orders. This is a standard
-- append-only dynamic table.
CREATE TABLE orders (
    order_id    STRING,
    price       DECIMAL(32,2),
    currency    STRING,
    order_time  TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '15' SECOND
) WITH (/* ... */);

-- Define a versioned table of currency rates. 
-- This could be from a change-data-capture
-- such as Debezium, a compacted Kafka topic, or any other
-- way of defining a versioned table. 
CREATE TABLE currency_rates (
    currency STRING,
    conversion_rate DECIMAL(32, 2),
    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,
    WATERMARK FOR update_time AS update_time - INTERVAL '15' SECOND,
    PRIMARY KEY(currency) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'value.format' = 'debezium-json',
   /* ... */
);

SELECT 
     order_id,
     price,
     orders.currency,
     conversion_rate,
     order_time
FROM orders
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time
ON orders.currency = currency_rates.currency;

order_id  price  currency  conversion_rate  order_time
========  =====  ========  ===============  =========
o_001     11.11  EUR       1.14             12:00:00
o_002     12.51  EUR       1.10             12:06:00

```

**注意：** 事件时间 temporal join 是通过左和右两侧的 watermark 触发的；
这里的 `INTERVAL` 时间减法用于等待后续事件，以确保 join 满足预期。
请确保 join 两边设置了正确的 watermark 。

**注意：** 事件时间 temporal join 需要包含主键相等的条件，即：`currency_rates` 表的主键 `currency_rates.currency` 包含在条件 `orders.currency = currency_rates.currency` 中。

与 [regular joins](#regular-joins) 相比，就算 build side（例子中的 currency_rates 表）发生变更了，之前的 temporal table 的结果也不会被影响。
与 [interval joins](#interval-joins) 对比，temporal join没有定义join的时间窗口。
Probe side （例子中的 orders 表）的记录总是在 time 属性指定的时间与 build side 的版本行进行连接。因此，build side 表的行可能已经过时了。
随着时间的推移，不再被需要的记录版本（对于给定的主键）将从状态中删除。

### 处理时间 Temporal Join

基于处理时间的 temporal join 使用处理时间属性将数据与外部版本表（例如 mysql、hbase）的最新版本相关联。

通过定义一个处理时间属性，这个 join 总是返回最新的值。可以将 build side 中被查找的表想象成一个存储所有记录简单的 `HashMap<K,V>`。
这种 join 的强大之处在于，当无法在 Flink 中将表具体化为动态表时，它允许 Flink 直接针对外部系统工作。

下面这个处理时间 temporal join 示例展示了一个追加表 `orders` 与 `LatestRates` 表进行 join。
`LatestRates` 是一个最新汇率的维表，比如 HBase 表，在 `10:15`，`10:30`，`10:52`这些时间，`LatestRates` 表的数据看起来是这样的：

```sql
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
```

`LastestRates` 表的数据在 `10:15` 和 `10:30` 是相同的。
欧元（Euro）的汇率（rate）在 `10:52` 从 114 变更为 116。

`Orders` 表示支付金额的 `amount` 和`currency`的追加表。
例如：在 `10:15` ，有一个金额为 `2 Euro` 的 order。

```sql
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
```

给出下面这些表，我们希望所有 `Orders` 表的记录转换为一个统一的货币。

```text
amount currency     rate   amount*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
```

目前，temporal join 还不支持与任意 view/table 的最新版本 join 时使用 `FOR SYSTEM_TIME AS OF` 语法。可以像下面这样使用 temporal table function 语法来实现（时态表函数）:

```sql
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
```

**注意** Temporal join 不支持与 table/view 的最新版本进行 join 时使用 `FOR SYSTEM_TIME AS OF` 语法是出于语义考虑，因为左流的连接处理不会等待 temporal table 的完整快照，这可能会误导生产环境中的用户。处理时间 temporal join 使用 temporal table function 也存在相同的语义问题，但它已经存在了很长时间，因此我们从兼容性的角度支持它。

processing-time 的结果是不确定的。
processing-time temporal join 常常用在使用外部系统来丰富流的数据。（例如维表）

与 [regular joins](#regular-joins) 的差异，就算 build side（例子中的 currency_rates 表）发生变更了，之前的 temporal table 结果也不会被影响。
与 [interval joins](#interval-joins) 的差异，temporal join 没有定义数据连接的时间窗口。即：旧数据没存储在状态中。

### Temporal Table Function Join

使用 [temporal table function]({{< ref "docs/dev/table/concepts/temporal_table_function" >}}) 去 join 表的语法和 [Table Function](#table-function) 相同。

注意：目前只支持 inner join 和 left outer join。

假设`Rates`是一个 temporal table function，这个 join 在 SQL 中可以被表达为：

```sql
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
```

上述 temporal table DDL 和 temporal table function 的主要区别在于：

- SQL 中可以定义 temporal table DDL，但不能定义 temporal table 函数;
- temporal table DDL 和 temporal table function 都支持 temporal join 版本表，但只有 temporal table function 可以 temporal join 任何表/视图的最新版本（即"处理时间 Temporal Join"）。

Lookup Join
--------------

lookup join 通常用于使用从外部系统查询的数据来丰富表。join 要求一个表具有处理时间属性，另一个表由查找源连接器（lookup source connnector）支持。

lookup join 和上面的 [处理时间 Temporal Join](#processing-time-temporal-join) 语法相同，右表使用查找源连接器支持。

下面的例子展示了 lookup join 的语法。

```sql
-- Customers is backed by the JDBC connector and can be used for lookup joins
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
  'table-name' = 'customers'
);

-- enrich each order with customer information
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id;
```

在上面的示例中，Orders 表由保存在 MySQL 数据库中的 Customers 表数据来丰富。带有后续 process time 属性的 `FOR SYSTEM_TIME AS OF` 子句确保在联接运算符处理 `Orders` 行时，`Orders` 的每一行都与 join 条件匹配的 Customer 行连接。它还防止连接的 `Customer` 表在未来发生更新时变更连接结果。lookup join 还需要一个强制的相等连接条件，在上面的示例中是 `o.customer_id = c.id`。

数组展开
--------------

返回给定数组中每个元素的新行。尚不支持 `WITH ORDINALITY`（会额外生成一个标识顺序的整数列）展开。

```sql
SELECT order_id, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
```

Table Function
--------------

将表与表函数的结果联接。左侧（外部）表的每一行都与表函数的相应调用产生的所有行相连接。[用户自定义表函数]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions) 必须在使用前注册。

### INNER JOIN

如果表函数调用返回一个空结果，那么左表的这行数据将不会输出。

```sql
SELECT order_id, res
FROM Orders,
LATERAL TABLE(table_func(order_id)) t(res)
```

### LEFT OUTER JOIN

如果表函数调用返回了一个空结果，则保留相应的行，并用空值填充未关联到的结果。当前，针对 lateral table 的 left outer join 需要 ON 子句中有一个固定的 TRUE 连接条件。

```sql
SELECT order_id, res
FROM Orders
LEFT OUTER JOIN LATERAL TABLE(table_func(order_id)) t(res)
  ON TRUE
```

{{< top >}}
