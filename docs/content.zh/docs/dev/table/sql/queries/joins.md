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

Flink SQL在动态表上支持复杂且灵活的join操作.不同类型的Join操作来完成各种语义的查询.

默认情况下,joins的顺序是没有优化的.表的join顺序是在`FROM`从句指定的.可以通过把更新频率最低的表放在第一个、频率最高的放在最后这种方式来微调join查询的性能.确保表的顺序不会产生笛卡尔积,因为不支持这样的操作并且会导致查询失败.

Regular Joins
-------------

Regular join是最通用的join类型.在这种join下,join两侧表的任何新记录或变更都是可见的,并会影响整个join的结果.例如:如果左边有一条新纪录,在product id相等的情况下,它将和右边表的之前和之后的所有记录进行join.


```sql
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
```


对于流式查询,regular查询的语法是最灵活的,并且允许任何类型的更新(insert,update,delete)输入表.这个操作有很重要的操作含义:Flink要用状态一直维护join两边的输入.因此,这些用于计算查询结果的状态可能会因为所有表输入的distinct输入行和中间join结果而无限膨胀.你可以提供一个合适的状态 time-to-live(TTL)配置来防止状态过大.注意:这样做可能会影响查询的正确性.查看[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) 了解详情.

{{< query_state_warning >}}

### INNER Equi-JOIN

根据join限制条件返回一个简单的笛卡尔积.目前只支持equi-joins,即:至少有一个等式判断(equality predicate)的连接条件.不支持arbitrary cross join和theta join. (译者注:arbitrary cross join指的是类似`select * from table_a corss join table_b`,theta join指的是类似`select * from table_a,table_b`)

```sql
SELECT *
FROM Orders
INNER JOIN Product
ON Orders.product_id = Product.id
```

### OUTER Equi-JOIN


返回所有符合条件的笛卡尔积(即:所有通过join条件连接的行),加上所有外表没有匹配到的行.Flink支持LEFT,RIGHT,和FULL outer joins.目前只支持equi-joins,即:至少有一个等式判断(equality predicate)的连接条件.不支持arbitrary cross join和theta join. (译者注:arbitrary cross join指的是类似:`select * from table_a corss join table_b`,theta join指的是类似:`select * from table_a,table_b`)

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

返回一个符合join条件和时间限制的简单笛卡尔积.interval join需要至少一个equi-join条件和一个join两边都包含的时间限定join条件.范围判断可以定义成就像一个条件(<, <=, >=, >),也可以是一个BETWEEN条件,或者两边表的一个相同类型(即:处理时间 或 事件时间)的时间属性[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})的等式判断.


例如:如果订单是在被接收到4小时后发货,这个查询会把所有订单和它们相应的shipments join起来.

```sql
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time
```

下面的判断是有效的interval join条件:

- `ltime = rtime`
- `ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE`
- `ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND`


对于流式查询,对比regular join,interval join 只支持有时间属性的append-only表.
由于时间属性是递增的,Flink从状态中移除旧值也不会影响结果的正确性.

Temporal Joins
--------------

Temporal table是通过时间演进的表 - Flink中称为[动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}).行数据与temporal表中与一个或多个时态时间段相关联,所有Flink中的表都是时态的(动态的).
Temporal table包含一个或多个版本表的快照,它可以是一个追踪变更的变更历史表(例如数据库变更日志,包含所有快照),也可以是一个实现变更的维表(dimensioned table)(例如存放最终快照的数据表)

### Event Time Temporal Join

事件时间Temporal join允许对版本表[versioned table]({{< ref "docs/dev/table/concepts/versioned_tables" >}})进行join.
这意味着一个表可以使用变化的元数据来丰富,并在某个时间点用来检索.

Temporal Joins使用任意表(左侧输入/探针侧)的每一行与versioned table中对应的行进行关联(右侧输入/构建端).
Flink使用`SQL:2011标准`中的`FOR SYSTEM_TIME AS OF`语法去执行操作.
Temporal join的句法如下:


```sql
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
```

使用事件时间属性(即:rowtime属性),可以取得过去某个时间点的值.
这允许两个表在一段共有的时间点连接.
Versioned table将保存从上次水位线以来的所有版本(按时间标识).


例如:假设我们有一个订单表,每个订单都有不同货币的价格.
为了正确地将该表统一为单一货币(如美元),每个订单都需要与下单时相应的汇率相关联.

```sql
-- 创建一个订单表,这是一个标准的append-only动态表
CREATE TABLE orders (
    order_id    STRING,
    price       DECIMAL(32,2),
    currency    STRING,
    order_time  TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time
) WITH (/* ... */);

-- 定义一个汇率的versioned table.
-- 它可能来自CDC
-- 比如说Debezium,kafka消息,或者任何其他的来源定义的版本表
CREATE TABLE currency_rates (
    currency STRING,
    conversion_rate DECIMAL(32, 2),
    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,
    WATERMARK FOR update_time AS update_time,
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
**注意:** 事件时间temporal join是通过左和右两侧的watermark触发的;
请确保join两边设置了正确的watermark.

**注意:** 事件时间temporal join需要包含主键相等的条件,即:`currency_rates`表的主键 `currency_rates.currency`包含在条件`orders.currency = currency_rates.currency`中.


与[regular joins](#regular-joins)相比,就算build side(例子中的currency_rates表)发生变更了,之前的temporal table的结果也不会被影响.
与[interval joins](#interval-joins)的对比,temporal join没有定义join的时间窗口.
Probe side(例子中的orders表)的记录总是在time属性指定的时间与build side的版本行进行连接.因此,build side表的行可能已经过时了.
随着时间的推移,不再被需要的记录版本(对于给定的主键)将从状态中删除.

### Processing Time Temporal Join

处理时间temporal join使用处理时间属性将数据与外部版本表(例如mysql,hbase)的最新版本相关联.

通过定义一个处理时间属性,这个join将总是返回最新的值.可以将build side中被查找的表想象成一个存储所有记录简单的HashMap<K,V>.
这种join的强大之处在于,当无法在Flink中将表具体化为动态表时,它允许Flink直接针对外部系统工作.

下面这个处理时间temporal join示例展示了一个append-only表`orders`与`LatestRates`表进行join.
`LatestRates` 是一个最新汇率的维表(dismension table)(比如Hbase表),在`10:15`, `10:30`, `10:52`这些时间,`LatestRates`表的数据看起来是这样的:

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
Euro        116     <==== 由114 变更为 116
Yen           1
```

`LastestRates`表的数据在`10:15`和`10:30`是相同的.


`Orders`表是一个代表支付了指定`amount`和`currency`的append-only表.
例如:在 `10:15` ,有一个金额为'2 Euro'的order.

```sql
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
```

给出下面这些表,我们希望所有 `Orders`表的记录转换为一个统一的货币. 

```text
amount currency     rate   amount*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
```

目前,temporal join还不支持与 任意view/table 的最新版本join时使用`FOR SYSTEM_TIME AS OF` 语法.可以像下面这样使用temporal table function语法来实现(时态表函数):

```sql
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
```

**注意** Temporal join不支持与 table/view 的最新版本进行join时使用“FOR SYSTEM_TIME AS OF”语法是出于语义考虑,因为左流的连接处理不会等待temporal table的完整快照,这可能会误导生产环境中的用户.处理时间temporal join使用temporal table function也存在相同的语义问题,但它已经存在了很长时间,因此我们从兼容性的角度支持它.

processing-time的结果是不确定的.
processing-time temporal join常常用在使用外部系统来丰富流的数据.(例如维表)

与[regular joins](#regular-joins)的差异,就算build side(例子中的currency_rates表)发生变更了,之前的temporal table结果也不会被影响.
与[interval joins](#interval-joins)的差异,temporal join没有定义数据连接的时间窗口.即:老数据没存储在状态中.

### Temporal Table Function Join

使用[temporal table function]({{< ref "docs/dev/table/concepts/temporal_table_function" >}})去join表的语法和[Table Function](#table-function)相同.


注意:目前只支持inner join和left outer join.


假设`Rates`是一个temporal table function,这个join在SQL中可以被表达为:

```sql
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
```

上述temporal table DDL和temporal table function的主要区别在于：

- SQL可以定义temporal table DDL,但不能定义temporal table函数;
- temporal table DDL和temporal table function都支持temporal join版本表,但只有temporal table function可以temporal join任何表/视图的最新版本.

Lookup Join
--------------

lookup join通常用于使用从外部系统查询的数据来丰富表.join要求一个表具有处理时间属性,另一个表由查找源连接器(lookup source connnector)支持.

The lookup join uses the above [Processing Time Temporal Join](#processing-time-temporal-join) syntax with the right table to be backed by a lookup source connector.

查找连接和上面的[Processing Time Temporal Join](#processing-time-temporal-join) 语法相同,右表使用查找源连接器支持.

The following example shows the syntax to specify a lookup join.

下面的例子展示了lookup join的语法.

```sql
-- Customers 由JDBC连接器支持,可用于lookup joins
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

-- 用客户信息丰富每个订单
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id;
```

在上面的示例中,Orders表由保存在MySQL数据库中的Customers表数据来丰富.带有后续process time属性的`FOR SYSTEM_TIME AS OF`子句确保在联接运算符处理`Orders`行时,`Orders`的每一行都与与join条件匹配的Customer行连接.它还防止在未来连接的`Customer`表的行发生更新时变更连接结果.lookup join还需要一个强制的相等连接条件,在上面的示例中是`o.customer_id=c.id`.

Array Expansion
--------------

返回给定数组中每个元素的新行.尚不支持`Unnesting` `WITH ORDINALITY` .

```sql
SELECT order_id, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
```

Table Function
--------------

将表与表函数的结果联接.左侧(外部)表的每一行都与表函数的相应调用产生的所有行相连接.[用户定义表函数]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions)必须在使用前注册.

### INNER JOIN

如果表函数调用返回空结果,则左(外部)表删除该行.

```sql
SELECT order_id, res
FROM Orders,
LATERAL TABLE(table_func(order_id)) t(res)
```

### LEFT OUTER JOIN

如果表函数调用返回了空结果,则保留相应的外部行,并用空值填充结果.当前,针对lateral table的的left outer join需要ON子句中有一个固定的TRUE连接条件.

```sql
SELECT order_id, res
FROM Orders
LEFT OUTER JOIN LATERAL TABLE(table_func(order_id)) t(res)
  ON TRUE
```

{{< top >}}
