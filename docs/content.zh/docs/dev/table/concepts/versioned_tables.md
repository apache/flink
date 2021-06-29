---
title: "时态表（Temporal Tables）"
weight: 4
type: docs
aliases:
  - /zh/dev/table/streaming/versioned_tables.html
  - /zh/dev/table/streaming/temporal_tables.html
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

# 时态表（Temporal Tables）

时态表（Temporal Table）是一张随时间变化的表 -- 在 Flink 中称为[动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}})，时态表中的每条记录都关联了一个或多个时间段，所有的 Flink 表都是时态的（动态的）。

时态表包含表的一个或多个有版本的表快照，时态表可以是一张跟踪所有变更记录的表（例如数据库表的 changelog，包含多个表快照），也可以是物化所有变更之后的表（例如数据库表，只有最新表快照）。

**版本**: 时态表可以划分成一系列带版本的表快照集合，表快照中的版本代表了快照中所有记录的有效区间，有效区间的开始时间和结束时间可以通过用户指定，根据时态表是否可以追踪自身的历史版本与否，时态表可以分为 `版本表` 和 `普通表`。 

**版本表**: 如果时态表中的记录可以追踪和并访问它的历史版本，这种表我们称之为版本表，来自数据库的 changelog 可以定义成版本表。
 
**普通表**: 如果时态表中的记录仅仅可以追踪并和它的最新版本，这种表我们称之为普通表，来自数据库 或 HBase 的表可以定义成普通表。



设计初衷
----------

### 关联一张版本表

以订单流关联产品表这个场景举例，`orders` 表包含了来自 Kafka 的实时订单流，`product_changelog` 表来自数据库表 `products` 的 changelog , 产品的价格在数据库表 `products` 中是随时间实时变化的。

```sql
SELECT * FROM product_changelog;

(changelog kind)  update_time  product_id product_name price
================= ===========  ========== ============ ===== 
+(INSERT)         00:01:00     p_001      scooter      11.11
+(INSERT)         00:02:00     p_002      basketball   23.11
-(UPDATE_BEFORE)  12:00:00     p_001      scooter      11.11
+(UPDATE_AFTER)   12:00:00     p_001      scooter      12.99
-(UPDATE_BEFORE)  12:00:00     p_002      basketball   23.11 
+(UPDATE_AFTER)   12:00:00     p_002      basketball   19.99
-(DELETE)         18:00:00     p_001      scooter      12.99 
```

表 `product_changelog` 表示数据库表 `products`不断增长的 changelog, 比如，产品 `scooter` 在时间点 `00:01:00`的初始价格是 `11.11`, 在 `12:00:00` 的时候涨价到了 `12.99`, 
在 `18:00:00` 的时候这条产品价格记录被删除。
 
如果我们想输出 `product_changelog` 表在 `10:00:00` 对应的版本，表的内容如下所示：
```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
```

如果我们想输出 `product_changelog` 表在 `13:00:00` 对应的版本，表的内容如下所示：
```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
```

上述例子中，`products` 表的版本是通过 `update_time` 和 `product_id` 进行追踪的，`product_id` 对应 `product_changelog` 表的主键，`update_time` 对应事件时间。

在 Flink 中, 这由[*版本表*](#声明版本表)表示。

### 关联一张普通表

另一方面，某些用户案列需要连接变化的维表，该表是外部数据库表。

假设 `LatestRates` 是一个物化的最新汇率表 (比如：一张 HBase 表)，`LatestRates` 总是表示 HBase 表 `Rates` 的最新内容。

我们在 `10:15:00` 时查询到的内容如下所示：
```sql
10:15:00 > SELECT * FROM LatestRates;

currency  rate
========= ====
US Dollar 102
Euro      114
Yen       1
```

我们在 `11:00:00` 时查询到的内容如下所示：
```sql
11:00:00 > SELECT * FROM LatestRates;

currency  rate
========= ====
US Dollar 102
Euro      116
Yen       1
```

在 Flink 中, 这由[*普通表*](#声明普通表)表示。

时态表
-----
<span class="label label-danger">注意</span> 仅 Blink planner 支持此功能。

Flink 使用主键约束和事件时间来定义一张版本表和版本视图。

### 声明版本表
在 Flink 中，定义了主键约束和事件时间属性的表就是版本表。
```sql
-- 定义一张版本表
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) 定义主键约束
  WATERMARK FOR update_time AS update_time   -- (2) 通过 watermark 定义事件时间              
) WITH (
  'connector' = 'kafka',
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);
```

行 `(1)` 为表 `product_changelog` 定义了主键, 行 `(2)` 把 `update_time` 定义为表 `product_changelog` 的事件时间，因此 `product_changelog` 是一张版本表。

**注意**: `METADATA FROM 'value.source.timestamp' VIRTUAL` 语法的意思是从每条 changelog 中抽取 changelog 对应的数据库表中操作的执行时间，强烈推荐使用数据库表中操作的
执行时间作为事件时间 ，否则通过时间抽取的版本可能和数据库中的版本不匹配。
 
### 声明版本视图

Flink 也支持定义版本视图只要一个视图包含主键和事件时间便是一个版本视图。

假设我们有表 `RatesHistory` 如下所示：
```sql
-- 定义一张 append-only 表
CREATE TABLE RatesHistory (
    currency_time TIMESTAMP(3),
    currency STRING,
    rate DECIMAL(38, 10),
    WATERMARK FOR currency_time AS currency_time   -- 定义事件时间
) WITH (
  'connector' = 'kafka',
  'topic' = 'rates',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'                                -- 普通的 append-only 流
)
```

表 `RatesHistory` 代表一个兑换日元货币汇率表（日元汇率为1），该表是不断增长的 append-only 表。
例如，`欧元` 兑换 `日元` 从 `09:00:00` 到 `10:45:00` 的汇率为 `114`。从 `10:45:00` 到 `11:15:00` 的汇率为 `116`。

```sql
SELECT * FROM RatesHistory;

currency_time currency  rate
============= ========= ====
09:00:00      US Dollar 102
09:00:00      Euro      114
09:00:00      Yen       1
10:45:00      Euro      116
11:15:00      Euro      119
11:49:00      Pounds    108
```

为了在 `RatesHistory` 上定义版本表，Flink 支持通过[去重查询]({{< ref "docs/dev/table/sql/queries/deduplication" >}}#去重)定义版本视图，
去重查询可以产出一个有序的 changelog 流，去重查询能够推断主键并保留原始数据流的事件时间属性。

```sql
CREATE VIEW versioned_rates AS              
SELECT currency, rate, currency_time            -- (1) `currency_time` 保留了事件时间
  FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY currency  -- (2) `currency` 是去重 query 的 unique key，可以作为主键
         ORDER BY currency_time DESC) AS rowNum 
      FROM RatesHistory )
WHERE rowNum = 1; 

-- 视图 `versioned_rates` 将会产出如下的 changelog:

(changelog kind) currency_time currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      US Dollar  102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      Yen        1
+(UPDATE_AFTER)  10:45:00      Euro       116
+(UPDATE_AFTER)  11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
{% endhighlight sql %}

行 `(1)` 保留了事件时间作为视图 `versioned_rates` 的事件时间，行 `(2)` 使得视图 `versioned_rates` 有了主键, 因此视图 `versioned_rates` 是一个版本视图。

视图中的去重 query 会被 Flink 优化并高效地产出 changelog stream, 产出的 changelog 保留了主键约束和事件时间。
 
如果我们想输出 `versioned_rates` 表在 `11:00:00` 对应的版本，表的内容如下所示：
```sql
currency_time currency   rate  
============= ========== ====
09:00:00      US Dollar  102
09:00:00      Yen        1
10:45:00      Euro       116
```

如果我们想输出 `versioned_rates` 表在 `12:00:00` 对应的版本，表的内容如下所示：
```sql
currency_time currency   rate  
============= ========== ====
09:00:00      US Dollar  102
09:00:00      Yen        1
10:45:00      Euro       119
11:49:00      Pounds     108
```

### 声明普通表
 
普通表的声明和 Flink 建表 DDL 一致，参考 [create table]({{< ref "docs/dev/table/sql/create" >}}#create-table) 页面获取更多如何建表的信息。
 
```sql
-- 用 DDL 定义一张 HBase 表，然后我们可以在 SQL 中将其当作一张时态表使用
-- 'currency' 列是 HBase 表中的 rowKey
 CREATE TABLE LatestRates (   
     currency STRING,   
     fam1 ROW<rate DOUBLE>   
 ) WITH (   
    'connector' = 'hbase-1.4',   
    'table-name' = 'rates',   
    'zookeeper.quorum' = 'localhost:2181'   
 );
```

<span class="label label-danger">注意</span> 理论上讲任意都能用作时态表并在基于处理时间的时态表 Join 中使用，但当前支持作为时态表的普通表必须实现接口 `LookupableTableSource`。接口 `LookupableTableSource` 的实例只能作为时态表用于基于处理时间的时态 Join 。

通过 `LookupableTableSource` 定义的表意味着该表具备了在运行时通过一个或多个 key 去查询外部存储系统的能力，当前支持在 基于处理时间的时态表 join 中使用的表包括
[JDBC]({{< ref "docs/connectors/table/jdbc" >}}), [HBase]({{< ref "docs/connectors/table/hbase" >}}) 和 [Hive]({{< ref "docs/connectors/table/hive/hive_read_write" >}}#temporal-table-join)。

另请参阅 [LookupableTableSource]({{< ref "docs/dev/table/sourcesSinks" >}}#lookup-table-source)页面了解更多信息。

在基于处理时间的时态表 Join 中支持任意表作为时态表会在不远的将来支持。

时态表函数
------------------------
时态表函数是一种过时的方式去定义时态表并关联时态表的数据，现在我们可以用时态表 DDL 去定义时态表，用[时态表 Join]({{< ref "docs/dev/table/sql/queries/joins" >}}#时态表-join) 语法去关联时态表。

时态表函数和时态表 DDL 最大的区别在于，时态表 DDL 可以在纯 SQL 环境中使用但是时态表函数不支持，用时态表 DDL 声明的时态表支持 changelog 流和 append-only 流但时态表函数仅支持 append-only 流。
 
为了访问时态表中的数据，必须传递一个[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})，该属性确定将要返回的表的版本。
Flink 使用[表函数]({{< ref "docs/dev/table/functions/udfs" >}}#表值函数)的 SQL 语法提供一种表达它的方法。

定义后，*时态表函数*将使用单个时间参数 timeAttribute 并返回一个行集合。
该集合包含相对于给定时间属性的所有现有主键的行的最新版本。

假设我们基于 `RatesHistory` 表定义了一个时态表函数，我们可以通过以下方式查询该函数 `Rates(timeAttribute)`： 

```sql
SELECT * FROM Rates('10:15:00');

rowtime  currency  rate
=======  ========= ====
09:00:00 US Dollar 102
09:00:00 Euro      114
09:00:00 Yen       1

SELECT * FROM Rates('11:00:00');

rowtime  currency  rate
======== ========= ====
09:00:00 US Dollar 102
10:45:00 Euro      116
09:00:00 Yen       1
```

对 `Rates(timeAttribute)` 的每个查询都将返回给定 `timeAttribute` 的 `Rates` 状态。

**注意**：当前 Flink 不支持使用常量时间属性参数直接查询时态表函数。目前，时态表函数只能在 join 中使用。上面的示例用于为函数 `Rates(timeAttribute)` 返回内容提供直观信息。

另请参阅有关[用于持续查询的 join ]({{< ref "docs/dev/table/sql/queries/joins" >}})页面，以获取有关如何与时态表 join 的更多信息。

### 定义时态表函数

以下代码段说明了如何从 append-only 表中创建时态表函数。

{{< tabs "53d51b01-eee7-49b7-965d-98ab237fb3a1" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

行`(1)`创建了一个 `rates` [时态表函数](#时态表函数)，
这使我们可以在[ Table API ]({{< ref "docs/dev/table/tableApi" >}}#joins)中使用 `rates` 函数。

行`(2)`在表环境中注册名称为 `Rates` 的函数，这使我们可以在[ SQL ]({{< ref "docs/dev/table/sql/queries/joins" >}})中使用 `Rates` 函数。

{{< top >}}
