---
title: 语法说明
weight: 2
type: docs
aliases:
- /dev/table/materialized-table/statements.html
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

# 物化表语法

Flink SQL 目前支持以下物化表操作：
- [CREATE MATERIALIZED TABLE](#create-materialized-table)
- [ALTER MATERIALIZED TABLE](#alter-materialized-table)
- [DROP MATERIALIZED TABLE](#drop-materialized-table)

# CREATE MATERIALIZED TABLE

```
CREATE MATERIALIZED TABLE [catalog_name.][db_name.]table_name
 
[ ([ <table_constraint> ]) ]
 
[COMMENT table_comment]
 
[PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
 
[WITH (key1=val1, key2=val2, ...)]
 
FRESHNESS = INTERVAL '<num>' { SECOND | MINUTE | HOUR | DAY }
 
[REFRESH_MODE = { CONTINUOUS | FULL }]
 
AS <select_statement>
 
<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED
```

## PRIMARY KEY

`PRIMARY KEY` 定义了一组可选的列，用于唯一标识表中的每一行。主键列必须非空。

## PARTITIONED BY

`PARTITIONED BY` 定义了一组可选的列，用于对物化表进行分区。如果物化表使用了 `filesystem connector` ，每个分区将创建一个目录。

**示例：**

```sql
-- 创建一个物化表并将分区字段设置为 `ds`。
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT 
        ds
    FROM
        ...
```

<span class="label label-danger">注意</span>
- 分区字段必须是物化表查询语句中的字段。

## WITH Options

`WITH Options` 可以定义创建物化表所需的属性，包括[连接器参数]({{< ref "docs/connectors/table/" >}})和分区字段的[时间格式参数]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter)。

```sql
-- 创建一个物化表，指定分区字段为 'ds' 和对应的时间格式为 'yyyy-MM-dd'
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    ...
```

如上例所示，我们为 `ds` 分区列指定了 `date-formatter` 选项。每次调度时，调度时间将转换为相应的 `ds` 分区值。例如，在 `2024-01-01 00:00:00` 的调度时间下，只有分区 `ds = '2024-01-01'` 会被刷新。

<span class="label label-danger">注意</span>
- [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 选项仅适用于全量模式。
- [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 中的字段必须是有效的字符串类型分区字段。

## FRESHNESS

`FRESHNESS` 用于指定物化表的数据新鲜度。

**数据新鲜度与刷新模式关系**

数据新鲜度定义了物化表内容滞后于基础表更新的最长时间。它有两个作用，首先通过[配置]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold)确定物化表的[刷新模式]({{< ref "docs/dev/table/materialized-table/overview" >}}#刷新模式)，然后确定数据刷新频率以满足实际数据新鲜度要求。

**FRESHNESS 参数详解**

FRESHNESS 参数的取值范围为 INTERVAL `'<num>'` { SECOND | MINUTE | HOUR | DAY }。`'<num>'` 必须为正整数，并且在全量模式下，`'<num>'` 应该是相应时间间隔单位的公约数。

**示例：**
（假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟）

```sql
-- 对应的刷新管道是一个 checkpoint 间隔为 1 秒的流处理作业
FRESHNESS = INTERVAL '1' SECOND

-- 对应的刷新管道是一个 checkpoint 间隔为 1 分钟的流处理作业
FRESHNESS = INTERVAL '1' MINUTE

-- 对应的刷新管道是一个调度周期为 1 小时的调度工作流
FRESHNESS = INTERVAL '1' HOUR

-- 对应的刷新管道是一个调度周期为 1 天的调度工作流
FRESHNESS = INTERVAL '1' DAY
```

**不合法的 `FRESHNESS` 示例：**

```sql
-- 间隔为负数
FRESHNESS = INTERVAL '-1' SECOND

-- 间隔为0
FRESHNESS = INTERVAL '0' SECOND

-- 间隔为月或者年
FRESHNESS = INTERVAL '1' MONTH
FRESHNESS = INTERVAL '1' YEAR

-- 全量模式下，间隔不是对应时间间隔单位的公约数
FRESHNESS = INTERVAL '60' SECOND
FRESHNESS = INTERVAL '5' HOUR
```

<span class="label label-danger">注意</span>
- 尽管物化表数据将尽可能在定义的新鲜度内刷新，但不能保证完全满足新鲜度要求。
- 在连续模式下，数据新鲜度和 `checkpoint` 间隔一致，设置过短的数据新鲜度可能会对作业性能产生影响。此外，为了优化 `checkpoint` 性能，建议[开启 Changelog]({{< ref "docs/ops/state/state_backends" >}}#开启-changelog)。
- 在全量模式下，数据新鲜度会转换为 `cron` 表达式，因此目前仅支持在预定义时间间隔单位内的新鲜度间隔，这种设计确保了与 `cron` 表达式语义的一致性。具体支持以下新鲜度间隔：
  - 秒：30、15、10、5、2 和 1 秒间隔。
  - 分钟：30、15、10、5、2 和 1 分钟间隔。
  - 小时：8、4、2 和 1 小时间隔。
  - 天：1 天。

## REFRESH_MODE

`REFRESH_MODE` 用于显式指定物化表的刷新模式。指定的刷新模式比框架自动推导的模式具有更高的优先级，以满足特定场景的需求。

**示例：**
(假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟)

```sql
-- 创建的物化表的刷新模式为连续模式，作业的 checkpoint 间隔为 1 小时。
CREATE MATERIALIZED TABLE my_materialized_table
    REFRESH_MODE = CONTINUOUS
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT
       ...    

-- 创建的物化表的刷新模式为全量模式，作业的调度周期为 10 分钟。
CREATE MATERIALIZED TABLE my_materialized_table
    REFRESH_MODE = FULL
    FRESHNESS = INTERVAL '10' MINUTE
    AS SELECT
       ...    
```

## AS <select_statement>

该子句用于定义填充物化表数据的查询。上游表可以是物化表、表或视图。select 语句支持所有 Flink SQL [查询]({{< ref "docs/dev/table/sql/queries/overview" >}})。

**示例：**

```sql
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS SELECT * FROM kafka_catalog.db1.kafka_table;
```

## 示例

（假设 materialized-table.refresh-mode.freshness-threshold 为 30 分钟）

创建一个数据新鲜度为 `10` 秒的物化表，推导出的刷新模式为连续模式:

```sql
CREATE MATERIALIZED TABLE my_materialized_table_continuous
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    FRESHNESS = INTERVAL '10' SECOND
    REFRESH_MODE = 'CONTINUOUS'
    AS 
    SELECT 
        k.ds,
        k.user_id,
        COUNT(*) AS event_count,
        SUM(k.amount) AS total_amount,
        MAX(u.age) AS max_age
    FROM 
        kafka_catalog.db1.kafka_table k
    JOIN 
        user_catalog.db1.user_table u
    ON 
        k.user_id = u.user_id
    WHERE 
        k.event_type = 'purchase'
    GROUP BY 
        k.ds, k.user_id
```

创建一个数据新鲜度为 `1` 小时的物化表，推导出的刷新模式为全量模式:

```sql
CREATE MATERIALIZED TABLE my_materialized_table_full
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    FRESHNESS = INTERVAL '10' MINUTE
    REFRESH_MODE = 'FULL'
    AS 
    SELECT 
        p.ds,
        p.product_id,
        p.product_name,
        AVG(s.sale_price) AS avg_sale_price,
        SUM(s.quantity) AS total_quantity
    FROM 
        paimon_catalog.db1.product_table p
    LEFT JOIN 
        paimon_catalog.db1.sales_table s
    ON 
        p.product_id = s.product_id
    WHERE 
        p.category = 'electronics'
    GROUP BY 
        p.ds, p.product_id, p.product_name
```

## 限制
- 不支持显式指定列
- 不支持修改查询语句
- 不支持在 select 查询中使用临时表、临时视图或临时函数

# ALTER MATERIALIZED TABLE

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND | RESUME [WITH (key1=val1, key2=val2, ...)] | REFRESH [PARTITION partition_spec]
```

`ALTER MATERIALIZED TABLE` 用于管理物化表。用户可以使用此命令暂停和恢复物化表的刷新管道，并手动触发数据刷新。


## SUSPEND

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND
```

`SUSPEND` 用于暂停物化表的后台刷新管道。

**示例:** 

```sql
-- 暂停前指定 SAVEPOINT 路径
SET 'execution.checkpointing.savepoint-dir' = 'hdfs://savepoint_path';

-- 暂停指定的物化表
ALTER MATERIALIZED TABLE my_materialized_table SUSPEND;
```

<span class="label label-danger">注意</span>
- 暂停连续模式的表时，默认会使用 `STOP WITH SAVEPOINT` 暂停作业，你需要使用[参数]({{< ref "docs/deployment/config" >}}#execution-checkpointing-savepoint-dir)设置 `SAVEPOINT` 保存路径。

## RESUME

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESUME [WITH (key1=val1, key2=val2, ...)]
```

`RESUME` 用于恢复物化表的刷新管道。在恢复时，可以通过 `WITH` 子句动态指定物化表的参数，该参数仅对当前恢复的刷新管道生效，并不会持久化到物化表中。

**示例:** 

```sql
-- 恢复指定的物化表
ALTER MATERIALIZED TABLE my_materialized_table RESUME;

-- 恢复指定的物化表并指定 sink 的并行度
ALTER MATERIALIZED TABLE my_materialized_table RESUME WITH ('sink.parallelism'='10');
```

## REFRESH

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name REFRESH [PARTITION partition_spec]
```

`REFRESH` 用于主动触发物化表的刷新。

**示例:**

```sql
-- 刷新全表数据
ALTER MATERIALIZED TABLE my_materialized_table REFRESH;

-- 刷新指定分区数据
ALTER MATERIALIZED TABLE my_materialized_table REFRESH PARTITION (ds='2024-06-28');
```

<span class="label label-danger">注意</span>
- REFRESH 操作会启动批作业来刷新表的数据。

# DROP MATERIALIZED TABLE

```text
DROP MATERIALIZED TABLE [IF EXISTS] [catalog_name.][database_name.]table_name
```

删除物化表时，首先删除后台刷新管道，然后从 Catalog 中移除该物化表的元数据。

**示例:**

```sql
-- 删除指定的物化表
DROP MATERIALIZED TABLE IF EXISTS my_materialized_table;
```




