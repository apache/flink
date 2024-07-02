---
title: 语法说明
weight: 2
type: docs
aliases:
- /dev/table/materialized-table/syntax.html
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

# 说明

这篇文档解释了物化表的语法和使用方法。

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

## 必须参数

### FRESHNESS

**FRESHNESS 定义与刷新模式关系**

FRESHNESS 定义物化表相比上游数据的滞后程度，会作为该表的数据新鲜度。 FRESHNESS 通过[配置]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold) 决定了物化表的[刷新模式]({{< ref "docs/dev/table/materialized-table/overview" >}}#刷新模式)，另一方面也决定了数据的刷新频率来满足实际的数据新鲜度要求。

**FRESHNESS 参数详解**

FRESHNESS 参数的取值范围为 INTERVAL `'<num>'` { SECOND | MINUTE | HOUR | DAY }。`'<num>'` 必须为正整数，并且在 FULL 模式下，`'<num>'` 应为对应时间区间范围的公约数。

**示例：**
（假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟）

```sql
-- 对应的刷新任务是一个实时作业，对应的 checkpoint 间隔为 1 秒
FRESHNESS = INTERVAL '1' SECOND

-- 对应的刷新任务是一个实时作业，对应的 checkpoint 间隔为 1 分钟
FRESHNESS = INTERVAL '1' MINUTE

-- 对应的刷新任务是一个定时调度工作流，对应的调度周期为 1 小时
FRESHNESS = INTERVAL '1' HOUR

-- 对应的刷新任务是一个定时调度工作流，对应的调度周期为 1 天
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

-- FULL 模式下，间隔不是对应时间范围的公约数
FRESHNESS = INTERVAL '60' SECOND
FRESHNESS = INTERVAL '5' HOUR
```

<span class="label label-danger">注意</span>
- 框架在执行时会尽量满足定义的数据新鲜度需求，但无法保证完全满足。
- 在 FULL 模式下，由于数据新鲜度需要转换为 cron 表达式，因此只支持特定时间范围内的公约数。例如，对于秒级别支持 30、15、10、5、2 和 1 秒；分钟级别支持 30、15、10、5、2 和 1 分钟；小时级别支持 8、4、2 和 1 小时；而天级别目前仅支持 1 天。

### AS <select_statement>

该子句使用来自 select 查询的数据填充物化表。上游表可以是物化表、表或视图

**示例：**

```sql
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS SELECT * FROM kafka_catalog.db1.kafka_table;
```

## 可选参数

### REFRESH_MODE

REFRESH_MODE 用于显式指定物化表的刷新模式。指定的模式优先级高于框架自动推导，以满足特定场景的需求。

**示例：**
(假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟)

```sql
-- 创建的物化表的刷新模式为 CONTINUOUS 模式，且作业的 checkpoint 周期为 1 小时。
CREATE MATERIALIZED TABLE my_materialized_table
    REFRESH_MODE = CONTINUOUS
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT
       ...    

-- 创建的物化表的刷新模式为 FULL 模式，且作业的调度周期为 10 min。
CREATE MATERIALIZED TABLE my_materialized_table
    REFRESH_MODE = FULL
    FRESHNESS = INTERVAL '10' MINUTE
    AS SELECT
       ...    
```
### PRIMARY KEY

PRIMARY KEY 用于指定物化表的主键约束。

### PARTITIONED BY

PARTITIONED BY 用于指定物化表的分区字段。

**示例：**

```sql
--  创建的物化表并指定分区字段为 `ds`。
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT 
        ds
    FROM
        ...
```

<span class="label label-danger">注意</span>
- 分区字段必须为物化表的查询语句中的字段。

### WITH Options

WITH Options 用于指定创建物化表的选项，包括[连接器参数]({{< ref "docs/connectors/table/" >}})和分区字段的[时间格式参数]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter)。

```sql
-- 创建一个物化表，指定分区字段为 'ds' ， 对应的时间格式为 'yyyy-MM-dd'
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    ...
```
如上例所示，我们指定了 ds 分区字段的 date-formatter 参数。在实际调度时，将调度时间转换为 ds 分区值。例如，在 2024-01-01 00:00:00 的调度时间下，只会更新具有 ds='2024-01-01' 分区值的分区。

<span class="label label-danger">注意</span>
- [时间格式参数]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 中的字段必须是一个合法的分区字段，且必须为字符串类型。

## 示例

创建一个数据新鲜度为 10 秒的物化表，刷新模式为 CONTINUOUS ：

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

创建一个数据新鲜度为 10 分钟的物化表，刷新模式为 FULL ：

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

## 限制说明

- 不支持显示指定列
- 不支持修改的查询语句
- 不支持在 select 查询中使用临时表，临时视图，临时函数

# ALTER MATERIALIZED TABLE

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND | RESUME [WITH (key1=val1, key2=val2, ...)] | REFRESH [PARTITION partition_spec]
```

ALTER MATERIALIZED TABLE 用于对物化表进行管理。通过此命令，用户可以暂停和恢复物化表的刷新任务，以及手动触发数据刷新。

## SUSPEND

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND
```

SUSPEND 用于暂停物化表的刷新任务。

**示例:** 

```sql
-- 暂停前指定 SAVEPOINT 路径
SET 'execution.checkpointing.savepoint-dir' = 'hdfs://savepoint_path';

-- 暂停指定的物化表
ALTER MATERIALIZED TABLE my_materialized_table SUSPEND;
```

<span class="label label-danger">注意</span>
- 暂停 CONTINUOUS 模式的表时，默认会使用 STOP WITH SAVEPOINT 暂停作业，需要使用[参数]({{< ref "docs/deployment/config" >}}#execution-checkpointing-savepoint-dir)设置 SAVEPOINT 保存路径。

## RESUME

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESUME [WITH (key1=val1, key2=val2, ...)]
```

RESUME 用于恢复物化表的刷新任务。

**示例:** 

```sql
-- 恢复指定的物化表
ALTER MATERIALIZED TABLE my_materialized_table RESUME;

-- 恢复指定的物化表并指定 sink 并发
ALTER MATERIALIZED TABLE my_materialized_table RESUME WITH ('sink.parallelism'='10');
```

## REFRESH

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name REFRESH [PARTITION partition_spec]
```

REFRESH 用于主动触发物化表的刷新。

**示例:**

```sql
-- 刷新全表数据
ALTER MATERIALIZED TABLE my_materialized_table REFRESH;

-- 刷新指定分区数据
ALTER MATERIALIZED TABLE my_materialized_table REFRESH PARTITION (ds='2024-06-28');
```

<span class="label label-danger">注意</span>
- REFRESH 操作会启动批作业来刷新表的数据。在 CONTINUOUS 模式下，需要先暂停物化表，以避免数据不一致的问题。

## 限制说明

- 当前不支持修改物化表的刷新模式、数据新鲜度、分区字段等属性，也无法更改物化表对应的查询定义。

# DROP MATERIALIZED TABLE

```text
DROP MATERIALIZED TABLE [IF EXISTS] [catalog_name.][database_name.]table_name
```

删除物化表时，会先停止后台的刷新任务，然后从 Catalog 中删除该物化表对应的元数据。

**示例:**

```sql
-- 删除指定的物化表
DROP MATERIALIZED TABLE IF EXISTS my_materialized_table;
```




