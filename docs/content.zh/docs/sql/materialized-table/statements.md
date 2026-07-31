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
- [CREATE [OR ALTER] MATERIALIZED TABLE](#create-or-alter-materialized-table)
- [ALTER MATERIALIZED TABLE](#alter-materialized-table)
- [DROP MATERIALIZED TABLE](#drop-materialized-table)

# CREATE [OR ALTER] MATERIALIZED TABLE

```text
CREATE [OR ALTER] MATERIALIZED TABLE [catalog_name.][db_name.]table_name

[(
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
)]

[COMMENT table_comment]

[ <distribution> ]

[PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]

[WITH (key1=val1, key2=val2, ...)]

[FRESHNESS = INTERVAL '<num>' { SECOND[S] | MINUTE[S] | HOUR[S] | DAY[S] }]

[REFRESH_MODE = { CONTINUOUS | FULL }]

AS <select_statement>

<physical_column_definition>:
  column_name column_type [ <column_constraint> ] [COMMENT column_comment]
  
<column_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY NOT ENFORCED

<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<metadata_column_definition>:
  column_name column_type METADATA [ FROM metadata_key ] [ VIRTUAL ]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
  
<distribution>:
{
    DISTRIBUTED BY [ { HASH | RANGE } ] (bucket_column_name1, bucket_column_name2, ...) [INTO n BUCKETS]
  | DISTRIBUTED INTO n BUCKETS
}
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

`FRESHNESS` is optional. When omitted, the system uses the default freshness based on the refresh mode: `materialized-table.default-freshness.continuous` (default: 3 minutes) for CONTINUOUS mode, or `materialized-table.default-freshness.full` (default: 1 hour) for FULL mode.

**数据新鲜度与刷新模式关系**

数据新鲜度定义了物化表内容滞后于基础表更新的最长时间。When not specified, it uses the default value from configuration based on the refresh mode. 它有两个作用，首先通过[配置]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold)确定物化表的[刷新模式]({{< ref "docs/sql/materialized-table/overview" >}}#刷新模式)，然后确定数据刷新频率以满足实际数据新鲜度要求。

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

**Default FRESHNESS Example:**
(Assuming `materialized-table.default-freshness.continuous` is 3 minutes, `materialized-table.default-freshness.full` is 1 hour, and `materialized-table.refresh-mode.freshness-threshold` is 30 minutes)

```sql
-- FRESHNESS is omitted, uses the configured default of 3 minutes for CONTINUOUS mode
-- The corresponding refresh pipeline is a streaming job with a checkpoint interval of 3 minutes
CREATE MATERIALIZED TABLE my_materialized_table
    AS SELECT * FROM source_table;

-- FRESHNESS is omitted and FULL mode is explicitly specified, uses the configured default of 1 hour
-- The corresponding refresh pipeline is a scheduled workflow with a schedule cycle of 1 hour
CREATE MATERIALIZED TABLE my_materialized_table_full
    REFRESH_MODE = FULL
    AS SELECT * FROM source_table;
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
- If FRESHNESS is not specified, the table will use the default freshness interval based on the refresh mode: `materialized-table.default-freshness.continuous` (default: 3 minutes) for CONTINUOUS mode, or `materialized-table.default-freshness.full` (default: 1 hour) for FULL mode.
- 尽管物化表数据将尽可能在定义的新鲜度内刷新，但不能保证完全满足新鲜度要求。
- 在持续模式下，数据新鲜度和 `checkpoint` 间隔一致，设置过短的数据新鲜度可能会对作业性能产生影响。此外，为了优化 `checkpoint` 性能，建议[开启 Changelog]({{< ref "docs/ops/state/state_backends" >}}#开启-changelog)。
- 在全量模式下，数据新鲜度会转换为 `cron` 表达式，因此目前仅支持在预定义时间间隔单位内的新鲜度间隔，这种设计确保了与 `cron` 表达式语义的一致性。具体支持以下新鲜度间隔：
  - 秒：1、2、3、4、5、6、10、12、15、20、30。
  - 分钟：1、2、3、4、5、6、10、12、15、20、30。
  - 小时：1、2、3、4、6、8、12。
  - 天：1。

## REFRESH_MODE

`REFRESH_MODE` 用于显式指定物化表的刷新模式。指定的刷新模式比框架自动推导的模式具有更高的优先级，以满足特定场景的需求。

**示例：**
(假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟)

```sql
-- 创建的物化表的刷新模式为持续模式，作业的 checkpoint 间隔为 1 小时。
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '1' HOUR
    REFRESH_MODE = CONTINUOUS
    AS SELECT
       ...

-- 创建的物化表的刷新模式为全量模式，作业的调度周期为 10 分钟。
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' MINUTE
    REFRESH_MODE = FULL
    AS SELECT
       ...
```

## AS <select_statement>

该子句用于定义填充物化表数据的查询。上游表可以是物化表、表或视图。select 语句支持所有 Flink SQL [查询]({{< ref "docs/sql/reference/queries/overview" >}})。

**示例：**

```sql
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS SELECT * FROM kafka_catalog.db1.kafka_table;
```

## OR ALTER

The `OR ALTER` clause provides create-or-update semantics:

- **If the materialized table does not exist**: Creates a new materialized table with the specified options
- **If the materialized table exists**: Modifies the query definition (behaves like `ALTER MATERIALIZED TABLE AS`)
- **If a regular table with the same name exists**: Converts it in place into a materialized table, when enabled (see [Converting a Table to a Materialized Table](#converting-a-table-to-a-materialized-table))

This is particularly useful in declarative deployment scenarios where you want to define the desired state without checking if the materialized table already exists.

**Behavior when materialized table exists:**

The operation updates the materialized table similarly to [ALTER MATERIALIZED TABLE AS](#as-select_statement-1):

**Full mode:**
1. Updates the schema and query definition
2. The materialized table is refreshed using the new query when the next refresh job is triggered

**Continuous mode:**
1. Pauses the current running refresh job
2. Updates the schema and query definition
3. Starts a new refresh job from the beginning

See [ALTER MATERIALIZED TABLE AS](#as-select_statement-1) for more details.

## Converting a Table to a Materialized Table

This lets you adopt a materialized table on top of a table that already exists, without dropping and recreating it.

`CREATE OR ALTER MATERIALIZED TABLE` can convert an existing regular table into a materialized table in place. The catalog object keeps its identity and underlying storage. Its kind becomes materialized table, and its schema, options, query definition, freshness, and refresh mode are taken from the conversion statement, exactly as for a newly created materialized table. After the conversion, a refresh job is launched just as it is for a newly created materialized table.

**Enabling conversion**

Conversion is disabled by default. It is a one-way operation: it permanently turns a regular table into a materialized table and cannot be undone, because there is no operation that converts a materialized table back into a regular table. Keeping it off by default also preserves source compatibility. A `CREATE OR ALTER MATERIALIZED TABLE` that happens to name an existing regular table keeps its previous behavior of being rejected, so no existing workflow silently changes meaning until you opt in.

When conversion is disabled, `CREATE OR ALTER MATERIALIZED TABLE` against a regular table is rejected. To enable it, set:

```yaml
table.materialized-table.conversion-from-table.enabled: true
```

The option is read at planning time from the session's root configuration, so it must be set when the `TableEnvironment` session is initialized. Set it in the cluster configuration file `config.yaml`, or in the configuration used to create the session. Changing it afterwards with a session-level `SET` statement has no effect.

**Schema**

The schema comes from the `CREATE OR ALTER MATERIALIZED TABLE` statement and its query, exactly as for a brand-new materialized table. Nothing is taken from the source table. These are the same rules `CREATE MATERIALIZED TABLE` already uses.

The examples below read from a source table `orders` and convert an existing regular table `user_spending`:

```sql
-- Source table the query reads from
CREATE TABLE orders (
    user_id BIGINT NOT NULL,
    amount BIGINT,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = '...'
);

-- The existing regular table to convert. It has a primary key and a watermark,
-- which the conversion does not carry over.
CREATE TABLE user_spending (
    user_id BIGINT NOT NULL,
    total_amount BIGINT,
    last_order TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED,
    WATERMARK FOR last_order AS last_order - INTERVAL '5' SECOND
) WITH (
    'connector' = '...'
);
```

With no column list, the schema is exactly the query output:

```sql
CREATE OR ALTER MATERIALIZED TABLE user_spending
    AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;
-- columns: user_id, total_amount
-- the source's last_order column, primary key, and watermark are not carried over
```

To keep a primary key or watermark, re-declare it in the conversion statement:

```sql
CREATE OR ALTER MATERIALIZED TABLE user_spending (
    PRIMARY KEY (user_id) NOT ENFORCED,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) AS SELECT user_id, order_time FROM orders;
-- columns: user_id, order_time (primary key: user_id, watermark on order_time)
```

If you declare columns, every persisted column must be produced by the query. Types do not have to match exactly: the query type just needs to be safely castable to the declared type, for example `INT` to `BIGINT`.

```sql
-- OK: both declared columns are produced by the query
CREATE OR ALTER MATERIALIZED TABLE user_spending (user_id BIGINT, total_amount BIGINT)
    AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;
-- columns: user_id, total_amount

-- Error: `note` is not produced by the query
CREATE OR ALTER MATERIALIZED TABLE user_spending (user_id BIGINT, total_amount BIGINT, note STRING)
    AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;

-- Error: BIGINT cannot be implicitly cast to BOOLEAN
CREATE OR ALTER MATERIALIZED TABLE user_spending (user_id BIGINT, total_amount BOOLEAN)
    AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;
```

A list of names only, without types, reorders the query columns. The names must be the query's columns and the count must match:

```sql
-- query produces (user_id, total_amount); store them in the order (total_amount, user_id)
CREATE OR ALTER MATERIALIZED TABLE user_spending (total_amount, user_id)
    AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;
-- columns: total_amount, user_id
```

You can also declare a primary key, a watermark, computed columns, and metadata columns, just like in any materialized table. Computed columns and `VIRTUAL` metadata columns are not stored, so they do not need to be in the query. Columns you declare that the query does not produce appear before the query's columns. The source table's own primary key and watermark are **not** carried over, so declare them again if you want them.

```sql
CREATE OR ALTER MATERIALIZED TABLE user_spending (
    total_doubled AS total_amount * 2,   -- computed column, not stored
    PRIMARY KEY (user_id) NOT ENFORCED
) AS SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id;
-- columns: total_doubled, user_id, total_amount (primary key: user_id)
```

Because the schema, primary key, and watermark all come from the statement and its query, not from the existing table, the result is fully defined by the DDL. The same statement always produces the same materialized table. Running it again after the conversion matches the now-existing materialized table and produces the same result.

**Using the converted table**

The conversion is one-way and cannot be undone. You can still use the result like a regular table: suspend it to stop the refresh job, then query it.

```sql
-- Suspend the materialized table to stop the refresh job
ALTER MATERIALIZED TABLE user_spending SUSPEND;

-- Use the materialized table as a regular table
SELECT * FROM user_spending;
```


<span class="label label-danger">Note</span>
- The catalog must support in-place conversion; catalogs that do not implement it reject the statement.
- Converting a view into a materialized table is not supported.
- If the refresh job (continuous mode) or refresh workflow (full mode) cannot be started, the conversion is **not** rolled back: the table is left as a materialized table in `SUSPENDED` status. Fix the underlying issue and `RESUME` it.

## 示例

假定 `materialized-table.refresh-mode.freshness-threshold` 为 30 分钟。

创建一个数据新鲜度为 `10` 秒的物化表，推导出的刷新模式为持续模式:

```sql
CREATE MATERIALIZED TABLE my_materialized_table_continuous
    PARTITIONED BY (ds)
    WITH (
        'format' = 'debezium-json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    FRESHNESS = INTERVAL '10' SECOND
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
    FRESHNESS = INTERVAL '1' HOUR
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
And same materialized table with explicitly specified columns

```sql
CREATE MATERIALIZED TABLE my_materialized_table_full (
    ds, product_id, product_name, avg_sale_price, total_quantity)
    ...
```
The order of the columns doesn't need to be the same as in the query,
Flink will do reordering if required  i.e. this will be also valid
```sql
CREATE MATERIALIZED TABLE my_materialized_table_full (
    product_id, product_name, ds, avg_sale_price, total_quantity)
    ...
```
Another way of doing this is putting name and data type
```sql
CREATE MATERIALIZED TABLE my_materialized_table_full (
    ds STRING, product_id STRING, product_name STRING, avg_sale_price DOUBLE, total_quantity BIGINT)
    ...
```
It might happen that types of columns are not the same, in that case implicit casts will be applied. 
If for some of the combinations implicit cast is not supported then there will be validation error thrown. 
Also, it is worth to note that reordering can also be done here. 

Create or alter a materialized table executed twice:

```sql
-- First execution: creates the materialized table
CREATE OR ALTER MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS
    SELECT
        user_id,
        COUNT(*) AS event_count,
        SUM(amount) AS total_amount
    FROM
        kafka_catalog.db1.events
    WHERE
        event_type = 'purchase'
    GROUP BY
        user_id;

-- Second execution: alters the query definition (adds avg_amount column)
CREATE OR ALTER MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS
    SELECT
        user_id,
        COUNT(*) AS event_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount  -- Add a new nullable column at the end
    FROM
        kafka_catalog.db1.events
    WHERE
        event_type = 'purchase'
    GROUP BY
        user_id;
```

<span class="label label-danger">Note</span>
- When altering an existing materialized table, schema evolution currently only supports adding `nullable` columns to the end of the original materialized table's schema.
- In continuous mode, the new refresh job will not restore from the state of the original refresh job when altering.
- All limitations from both CREATE and ALTER operations apply.

## 限制
- Does not support explicitly specifying physical columns which are not used in the query
- 不支持在 select 查询语句中引用临时表、临时视图或临时函数

# ALTER MATERIALIZED TABLE

```text
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name
    ADD { <schema_component> | (<schema_component> [, ...]) | <distribution> }
    | MODIFY { <schema_component> | (<schema_component> [, ...]) | <distribution> }
    | DROP {column_name | (column_name, column_name, ...) | PRIMARY KEY | CONSTRAINT constraint_name | WATERMARK | DISTRIBUTION }
    | SUSPEND | RESUME [WITH (key1=val1, key2=val2, ...)]
    | REFRESH [PARTITION partition_spec] |
    | SET (key1=val1, key2=val2, ...)
    | RESET (key1, key2, ...)
    | AS <select_statement>
<schema_component>:
  { <column_component> | <constraint_component> | <watermark_component> }

<column_component>:
  column_name <column_definition> [FIRST | AFTER column_name]

<constraint_component>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<watermark_component>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

<column_definition>:
  { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> } [COMMENT column_comment]

<physical_column_definition>:
  column_type

<metadata_column_definition>:
  column_type METADATA [ FROM metadata_key ] [ VIRTUAL ]

<computed_column_definition>:
  AS computed_column_expression
  
<distribution>:
{
    DISTRIBUTED BY [ { HASH | RANGE } ] (bucket_column_name1, bucket_column_name2, ...) [INTO n BUCKETS]
  | DISTRIBUTED INTO n BUCKETS
} 
```

`ALTER MATERIALIZED TABLE` 用于管理物化表。用户可以使用此命令暂停和恢复物化表的刷新管道，并手动触发数据刷新，以及修改物化表的查询定义。

## ADD
Use `ADD` clause to add [columns]({{< ref "docs/sql/reference/ddl/create" >}}#columns) (only non persisted like computed and metadata virtual), [constraints]({{< ref "docs/sql/reference/ddl/create" >}}#primary-key), a [watermark]({{< ref "docs/sql/reference/ddl/create" >}}#watermark), and a [distribution]({{< ref "docs/sql/reference/ddl/create" >}}#distributed) to an existing materialized table.

To add a column at the specified position, use `FIRST` or `AFTER col_name`. By default, the column is appended at last.

The following examples illustrate the usage of the `ADD` statements.

```sql
-- add a new column 
ALTER MATERIALIZED TABLE MyMaterializedTable ADD category_id STRING METADATA VIRTUAL;

-- add columns, constraint, and watermark
ALTER MATERIALIZED TABLE MyMaterializedTable ADD (
    log_ts STRING METADATA VIRTUAL FIRST,
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    PRIMARY KEY (id) NOT ENFORCED,
    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
);

-- add new distribution using a hash on uid into 4 buckets
ALTER MATERIALIZED TABLE MyMaterializedTable ADD DISTRIBUTION BY HASH(uid) INTO 4 BUCKETS;

-- add new distribution on uid into 4 buckets
ALTER MATERIALIZED TABLE MyMaterializedTable ADD DISTRIBUTION BY (uid) INTO 4 BUCKETS;

-- add new distribution on uid.
ALTER MATERIALIZED TABLE MyMaterializedTable ADD DISTRIBUTION BY (uid);

-- add new distribution into 4 buckets
ALTER MATERIALIZED TABLE MyMaterializedTable ADD DISTRIBUTION INTO 4 BUCKETS;
```
<span class="label label-danger">Note</span> Add a column to be primary key will change the column's nullability to false implicitly.

## MODIFY
Use `MODIFY` clause to change column's comment, position, type (only non persisted like computed and metadata virtual also see [columns]({{< ref "docs/sql/reference/ddl/create" >}}#columns)), change primary key columns and watermark strategy to an existing table.

To modify an existent column to a new position, use `FIRST` or `AFTER col_name`. By default, the position remains unchanged.

The following examples illustrate the usage of the `MODIFY` statements.

```sql
-- modify a column type, comment and position
ALTER MATERIALIZED TABLE MyMaterializedTable MODIFY measurement double METADATA COMMENT 'unit is bytes per second' AFTER `id`;

-- modify definition of column log_ts and ts, primary key, watermark. They must exist in table schema
ALTER MATERIALIZED TABLE MyMaterializedTable MODIFY (
    log_ts STRING METADATA COMMENT 'log timestamp string' AFTER `id`,  -- reorder columns
    ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
    PRIMARY KEY (id) NOT ENFORCED,
    WATERMARK FOR ts AS ts -- modify watermark strategy
);
```

<span class="label label-danger">Note</span> Modify a column to be primary key will change the column's nullability to false implicitly.

## DROP
Use the `DROP` clause to drop columns (only non persisted like computed and metadata virtual also see [columns]({{< ref "docs/sql/reference/ddl/create" >}}#columns)), primary key, partitions, and watermark strategy to an existing table.

The following examples illustrate the usage of the `DROP` statements.

```sql
-- drop a column
ALTER MATERIALIZED TABLE MyMaterializedTable DROP measurement;

-- drop columns
ALTER MATERIALIZED TABLE MyMaterializedTable DROP (col1, col2, col3);

-- drop primary key
ALTER MATERIALIZED TABLE MyMaterializedTable DROP PRIMARY KEY;

-- drop a watermark
ALTER MATERIALIZED TABLE MyMaterializedTable DROP WATERMARK;

-- drop distribution
ALTER MATERIALIZED TABLE MyMaterializedTable DROP DISTRIBUTION;
```

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
- 暂停持续模式的表时，默认会使用 `STOP WITH SAVEPOINT` 暂停作业，你需要使用[参数]({{< ref "docs/deployment/config" >}}#execution-checkpointing-savepoint-dir)设置 `SAVEPOINT` 保存路径。

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

## SET

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
```

`SET` is used to add or overwrite table options of a materialized table. Existing options not listed in the statement are preserved.

**Key handling:**
- Keys are applied in the order they appear in the statement.
- If the same key is listed multiple times, the last value in the list wins.
- The empty option list `SET ()` is rejected with a validation error.

**Example:**

```sql
-- Add a new option and overwrite an existing one
ALTER MATERIALIZED TABLE my_materialized_table SET ('format' = 'json', 'sink.parallelism' = '4');

-- Duplicate keys: the last value wins. After this statement, 'format' is 'csv'.
ALTER MATERIALIZED TABLE my_materialized_table SET ('format' = 'json', 'format' = 'csv');
```

<span class="label label-danger">Note</span> When run through the Flink SQL Gateway, the behavior depends on the refresh mode and current refresh status:
- `FULL` mode: the change is applied to the catalog. The refresh workflow is not touched.
- `CONTINUOUS` mode, `ACTIVATED` status: the running refresh job is stopped with savepoint, the change is applied to the catalog, and a new refresh job is started using the updated options. The new job does **not** restore from the savepoint taken during suspend, so streaming state is reset.
- `CONTINUOUS` mode, `SUSPENDED` status: the change is applied to the catalog and the savepoint stored in the refresh handler is cleared, so the next `RESUME` will also start a fresh job.
- `CONTINUOUS` mode, `INITIALIZING` status: the statement is rejected.

## RESET

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESET (key1, key2, ...)
```

`RESET` is used to remove table options from a materialized table.

**Key handling:**
- Keys that are not currently set on the table are silently ignored. The statement still succeeds.
- Duplicate keys in the key list are de-duplicated and treated as a single reset for that key.
- The empty key list `RESET ()` is rejected with a validation error.
- The `connector` key is reserved and cannot be reset. Attempting to do so is rejected with a validation error.

**Example:**

```sql
-- Remove the 'format' and 'sink.parallelism' options
ALTER MATERIALIZED TABLE my_materialized_table RESET ('format', 'sink.parallelism');

-- 'sink.parallelism' is not currently set on the table: this is a no-op for that key,
-- 'format' is still removed and the statement succeeds.
ALTER MATERIALIZED TABLE my_materialized_table RESET ('format', 'sink.parallelism');

-- Duplicates collapse to a single reset for 'format'.
ALTER MATERIALIZED TABLE my_materialized_table RESET ('format', 'format');
```

<span class="label label-danger">Note</span> When run through the Flink SQL Gateway, the behavior depends on the refresh mode and current refresh status:
- `FULL` mode: the change is applied to the catalog. The refresh workflow is not touched.
- `CONTINUOUS` mode, `ACTIVATED` status: the running refresh job is stopped with savepoint, the change is applied to the catalog, and a new refresh job is started using the updated options. The new job does **not** restore from the savepoint taken during suspend, so streaming state is reset.
- `CONTINUOUS` mode, `SUSPENDED` status: the change is applied to the catalog and the savepoint stored in the refresh handler is cleared, so the next `RESUME` will also start a fresh job.
- `CONTINUOUS` mode, `INITIALIZING` status: the statement is rejected.

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

## AS <select_statement>
```sql
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name AS <select_statement>
```

`AS <select_statement>` 子句用于修改刷新物化表的查询定义。它会先使用新查询推导的 `schema` 更新表的 `schema`，然后使用新查询刷新表数据。需要特别强调的是，默认情况下，这不会影响历史数据。

具体修改流程取决于物化表的刷新模式：

**全量模式：**

1. 更新物化表的 `schema` 和查询定义。
2. 在刷新作业下次触发执行时，将使用新的查询定义刷新数据:
   - 如果修改的物化表是分区表，且[partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 配置正确，则仅刷新最新分区。
   - 否则，将刷新整个表的数据。

**持续模式：**

1. 暂停当前的流式刷新作业。
2. 更新物化表的 `schema` 和查询定义。
3. 启动新的流式作业以刷新物化表：
   - 新的流式作业会从头开始，而不会从之前的流式作业状态恢复。
   - 数据源的起始位点会由到连接器的默认实现或查询中设置的 [dynamic hint]({{< ref "docs/sql/reference/queries/hints" >}}#dynamic-table-options) 决定。

**示例：**

```sql
-- 原始物化表定义
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS
    SELECT
        user_id,
        COUNT(*) AS event_count,
        SUM(amount) AS total_amount
    FROM
        kafka_catalog.db1.events
    WHERE
        event_type = 'purchase'
    GROUP BY
        user_id;

-- 修改现有物化表的查询
ALTER MATERIALIZED TABLE my_materialized_table
AS SELECT
    user_id,
    COUNT(*) AS event_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount  -- 在末尾追加新的可为空列
FROM
    kafka_catalog.db1.events
WHERE
    event_type = 'purchase'
GROUP BY
    user_id;
```

<span class="label label-danger">注意</span>
- Schema 演进当前仅支持在原表 schema 尾部追加`可空列`。
- 在持续模式下，新的流式作业不会从原来的流式作业的状态恢复。这可能会导致短暂的数据重复或丢失。

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
