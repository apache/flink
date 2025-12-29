---
title: Statements
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

# Materialized Table Statements

Flink SQL supports the following Materialized Table statements for now:
- [CREATE [OR ALTER] MATERIALIZED TABLE](#create-or-alter-materialized-table)
- [ALTER MATERIALIZED TABLE](#alter-materialized-table)
- [DROP MATERIALIZED TABLE](#drop-materialized-table)

# CREATE [OR ALTER] MATERIALIZED TABLE

```
CREATE [OR ALTER] MATERIALIZED TABLE [catalog_name.][db_name.]table_name

[(
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
)]

[COMMENT table_comment]

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
```

## PRIMARY KEY

`PRIMARY KEY` defines an optional list of columns that uniquely identifies each row within the table. The column as the primary key must be non-null.

## PARTITIONED BY

`PARTITIONED BY` defines an optional list of columns to partition the materialized table. A directory is created for each partition if this materialized table is used as a filesystem sink.

**Example:**

```sql
-- Create a materialized table and specify the partition field as `ds`.
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT
        ds
    FROM
        ...
```

<span class="label label-danger">Note</span>
- The partition column must be included in the query statement of the materialized table.

## WITH Options

`WITH Options` are used to specify the materialized table properties, including [connector options]({{< ref "docs/connectors/table/" >}}) and [time format option]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) for partition fields.

```sql
-- Create a materialized table, specify the partition field as 'ds', and the corresponding time format as 'yyyy-MM-dd'
CREATE MATERIALIZED TABLE my_materialized_table
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    ...
```

As shown in the above example, we specified the date-formatter option for the `ds` partition column. During each scheduling, the scheduling time will be converted to the ds partition value. For example, for a scheduling time of `2024-01-01 00:00:00`, only the partition `ds = '2024-01-01'` will be refreshed.

<span class="label label-danger">Note</span>
- The [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) option only works in full mode.
- The field in the [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) must be a valid string type partition field.

## FRESHNESS

`FRESHNESS` defines the data freshness of a materialized table.

`FRESHNESS` is optional. When omitted, the system uses the default freshness based on the refresh mode: `materialized-table.default-freshness.continuous` (default: 3 minutes) for CONTINUOUS mode, or `materialized-table.default-freshness.full` (default: 1 hour) for FULL mode.

**FRESHNESS and Refresh Mode Relationship**

FRESHNESS defines the maximum amount of time that the materialized table's content should lag behind updates to the base tables. When not specified, it uses the default value from configuration based on the refresh mode. It does two things: firstly it determines the [refresh mode]({{< ref "docs/dev/table/materialized-table/overview" >}}#refresh-mode) of the materialized table through [configuration]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold), followed by determining the data refresh frequency to meet the actual data freshness requirements.

**Explanation of FRESHNESS Parameter**

The FRESHNESS parameter range is INTERVAL `'<num>'` { SECOND | MINUTE | HOUR | DAY }. `'<num>'` must be a positive integer, and in FULL mode, `'<num>'` should be a common divisor of the respective time interval unit.

**Examples:**
(Assuming `materialized-table.refresh-mode.freshness-threshold` is 30 minutes)

```sql
-- The corresponding refresh pipeline is a streaming job with a checkpoint interval of 1 second
FRESHNESS = INTERVAL '1' SECOND

-- The corresponding refresh pipeline is a real-time job with a checkpoint interval of 1 minute
FRESHNESS = INTERVAL '1' MINUTE

-- The corresponding refresh pipeline is a scheduled workflow with a schedule cycle of 1 hour
FRESHNESS = INTERVAL '1' HOUR

-- The corresponding refresh pipeline is a scheduled workflow with a schedule cycle of 1 day
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

**Invalid `FRESHNESS` Examples:**

```sql
-- Interval is a negative number
FRESHNESS = INTERVAL '-1' SECOND

-- Interval is 0
FRESHNESS = INTERVAL '0' SECOND

-- Interval is in months or years
FRESHNESS = INTERVAL '1' MONTH
FRESHNESS = INTERVAL '1' YEAR

-- In FULL mode, the interval is not a common divisor of the respective time range
FRESHNESS = INTERVAL '60' SECOND
FRESHNESS = INTERVAL '5' HOUR
```

<span class="label label-danger">Note</span>
- If FRESHNESS is not specified, the table will use the default freshness interval based on the refresh mode: `materialized-table.default-freshness.continuous` (default: 3 minutes) for CONTINUOUS mode, or `materialized-table.default-freshness.full` (default: 1 hour) for FULL mode.
- The materialized table data will be refreshed as closely as possible within the defined freshness but cannot guarantee complete satisfaction.
- In CONTINUOUS mode, setting a data freshness interval that is too short can impact job performance as it aligns with the checkpoint interval. To optimize checkpoint performance, consider [enabling-changelog]({{< ref "docs/ops/state/state_backends" >}}#incremental-checkpoints).
- In FULL mode, data freshness must be translated into a cron expression, consequently, only freshness intervals within predefined time spans are presently accommodated, this design ensures alignment with cron's capabilities. Specifically, support for the following freshness:
    - Second: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30.
    - Minute: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30.
    - Hour: 1, 2, 3, 4, 6, 8, 12.
    - Day: 1.

## REFRESH_MODE

`REFRESH_MODE` is used to explicitly specify the refresh mode of the materialized table. The specified mode takes precedence over the framework's automatic inference to meet specific scenarios' needs.

**Examples:**
(Assuming `materialized-table.refresh-mode.freshness-threshold` is 30 minutes)

```sql
-- The refresh mode of the created materialized table is CONTINUOUS, and the job's checkpoint interval is 1 hour.
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '1' HOUR
    REFRESH_MODE = CONTINUOUS
    AS SELECT
       ...

-- The refresh mode of the created materialized table is FULL, and the job's schedule cycle is 10 minutes.
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' MINUTE
    REFRESH_MODE = FULL
    AS SELECT
       ...
```

## AS <select_statement>

This clause is used to define the query for populating materialized table data. The upstream table can be a materialized table, table, or view. The select statement supports all Flink SQL [Queries]({{< ref "docs/dev/table/sql/queries/overview" >}}).

**Example:**

```sql
CREATE MATERIALIZED TABLE my_materialized_table
    FRESHNESS = INTERVAL '10' SECOND
    AS SELECT * FROM kafka_catalog.db1.kafka_table;
```

## OR ALTER

The `OR ALTER` clause provides create-or-update semantics:

- **If the materialized table does not exist**: Creates a new materialized table with the specified options
- **If the materialized table exists**: Modifies the query definition (behaves like `ALTER MATERIALIZED TABLE AS`)

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

## Examples

Assuming `materialized-table.refresh-mode.freshness-threshold` is 30 minutes.

Create a materialized table with a data freshness of 10 seconds and the derived refresh mode is CONTINUOUS:

```sql
CREATE MATERIALIZED TABLE my_materialized_table_continuous
    PARTITIONED BY (ds)
    WITH (
        'format' = 'debezium-json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    FRESHNESS = INTERVAL '10' SECOND
    AS SELECT
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

Create a materialized table with a data freshness of 1 hour and the derived refresh mode is FULL:

```sql
CREATE MATERIALIZED TABLE my_materialized_table_full
    PARTITIONED BY (ds)
    WITH (
        'format' = 'json',
        'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
    )
    FRESHNESS = INTERVAL '1' HOUR
    AS SELECT
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
Flink will do reordering if required i.e. this will be also valid
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

## Limitations

- Does not support explicitly specifying physical columns which are not used in the query 
- Does not support referring to temporary tables, temporary views, or temporary functions in the select query

# ALTER MATERIALIZED TABLE

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name
    SUSPEND | RESUME [WITH (key1=val1, key2=val2, ...)] |
    REFRESH [PARTITION partition_spec] |
    AS <select_statement>
```

`ALTER MATERIALIZED TABLE` is used to manage materialized tables. This command allows users to suspend and resume refresh pipeline of materialized tables and manually trigger data refreshes, and modify the query definition of materialized tables.

## SUSPEND

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name SUSPEND
```

`SUSPEND` is used to pause the background refresh pipeline of the materialized table.

**Example:**

```sql
-- Specify SAVEPOINT path before pausing
SET 'execution.checkpointing.savepoint-dir' = 'hdfs://savepoint_path';

-- Suspend the specified materialized table
ALTER MATERIALIZED TABLE my_materialized_table SUSPEND;
```

<span class="label label-danger">Note</span>
- When suspending a table in CONTINUOUS mode, the job will be paused using STOP WITH SAVEPOINT by default. You need to set the SAVEPOINT save path using [parameters]({{< ref "docs/deployment/config" >}}#execution-checkpointing-savepoint-dir).

## RESUME

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name RESUME [WITH (key1=val1, key2=val2, ...)]
```

`RESUME` is used to resume the refresh pipeline of a materialized table. Materialized table dynamic options can be specified through `WITH options` clause, which only take effect on the current refreshed pipeline and are not persistent.

**Example:**

```sql
-- Resume the specified materialized table
ALTER MATERIALIZED TABLE my_materialized_table RESUME;

-- Resume the specified materialized table and specify sink parallelism
ALTER MATERIALIZED TABLE my_materialized_table RESUME WITH ('sink.parallelism'='10');
```

## REFRESH

```
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name REFRESH [PARTITION partition_spec]
```

`REFRESH` is used to proactively trigger the refresh of the materialized table.

**Example:**

```sql
-- Refresh the entire table data
ALTER MATERIALIZED TABLE my_materialized_table REFRESH;

-- Refresh specified partition data
ALTER MATERIALIZED TABLE my_materialized_table REFRESH PARTITION (ds='2024-06-28');
```

<span class="label label-danger">Note</span>
- The REFRESH operation will start a Flink batch job to refresh the materialized table data.

## AS <select_statement>

```sql
ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name AS <select_statement>
```

The `AS <select_statement>` clause allows you to modify the query definition for refreshing materialized table. It will first evolve the table's schema using the schema derived from the new query and then use the new query to refresh the table data. It is important to emphasize that, by default, this does not impact historical data.

The modification process depends on the refresh mode of the materialized table:

**Full mode:**

1. Update the `schema` and `query definition` of the materialized table.
2. The table is refreshed using the new query definition when the next refresh job is triggered:
   - If it is a partitioned table and [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) is correctly set, only the latest partition will be refreshed.
   - Otherwise, the table will be overwritten entirely.

**Continuous mode:**

1. Pause the current running refresh job.
2. Update the `schema` and `query definition` of the materialized table.
3. Start a new refresh job to refresh the materialized table:
   - The new refresh job starts from the beginning and does not restore from the previous state.
   - The starting offset of the data source is determined by the connector’s default implementation or the [dynamic hint]({{< ref "docs/dev/table/sql/queries/hints" >}}#dynamic-table-options) specified in the query.

**Example:**

```sql
-- Definition of origin materialized table
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

-- Modify the query definition of materialized table
ALTER MATERIALIZED TABLE my_materialized_table
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
- Schema evolution currently only supports adding `nullable` columns to the end of the original table's schema.
- In continuous mode, the new refresh job will not restore from the state of the original refresh job. This may result in temporary data duplication or loss.

# DROP MATERIALIZED TABLE

```text
DROP MATERIALIZED TABLE [IF EXISTS] [catalog_name.][database_name.]table_name
```

When dropping a materialized table, the background refresh pipeline will be deleted first, and then the metadata corresponding to the materialized table will be removed from the Catalog.

**Example:**

```sql
-- Delete the specified materialized table
DROP MATERIALIZED TABLE IF EXISTS my_materialized_table;
```
