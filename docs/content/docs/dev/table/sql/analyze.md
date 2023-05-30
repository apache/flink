---
title: "ANALYZE Statements"
weight: 8
type: docs
aliases:
  - /dev/table/sql/analyze.html
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

# ANALYZE Statements

`ANALYZE` statements are used to collect statistics for existing tables and store the result to catalog. Only
`ANALYZE TABLE` statements are supported now, and need to be triggered manually instead of automatically.

<span class="label label-danger">Attention</span> Currently, `ANALYZE TABLE` only supports in batch mode. Only existing 
table is supported, and an exception will be thrown if the table is a view or table not exists.


## Run an ANALYZE TABLE statement

{{< tabs "analyze table" >}}
{{< tab "Java" >}}
`ANALYZE TABLE` statements can be executed with the `executeSql()` method of the `TableEnvironment`. 

The following examples show how to run a `ANALYZE TABLE` statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Scala" >}}
`ANALYZE TABLE` statements can be executed with the `executeSql()` method of the `TableEnvironment`. 

The following examples show how to run a `ANALYZE TABLE` statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

`ANALYZE TABLE` statements can be executed with the `execute_sql()` method of the `TableEnvironment`.

The following examples show how to run a `ANALYZE TABLE` statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

`ANALYZE TABLE` statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a `ANALYZE TABLE` statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// register a non-partition table named "Store"
tableEnv.executeSql(
        "CREATE TABLE Store (" +
        " `id` BIGINT NOT NULl," +
        " `location` VARCHAR(32)," +
        " `owner` VARCHAR(32)" +
        ") with (...)");

// register a partition table named "Orders"
tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `id` BIGINT NOT NULl," +
        " `product` VARCHAR(32)," +
        " `amount` INT," +
        " `sold_year` BIGINT," +
        " `sold_month` BIGINT," +
        " `sold_day` BIGINT" +
        ") PARTITIONED BY (`sold_year`, `sold_month`, `sold_day`) "
        ") with (...)");

// Non-partition table, collect row count.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// Non-partition table, collect row count and statistics for all columns.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// Non-partition table, collect row count and statistics for column `location`.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


// Suppose table "Orders" has 4 partitions with specs:
// Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
// Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
// Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
// Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


// Partition table, collect row count for Partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

// Partition table, collect row count for Partition1 and Partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

// Partition table, collect row count for all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

// Partition table, collect row count and statistics for all columns on partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for all columns on partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for all columns on all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for column `amount` on partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

// Partition table, collect row count and statistics for `amount` and `product` on partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

// Partition table, collect row count and statistics for column `amount` and `product` on all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// register a non-partition table named "Store"
tableEnv.executeSql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " `location` VARCHAR(32)," +
          " `owner` VARCHAR(32)" +
          ") with (...)");

// register a partition table named "Orders"
tableEnv.executeSql(
  "CREATE TABLE Orders (" +
          " `id` BIGINT NOT NULl," +
          " `product` VARCHAR(32)," +
          " `amount` INT," +
          " `sold_year` BIGINT," +
          " `sold_month` BIGINT," +
          " `sold_day` BIGINT" +
          ") PARTITIONED BY (`sold_year`, `sold_month`, `sold_day`) "
") with (...)");

// Non-partition table, collect row count.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// Non-partition table, collect row count and statistics for all columns.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// Non-partition table, collect row count and statistics for column `location`.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


// Suppose table "Orders" has 4 partitions with specs:
// Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
// Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
// Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
// Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


// Partition table, collect row count for Partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

// Partition table, collect row count for Partition1 and Partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

// Partition table, collect row count for all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

// Partition table, collect row count and statistics for all columns on partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for all columns on partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for all columns on all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and statistics for column `amount` on partition1.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

// Partition table, collect row count and statistics for `amount` and `product` on partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

// Partition table, collect row count and statistics for column `amount` and `product` on all partitions.
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# register a non-partition table named "Store"
table_env.execute_sql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " `location` VARCHAR(32)," +
          " `owner` VARCHAR(32)" +
          ") with (...)");

# register a partition table named "Orders"
table_env.execute_sql(
  "CREATE TABLE Orders (" +
          " `id` BIGINT NOT NULl," +
          " `product` VARCHAR(32)," +
          " `amount` INT," +
          " `sold_year` BIGINT," +
          " `sold_month` BIGINT," +
          " `sold_day` BIGINT" +
          ") PARTITIONED BY (`sold_year`, `sold_month`, `sold_day`) "
") with (...)");

# Non-partition table, collect row count.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS");

# Non-partition table, collect row count and statistics for all columns.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

# Non-partition table, collect row count and statistics for column `location`.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


# Suppose table "Orders" has 4 partitions with specs:
# Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
# Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
# Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
# Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


# Partition table, collect row count for Partition1.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

# Partition table, collect row count for Partition1 and Partition2.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

# Partition table, collect row count for all partitions.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

# Partition table, collect row count and statistics for all columns on partition1.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and statistics for all columns on partition1 and partition2.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and statistics for all columns on all partitions.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and statistics for column `amount` on partition1.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

# Partition table, collect row count and statistics for `amount` and `product` on partition1 and partition2.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

# Partition table, collect row count and statistics for column `amount` and `product` on all partitions.
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Store (
> `id` BIGINT NOT NULl,
> `location` VARCHAR(32),
> `owner` VARCHAR(32)
> ) with (
> ...
> );
[INFO] Table has been created.

Flink SQL> CREATE TABLE Orders (
> `id` BIGINT NOT NULl,
> `product` VARCHAR(32),
> `amount` INT,
> `sold_year` BIGINT,
> `sold_month` BIGINT,
> `sold_day` BIGINT   
> ) PARTITIONED BY (`sold_year`, `sold_month`, `sold_day`)
> ) with (
> ...
> );
[INFO] Table has been created.

Flink SQL> ANALYZE TABLE Store COMPUTE STATISTICS;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS;
[INFO] Execute statement succeed.

Flink SQL> ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS;
[INFO] Execute statement succeed.

Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS;
[INFO] Execute statement succeed.

Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS;
[INFO] Execute statement succeed.

Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS;
[INFO] Execute statement succeed.    

Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Orders PARTITION (sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product;
[INFO] Execute statement succeed.
    
Flink SQL> ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product;
[INFO] Execute statement succeed.
```
{{< /tab >}}
{{< /tabs >}}


## Syntax

```sql
ANALYZE TABLE [catalog_name.][db_name.]table_name PARTITION(partcol1[=val1] [, partcol2[=val2], ...]) COMPUTE STATISTICS [FOR COLUMNS col1 [, col2, ...] | FOR ALL COLUMNS]
```
- PARTITION(partcol1[=val1] [, partcol2[=val2], ...]) is required for the partition table
  - If no partition is specified, the statistics will be gathered for all partitions
  - If a certain partition is specified, the statistics will be gathered only for specific partition
  - If the table is non-partition table , while a partition is specified, an exception will be thrown
  - If a certain partition is specified, but the partition does not exist, an exception will be thrown
  
- FOR COLUMNS col1 [, col2, ...] or FOR ALL COLUMNS are optional
  - If no column is specified, only the table level statistics will be gathered
  - If a column does not exist, or column is not a physical column, an exception will be thrown.
  - If a column or any column is specified, the column level statistics will be gathered
    - the column level statistics include:
      - ndv: the number of distinct values
      - nullCount: the number of nulls
      - avgLen: the average length of column values 
      - maxLen: the max length of column values
      - minValue: the min value of column values
      - maxValue: the max value of column values
      - valueCount: the value count only for boolean type
    - the supported types and its corresponding column level statistics are as following sheet lists("Y" means support, "N" means unsupported):

| Types                            | `ndv` | `nullCount` | `avgLen` | `maxLen` | `maxValue` | `minValue` | `valueCount` |
|:---------------------------------|:-----:|:-----------:|:--------:|:--------:|:----------:|:----------:|:------------:|
| `BOOLEAN`                        |   N   |      Y      |    N     |    N     |     N      |     N      |      Y       |
| `TINYINT`                        |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `SMALLINT`                       |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `INTEGER`                        |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `FLOAT`                          |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `DATE`                           |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `TIME_WITHOUT_TIME_ZONE`         |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `BIGINT`                         |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `DOUBLE`                         |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `DECIMAL`                        |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `TIMESTAMP_WITH_LOCAL_TIME_ZONE` |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `TIMESTAMP_WITHOUT_TIME_ZONE`    |   Y   |      Y      |    N     |    N     |     Y      |     Y      |      N       |
| `CHAR`                           |   Y   |      Y      |    Y     |    Y     |     N      |     N      |      N       |
| `VARCHAR`                        |   Y   |      Y      |    Y     |    Y     |     N      |     N      |      N       |
| `other types`                    |   N   |      Y      |    N     |    N     |     N      |     N      |      N       |

*NOTE:* For the fix length types (like `BOOLEAN`, `INTEGER`, `DOUBLE` etc.), we need not collect the `avgLen` and `maxLen` from the original records.

{{< top >}}