---
title: "ANALYZE TABLE Statements"
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

# ANALYZE TABLE Statements

- `ANALYZE TABLE` statements are used to collect statistics for existing tables, and write statistics back to source table. 
- Only existing table is supported, and an exception will be thrown if the table is a view.
- Currently, `ANALYZE TABLE` only support in batch mode.
- `ANALYZE TABLE` statements is triggered manually instead of automatically.


## Run a ANALYZE TABLE statement

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

// register a non-partition table named "Orders"
tableEnv.executeSql(
        "CREATE TABLE Orders (" +
        " `user` BIGINT NOT NULl," +
        " product VARCHAR(32)," +
        " amount INT," +
        " ts TIMESTAMP(3)," +
        " ptime AS PROCTIME()"
        ") with (...)");

// register a partition table named "Store"
tableEnv.executeSql(
        "CREATE TABLE Store (" +
        " `id` BIGINT NOT NULl," +
        " product VARCHAR(32)," +
        " amount INT," +
        " `date_sk` BIGINT" +
        ") PARTITIONED BY (`date_sk`, `id`) "
        ") with (...)");

// Non-partition table, collect row count.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS");

// Non-partition table, collect row count and all columns statistics.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR ALL COLUMNS");

// Non-partition table, collect row count and specify column `user`'s statistics.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR COLUMNS(a)");


// Suppose table "Store" has 4 partitions with specs:
// Partition1 : (date_sk='2022-8-8', id=1)
// Partition2 : (date_sk='2022-8-8', id=2)
// Partition3 : (date_sk='2022-8-9', id=1)
// Partition3 : (date_sk='2022-8-9', id=2)


// Partition table, collect row count for Partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS");

// Partition table, collect row count for Partition1 and Partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS");

// Partition table, collect row count for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// Partition table, collect row count and all columns statistics for partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and all columns statistics for partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and all columns statistics for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and specify column `amount`'s statistics for partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR COLUMNS(amount)");

// Partition table, collect row count and specify column `amount` and `product`'s statistics for partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR COLUMNS(amount, product)");

// Partition table, collect row count and column `amount` and `product`'s statistics for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS(amount, product)");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// register a non-partition table named "Orders"
tableEnv.executeSql(
  "CREATE TABLE Orders (" +
          " `user` BIGINT NOT NULl," +
          " product VARCHAR(32)," +
          " amount INT," +
          " ts TIMESTAMP(3)," +
          " ptime AS PROCTIME()"
") with (...)");

// register a partition table named "Store"
tableEnv.executeSql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " product VARCHAR(32)," +
          " amount INT," +
          " `date_sk` BIGINT" +
          ") PARTITIONED BY (`date_sk`, `id`) "
") with (...)");

// Non-partition table, collect row count.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS");

// Non-partition table, collect row count and all columns statistics.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR ALL COLUMNS");

// Non-partition table, collect row count and specify column `user`'s statistics.
tableEnv.executeSql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR COLUMNS(a)");


// Suppose table "Store" has 4 partitions with specs:
// Partition1 : (date_sk='2022-8-8', id=1)
// Partition2 : (date_sk='2022-8-8', id=2)
// Partition3 : (date_sk='2022-8-9', id=1)
// Partition3 : (date_sk='2022-8-9', id=2)


// Partition table, collect row count for Partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS");

// Partition table, collect row count for Partition1 and Partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS");

// Partition table, collect row count for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// Partition table, collect row count and all columns statistics for partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and all columns statistics for partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and all columns statistics for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// Partition table, collect row count and specify column `amount`'s statistics for partition1.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR COLUMNS(amount)");

// Partition table, collect row count and specify column `amount` and `product`'s statistics for partition1 and partition2.
tableEnv.executeSql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR COLUMNS(amount, product)");

// Partition table, collect row count and column `amount` and `product`'s statistics for all partitions.
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS(amount, product)");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# register a non-partition table named "Orders"
table_env.execute_sql( \
  "CREATE TABLE Orders (" +
          " `user` BIGINT NOT NULl," +
          " product VARCHAR(32)," +
          " amount INT," +
          " ts TIMESTAMP(3)," +
          " ptime AS PROCTIME()"
") with (...)");

# register a partition table named "Store"
table_env.execute_sql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " product VARCHAR(32)," +
          " amount INT," +
          " `date_sk` BIGINT" +
          ") PARTITIONED BY (`date_sk`, `id`) "
") with (...)");

# Non-partition table, collect row count.
table_env.execute_sql("ANALYZE TABLE Orders COMPUTE STATISTICS");

# Non-partition table, collect row count and all columns statistics.
table_env.execute_sql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR ALL COLUMNS");

# Non-partition table, collect row count and specify column `user`'s statistics.
table_env.execute_sql("ANALYZE TABLE Orders COMPUTE STATISTICS FOR COLUMNS(a)");


# Suppose table "Store" has 4 partitions with specs:
# Partition1 : (date_sk='2022-8-8', id=1)
# Partition2 : (date_sk='2022-8-8', id=2)
# Partition3 : (date_sk='2022-8-9', id=1)
# Partition3 : (date_sk='2022-8-9', id=2)


# Partition table, collect row count for Partition1.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS");

# Partition table, collect row count for Partition1 and Partition2.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS");

# Partition table, collect row count for all partitions.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS");

# Partition table, collect row count and all columns statistics for partition1.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and all columns statistics for partition1 and partition2.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and all columns statistics for all partitions.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

# Partition table, collect row count and specify column `amount`'s statistics for partition1.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR COLUMNS(amount)");

# Partition table, collect row count and specify column `amount` and `product`'s statistics for partition1 and partition2.
table_env.execute_sql("ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR COLUMNS(amount, product)");

# Partition table, collect row count and column `amount` and `product`'s statistics for all partitions.
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS(amount, product)");
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (
>  `user` BIGINT NOT NULl,
>  product VARCHAR(32),
>  amount INT,
>  ts TIMESTAMP(3)
> ) with (
>  ...
> );
[INFO] Table has been created.

Flink SQL> CREATE TABLE Store (
>  `id` BIGINT NOT NULl,
>  product VARCHAR(32),
>  amount INT,
>  `date_sk` BIGINT
>  ) PARTITIONED BY (`date_sk`, `id`)
>  ) with (
>    ...
>  );
[INFO] Table has been created.

Flink SQL> ANALYZE TABLE Orders COMPUTE STATISTICS;
    
Flink SQL> ANALYZE TABLE Orders COMPUTE STATISTICS FOR ALL COLUMNS;

Flink SQL> ANALYZE TABLE Orders COMPUTE STATISTICS FOR COLUMNS(a);
    
Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS;

Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS;

Flink SQL> ANALYZE TABLE Store COMPUTE STATISTICS;

Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR ALL COLUMNS;

Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR ALL COLUMNS;
    
Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8', id=1) COMPUTE STATISTICS FOR COLUMNS(amount);
    
Flink SQL> ANALYZE TABLE Store PARTITION (date_sk='2022-8-8') COMPUTE STATISTICS FOR COLUMNS(amount, product);
    
Flink SQL> ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS(amount, product);
```
{{< /tab >}}
{{< /tabs >}}


## Syntax

```sql
ANALYZE TABLE [catalog_name.][db_name.]table_name [PARTITION(partcol1=val1 [, partcol2=val2, ...])] COMPUTE STATISTICS [FOR COLUMNS col1 [, col2, ...] | FOR ALL COLUMNS]
```
- PARTITION(partcol1=val1 [, partcol2=val2, ...]) is optional for the partition table.
  - If no partition is specified, the statistics will be gathered for all partitions
  - If a certain partition is specified, the statistics will be gathered only for specific partition
  - If the table is non-partition table , while a partition is specified, an exception will be thrown
  
- FOR COLUMNS col1 [, col2, ...] or FOR ALL COLUMNS are also optional.
  - If no columns is specified, only the table level statistics will be gathered.
  - If a column or any column is specified, the column level statistics will be gathered.
    - the column level statistics including:
      - ndv : the number of distinct values
      - nullCount : the number of nulls
      - avgLen : the average length of column values 
      - maxLen : the max length of column values
      - minValue : the min value of column values
      - maxValue : the max value of column values
      - valueCount : the value count only for boolean type
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