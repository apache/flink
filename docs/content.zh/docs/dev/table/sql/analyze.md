---
title: "ANALYZE 语句"
weight: 8
type: docs
aliases:
  - /zh/dev/table/sql/analyze.html
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

<a name="analyze-statements"></a>

# ANALYZE 语句

`ANALYZE` 语句被用于为存在的表收集统计信息，并将统计信息写入该表的 catalog 中。当前版本中，`ANALYZE` 语句只支持 `ANALYZE TABLE`，
且只能由用户手动触发。

<span class="label label-danger">注意</span> 现在, `ANALYZE TABLE` 只支持批模式（Batch Mode），且只能用于已存在的表，
如果表不存在或者是视图（View）则会报错。


<a name="run-a-analyze-table-statement"></a>

## 执行 ANALYZE TABLE 语句

{{< tabs "analyze table" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 `ANALYZE TABLE` 语句。

以下示例展示了如何在 `TableEnvironment` 中执行一条 `ANALYZE TABLE` 语句。
{{< /tab >}}
{{< tab "Scala" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 `ANALYZE TABLE` 语句。

以下示例展示了如何在 `TableEnvironment` 中执行一条 `ANALYZE TABLE` 语句。
{{< /tab >}}
{{< tab "Python" >}}
可以使用 `TableEnvironment` 的 `execute_sql()` 方法执行 `ANALYZE TABLE` 语句。

以下示例展示了如何在 `TableEnvironment` 中执行一条 `ANALYZE TABLE` 语句。
{{< /tab >}}
{{< tab "SQL CLI" >}}

`ANALYZE TABLE` 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 `ANALYZE TABLE` 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(...);

// 注册名为 “Store” 的非分区表
tableEnv.executeSql(
        "CREATE TABLE Store (" +
        " `id` BIGINT NOT NULl," +
        " `location` VARCHAR(32)," +
        " `owner` VARCHAR(32)" +
        ") with (...)");

// 注册名为 “Orders” 的分区表
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

// 非分区表，收集表级别的统计信息(表的统计信息主要为行数(row count))。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// 非分区表，收集表级别的统计信息和所有列的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// 非分区表，收集表级别的统计信息和指定列(列: location)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


// 假设分区表 “Orders” 有 4 个分区，分区信息如下：
// Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
// Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
// Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
// Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


// 分区表，收集分区 Partition1 的表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

// 分区表，为所有分区收集表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

// 分区表，收集分区 Partition1 的表级别统计信息和所有列的统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和所有列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，为所有分区收集表级别统计信息和所有列的统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，收集分区 Partition1 的表级别统计信息和分区中指定列(列: amount)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和分区中指定列(列: amount，列: product)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

// 分区表，收集所有分区的表级别统计信息和指定列(列: amount，列: product)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// 注册名为 “Store” 的非分区表
tableEnv.executeSql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " `location` VARCHAR(32)," +
          " `owner` VARCHAR(32)" +
          ") with (...)");

// 注册名为 “Orders” 的分区表
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

// 非分区表，收集表级别的统计信息(表的统计信息主要为行数(row count))。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS");

// 非分区表，收集表级别的统计信息和所有列的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

// 非分区表，收集表级别的统计信息和指定列(列: location)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


// 假设分区表 “Orders” 有 4 个分区，分区信息如下：
// Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
// Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
// Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
// Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


// 分区表，收集分区 Partition1 的表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

// 分区表，为所有分区收集表级别统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

// 分区表，收集分区 Partition1 的表级别统计信息和所有列的统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和所有列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，为所有分区收集表级别统计信息和所有列的统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

// 分区表，收集分区 Partition1 的表级别统计信息和分区中指定列(列: amount)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

// 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和分区中指定列(列: amount，列: product)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

// 分区表，收集所有分区的表级别统计信息和指定列(列: amount，列: product)的列统计信息。
tableEnv.executeSql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 注册名为 “Store” 的非分区表
table_env.execute_sql(
  "CREATE TABLE Store (" +
          " `id` BIGINT NOT NULl," +
          " `location` VARCHAR(32)," +
          " `owner` VARCHAR(32)" +
          ") with (...)");

# 注册名为 “Orders” 的分区表
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

# 非分区表，收集表级别的统计信息(表的统计信息主要为行数(row count))。
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS");

# 非分区表，收集表级别的统计信息和所有列的列统计信息。
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS");

# 非分区表，收集表级别的统计信息和指定列(列: location)的列统计信息。
table_env.execute_sql("ANALYZE TABLE Store COMPUTE STATISTICS FOR COLUMNS location");


# 假设分区表 “Orders” 有 4 个分区，分区信息如下：
# Partition1 : (sold_year='2022', sold_month='1', sold_day='10')
# Partition2 : (sold_year='2022', sold_month='1', sold_day='11')
# Partition3 : (sold_year='2022', sold_month='2', sold_day='10')
# Partition4 : (sold_year='2022', sold_month='2', sold_day='11')


# 分区表，收集分区 Partition1 的表级别统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS");

# 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS");

# 分区表，为所有分区收集表级别统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS");

# 分区表，收集分区 Partition1 的表级别统计信息和所有列的统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR ALL COLUMNS");

# 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和所有列统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

# 分区表，为所有分区收集表级别统计信息和所有列的统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year, sold_month, sold_day) COMPUTE STATISTICS FOR ALL COLUMNS");

# 分区表，收集分区 Partition1 的表级别统计信息和分区中指定列(列: amount)的列统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day='10') COMPUTE STATISTICS FOR COLUMNS amount");

# 分区表，收集分区 Partition1 和 Partition2 的表级别统计信息和分区中指定列(列: amount，列: product)的列统计信息。
table_env.execute_sql("ANALYZE TABLE Orders PARTITION(sold_year='2022', sold_month='1', sold_day) COMPUTE STATISTICS FOR COLUMNS amount, product");

# 分区表，收集所有分区的表级别统计信息和指定列(列: amount，列: product)的列统计信息。
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

<a name="syntax"></a>

## 语法

```sql
ANALYZE TABLE [catalog_name.][db_name.]table_name PARTITION(partcol1[=val1] [, partcol2[=val2], ...]) COMPUTE STATISTICS [FOR COLUMNS col1 [, col2, ...] | FOR ALL COLUMNS]
```
- 对于分区表， 语法中 PARTITION(partcol1[=val1] [, partcol2[=val2], ...]) 是必须指定的
  - 如果没有指定某分区，则会收集所有分区的统计信息
  - 如果指定了某分区，则只会收集该分区的统计信息
  - 如果该表为非分区表，但语句中指定了分区，则会报异常
  - 如果指定了某个分区，但是该分区不存在，则会报异常

- 语法中，FOR COLUMNS col1 [, col2, ...] 或者 FOR ALL COLUMNS 也是可选的
  - 如果没有指定某一列，则只会收集表级别的统计信息
  - 如果指定的列不存在，或者该列不是物理列，则会报异常
  - 如果指定了某一列或者某几列，则会收集列的统计信息
  - 列级别的统计信息包括:
    - ndv: 该列中列值不同的数量
    - nullCount: 该列中空值的数量
    - avgLen: 列值的平均长度
    - maxLen: 列值的最大长度
    - minValue: 列值的最小值
    - maxValue: 列值的最大值
    - valueCount: 该值只应用于 boolean 类型
  - 对于列统计信息，支持类型和对应的列统计信息值如下表所示("Y" 代表支持，"N" 代表不支持):

| 类型                               | `ndv` | `nullCount` | `avgLen` | `maxLen` | `maxValue` | `minValue` | `valueCount` |
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

*注意:* 对于数据值定长的类型(例如：`BOOLEAN`, `INTEGER`, `DOUBLE` 等)， Flink 不会去收集 `avgLen` 和 `maxLen` 值。

{{< top >}}
