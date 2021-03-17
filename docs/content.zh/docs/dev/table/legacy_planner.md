---
title: "Legacy Planner"
weight: 1002
type: docs
aliases:
  - /zh/dev/table/legacy_planner.html
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

# Legacy Planner

Table planners are responsible for translating relational operators into an executable, optimized Flink job.
Flink supports two different planner implementations; the modern planner (sometimes referred to as `Blink`) and the legacy planner.
For production use cases, we recommend the modern planner which is the default.

The legacy planner is in maintenance mode and no longer under active development.
The primary reason to continue using the legacy planner is [DataSet]({{< ref "docs/dev/dataset/overview" >}}) interop.

{{< hint warning >}}
If you are not using the Legacy planner for DataSet interop, the community strongly
encourages you to consider the modern table planner. Both batch and stream processing pipelines
can be expressed in the unified `TableEnvironment`.

** The legacy planner is deprecated and will be dropped in Flink 1.14.**
{{< /hint >}}

This page describes how to use the Legacy planner and where its semantics differ from the 
modern planner. 



## Setup

### Dependencies

When deploying to a cluster, the legacy planner is bundled in Flinks distribution by default.
If you want to run the Table API & SQL programs locally within your IDE, you must add the
following set of modules to your application.

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

### Configuring the TableEnvironment 

When creating a `TableEnvironment` the Legacy planner is configured via the `EnvironmentSettings`.

{{< tabs "95cbb391-0326-4830-b38e-a5871c87d19f" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    // or in batch mode
    //.inBatchMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(settings);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings
    .newInstance()
    .useOldPlanner()
    .inStreamingMode()
    // or in batch mode
    //.inBatchMode()
    .build()

val tEnv = TableEnvironment.create(settings)
```
{{< /tab >}}
{{< /tabs >}}

`BatchTableEnvironment` may used for [DataSet]({{< ref "docs/dev/dataset/overview" >}}) and [DataStream]({{< ref "docs/dev/datastream/overview" >}}) interop respectively.

{{< tabs "3d8abf94-f59c-4756-a5c0-7b645bf43870" >}}
{{< tab "Java" >}}
```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.getExecutionEnvironment()
val tEnv = BatchTableEnvironment.create(env)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment

b_env = ExecutionEnvironment.get_execution_environment()
t_env = BatchTableEnvironment.create(b_env, table_config)
```
{{< /tab >}}
{{< /tabs >}}

## Integration with DataSet

The primary use case for the Legacy planner is interoperation with the DataSet API. 
To translate `DataSet`s to and from tables, applications must use the `BatchTableEnvironment`.

### Create a View from a DataSet

A `DataSet` can be registered in a `BatchTableEnvironment` as a `View`.
The schema of the resulting view depends on the data type of the registered collection.

**Note:** Views created from a `DataSet` can be registered as temporary views only.

{{< tabs "02002c94-df73-4f10-b7b7-6c0f6fbf5909" >}}
{{< tab "Java" >}}
```java
BatchTableEnvironment tEnv = ...; 
DataSet<Tuple2<Long, String>> dataset = ...;

tEnv.createTemporaryView("my-table", dataset, $("myLong"), $("myString"))
```
{{< /tab >}}
{{< tab "Scala" >}}
```java
val tEnv: BatchTableEnvironment = ??? 
val dataset: DataSet[(Long, String)] = ???

tEnv.createTemporaryView("my-table", dataset, $"myLong", $"myString")
```
{{< /tab >}}
{{< /tabs >}}

### Create a Table from a DataSet

A `DataSet` can be directly converted to a `Table` in a `BatchTableEnvironment`.
The schema of the resulting view depends on the data type of the registered collection.

{{< tabs "9bc01f1c-f24c-4be7-9c65-7f6575a12740" >}}
{{< tab "Java" >}}
```java
BatchTableEnvironment tEnv = ...; 
DataSet<Tuple2<Long, String>> dataset = ...;

Table myTable = tEnv.fromDataSet("my-table", dataset, $("myLong"), $("myString"))
```
{{< /tab >}}
{{< tab "Scala" >}}
```java
val tEnv: BatchTableEnvironment = ??? 
val dataset: DataSet[(Long, String)] = ???

val table = tEnv.fromDataSet("my-table", dataset, $"myLong", $"myString")
```
{{< /tab >}}
{{< /tabs >}}

### Convert a Table to a DataSet

A `Table` can be converted to a `DataSet`.
In this way, custom DataSet programs can be run on the result of a Table API or SQL query.

When converting from a `Table`, users must specify the data type of the results.
Often the most convenient conversion type is `Row`.
The following list gives an overview of the features of the different options.

- **Row**: fields are mapped by position, arbitrary number of fields, support for `null` values, no type-safe access.
- **POJO**: fields are mapped by name (POJO fields must be named as `Table` fields), arbitrary number of fields, support for `null` values, type-safe access.
- **Case Class**: fields are mapped by position, no support for `null` values, type-safe access.
- **Tuple**: fields are mapped by position, limitation to 22 (Scala) or 25 (Java) fields, no support for `null` values, type-safe access.
- **Atomic Type**: `Table` must have a single field, no support for `null` values, type-safe access.

{{< tabs "c34bd733-f34e-4780-8cf0-c440465c9f58" >}}
{{< tab "Java" >}}
```java
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

Table table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into a DataSet of Row by specifying a class
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

// Convert the Table into a DataSet of Tuple2<String, Integer> via a TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple = tableEnv.toDataSet(table, tupleType);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = BatchTableEnvironment.create(env)

val table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into a DataSet of Row
val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](table)

// Convert the Table into a DataSet of Tuple2[String, Int]
val dsTuple: DataSet[(String, Int)] = tableEnv.toDataSet[(String, Int)](table)
```
{{< /tab >}}
{{< /tabs >}}

<span class="label label-danger">Attention</span> **Once the Table is converted to a DataSet, we must use the ExecutionEnvironment.execute method to execute the DataSet program.**

## Data Types

The legacy planner, introduced before Flink 1.9, primarily supports type information.
It has only limited support for data types.
It is possible to declare data types that can be translated into type information such that the legacy planner understands them.

The following table summarizes the difference between data type and type information.
Most simple types, as well as the row type remain the same.
Time types, array types, and the decimal type need special attention.
Other hints as the ones mentioned are not allowed.

For the *Type Information* column the table omits the prefix `org.apache.flink.table.api.Types`.

For the *Data Type Representation* column the table omits the prefix `org.apache.flink.table.api.DataTypes`.

| Type Information | Java Expression String | Data Type Representation | Remarks for Data Type |
|:-----------------|:-----------------------|:-------------------------|:----------------------|
| `STRING()` | `STRING` | `STRING()` | |
| `BOOLEAN()` | `BOOLEAN` | `BOOLEAN()` | |
| `BYTE()` | `BYTE` | `TINYINT()` | |
| `SHORT()` | `SHORT` | `SMALLINT()` | |
| `INT()` | `INT` | `INT()` | |
| `LONG()` | `LONG` | `BIGINT()` | |
| `FLOAT()` | `FLOAT` | `FLOAT()` | |
| `DOUBLE()` | `DOUBLE` | `DOUBLE()` | |
| `ROW(...)` | `ROW<...>` | `ROW(...)` | |
| `BIG_DEC()` | `DECIMAL` | [`DECIMAL()`] | Not a 1:1 mapping as precision and scale are ignored and Java's variable precision and scale are used. |
| `SQL_DATE()` | `SQL_DATE` | `DATE()`<br>`.bridgedTo(java.sql.Date.class)` | |
| `SQL_TIME()` | `SQL_TIME` | `TIME(0)`<br>`.bridgedTo(java.sql.Time.class)` | |
| `SQL_TIMESTAMP()` | `SQL_TIMESTAMP` | `TIMESTAMP(3)`<br>`.bridgedTo(java.sql.Timestamp.class)` | |
| `INTERVAL_MONTHS()` | `INTERVAL_MONTHS` | `INTERVAL(MONTH())`<br>`.bridgedTo(Integer.class)` | |
| `INTERVAL_MILLIS()` | `INTERVAL_MILLIS` | `INTERVAL(DataTypes.SECOND(3))`<br>`.bridgedTo(Long.class)` | |
| `PRIMITIVE_ARRAY(...)` | `PRIMITIVE_ARRAY<...>` | `ARRAY(DATATYPE.notNull()`<br>`.bridgedTo(PRIMITIVE.class))` | Applies to all JVM primitive types except for `byte`. |
| `PRIMITIVE_ARRAY(BYTE())` | `PRIMITIVE_ARRAY<BYTE>` | `BYTES()` | |
| `OBJECT_ARRAY(...)` | `OBJECT_ARRAY<...>` | `ARRAY(`<br>`DATATYPE.bridgedTo(OBJECT.class))` | |
| `MULTISET(...)` | | `MULTISET(...)` | |
| `MAP(..., ...)` | `MAP<...,...>` | `MAP(...)` | |
| other generic types | | `RAW(...)` | |

<span class="label label-danger">Attention</span> If there is a problem with the new type system. Users
can fallback to type information defined in `org.apache.flink.table.api.Types` at any time.

## Unsupported Features

The following features are not supported by the legacy planner.

- [Deduplication]({{< ref "docs/dev/table/sql/queries" >}}#deduplication %})
- [Key Value Configurations]({{< ref "docs/dev/table/config" >}}#overview)
- [Streaming Aggregation Optimization]({{< ref "docs/dev/table/tuning" >}})
- Streaming mode Grouping sets, Rollup and Cube aggregations
- [Top-N]({{< ref "docs/dev/table/sql/queries" >}}#top-n)
- [Versioned Tables]({{< ref "docs/dev/table/concepts/versioned_tables" >}})

## Unsupported Built-In Functions

The following built-in functions are not supported by the legacy planner.

- `PI`
- `ASCII(string)`
- `CHR(integer)`
- `DECODE(binary, string)`
- `ENCODE(string1, string2)`
- `INSTR(string1, string2)`
- `LEFT(string, integer)`
- `RIGHT(string, integer)`
- `LOCATE(string1, string2[, integer])`
- `PARSE_URL(string1, string2[, string3])`
- `REGEXP(string1, string2)`
- `REVERSE(string)`
- `SPLIT_INDEX(string1, string2, integer1)`
- `STR_TO_MAP(string1[, string2, string3]])`
- `SUBSTR(string[, integer1[, integer2]])`
- `CONVERT_TZ(string1, string2, string3)`
- `FROM_UNIXTIME(numeric[, string])`
- `UNIX_TIMESTAMP()`
- `UNIX_TIMESTAMP(string1[, string2])`
- `TO_DATE(string1[, string2])`
- `TO_TIMESTAMP(string1[, string2])`
- `NOW()`
- `IF(condition, true_value, false_value)`
- `IS_ALPHA(string)`
- `IS_DECIMAL(string)`
- `IS_DIGIT(string)`
- `VARIANCE([ ALL | DISTINCT ] expression)`
- `RANK()`
- `DENSE_RANK()`
- `ROW_NUMBER()`
- `LEAD(expression [, offset] [, default] )`
- `LAG(expression [, offset] [, default])`
- `FIRST_VALUE(expression)`
- `LAST_VALUE(expression)`
- `LISTAGG(expression [, separator])`

{{< hint danger >}}
`DATE_FORMAT(timestamp, string)` is available in the legacy planner but has serious bugs and should not be used.
Please implement a custom UDF instead or use `EXTRACT` as a workaround.
{{< /hint >}}



