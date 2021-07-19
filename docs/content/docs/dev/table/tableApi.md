---
title: "Table API"
weight: 31
type: docs
aliases:
  - /dev/table/tableApi.html
  - /dev/table_api.html
  - /apis/table.html
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

# Table API

The Table API is a unified, relational API for stream and batch processing. Table API queries can be run on batch or streaming input without modifications. The Table API is a super set of the SQL language and is specially designed for working with Apache Flink. The Table API is a language-integrated API for Scala, Java and Python. Instead of specifying queries as String values as common with SQL, Table API queries are defined in a language-embedded style in Java, Scala or Python with IDE support like autocompletion and syntax validation. 

The Table API shares many concepts and parts of its API with Flink's SQL integration. Have a look at the [Common Concepts & API]({{< ref "docs/dev/table/common" >}}) to learn how to register tables or to create a `Table` object. The [Streaming Concepts]({{< ref "docs/dev/table/concepts/overview" >}}) pages discuss streaming specific concepts such as dynamic tables and time attributes.

The following examples assume a registered table called `Orders` with attributes `(a, b, c, rowtime)`. The `rowtime` field is either a logical [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}) in streaming or a regular timestamp field in batch.


Overview & Examples
-----------------------------

The Table API is available for Scala, Java and Python. The Scala Table API leverages on Scala expressions, the Java Table API supports both Expression DSL and strings which are parsed and converted into equivalent expressions, the Python Table API currently only supports strings which are parsed and converted into equivalent expressions.

The following example shows the differences between the Scala, Java and Python Table API. The table program is executed in a batch environment. It scans the `Orders` table, groups by field `a`, and counts the resulting rows per group.

{{< tabs "8ffbc88b-54d7-4936-9a53-b63cb22c4a56" >}}
{{< tab "Java" >}}

The Java Table API is enabled by importing `org.apache.flink.table.api.java.*`. The following example shows how a Java Table API program is constructed and how expressions are specified as strings.
For the Expression DSL it is also necessary to import static `org.apache.flink.table.api.Expressions.*`

```java
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(settings);

// register Orders table in table environment
// ...

// specify table program
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table counts = orders
        .groupBy($("a"))
        .select($("a"), $("b").count().as("cnt"));

// conversion to DataSet
DataSet<Row> result = tEnv.toDataSet(counts, Row.class);
result.print();
```

{{< /tab >}}
{{< tab "Scala" >}}

The Scala Table API is enabled by importing `org.apache.flink.table.api._`, `org.apache.flink.api.scala._`, and `org.apache.flink.table.api.bridge.scala._` (for bridging to/from DataStream).

The following example shows how a Scala Table API program is constructed. Table fields are referenced using Scala's String interpolation using a dollar character (`$`).

```scala
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

// environment configuration
val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build();

val tEnv = TableEnvironment.create(settings);

// register Orders table in table environment
// ...

// specify table program
val orders = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result = orders
               .groupBy($"a")
               .select($"a", $"b".count as "cnt")
               .toDataSet[Row] // conversion to DataSet
               .print()
```

{{< /tab >}}
{{< tab "Python" >}}

The following example shows how a Python Table API program is constructed and how expressions are specified as strings.

```python
from pyflink.table import *

# environment configuration
t_env = BatchTableEnvironment.create(
    environment_settings=EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build())

# register Orders table and Result table sink in table environment
source_data_path = "/path/to/source/directory/"
result_data_path = "/path/to/result/directory/"
source_ddl = f"""
        create table Orders(
            a VARCHAR,
            b BIGINT,
            c BIGINT,
            rowtime TIMESTAMP(3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path}'
        )
        """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    create table `Result`(
        a VARCHAR,
        cnt BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

orders.group_by("a").select(orders.a, orders.b.count.alias('cnt')).execute_insert("result").wait()

```

{{< /tab >}}
{{< /tabs >}}

The next example shows a more complex Table API program. The program scans again the `Orders` table. It filters null values, normalizes the field `a` of type String, and calculates for each hour and product `a` the average billing amount `b`.

{{< tabs "6df651bc-ae06-44de-a36c-6a1e6d1b7355" >}}
{{< tab "Java" >}}

```java
// environment configuration
// ...

// specify table program
Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

Table result = orders
        .filter(
            and(
                $("a").isNotNull(),
                $("b").isNotNull(),
                $("c").isNotNull()
            ))
        .select($("a").lowerCase().as("a"), $("b"), $("rowtime"))
        .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"))
        .groupBy($("hourlyWindow"), $("a"))
        .select($("a"), $("hourlyWindow").end().as("hour"), $("b").avg().as("avgBillingAmount"));
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// environment configuration
// ...

// specify table program
val orders: Table = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result: Table = orders
        .filter($"a".isNotNull && $"b".isNotNull && $"c".isNotNull)
        .select($"a".lowerCase() as "a", $"b", $"rowtime")
        .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
        .groupBy($"hourlyWindow", $"a")
        .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")
```

{{< /tab >}}
{{< tab "Python" >}}

```python
# specify table program
from pyflink.table.expressions import col, lit

orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

result = orders.filter(orders.a.is_not_null & orders.b.is_not_null & orders.c.is_not_null) \
               .select(orders.a.lower_case.alias('a'), orders.b, orders.rowtime) \
               .window(Tumble.over(lit(1).hour).on(orders.rowtime).alias("hourly_window")) \
               .group_by(col('hourly_window'), col('a')) \
               .select(col('a'), col('hourly_window').end.alias('hour'), b.avg.alias('avg_billing_amount'))
```

{{< /tab >}}
{{< /tabs >}}

Since the Table API is a unified API for batch and streaming data, both example programs can be executed on batch and streaming inputs without any modification of the table program itself. In both cases, the program produces the same results given that streaming records are not late (see [Streaming Concepts]({{< ref "docs/dev/table/concepts/overview" >}}) for details).

{{< top >}}

Operations
----------

The Table API supports the following operations. Please note that not all operations are available in both batch and streaming yet; they are tagged accordingly.

### Scan, Projection, and Filter

#### From

{{< label "Batch" >}} {{< label "Streaming" >}}

Similar to the `FROM` clause in a SQL query.
Performs a scane of registered table.

{{< tabs "from" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
```
{{< /tab >}}
{{< /tabs >}}

#### FromValues

{{< label "Batch" >}} {{< label "Streaming" >}}

Similar to the `VALUES` clause in a SQL query.
Produces an inline table out of the provided rows.

You can use a `row(...)` expression to create composite rows:

{{< tabs "fromvalues" >}}
{{< tab "Java" >}}
```java
Table table = tEnv.fromValues(
   row(1, "ABC"),
   row(2L, "ABCDE")
);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table = tEnv.fromValues(
   row(1, "ABC"),
   row(2L, "ABCDE")
)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table = t_env.from_elements([(1, 'ABC'), (2, 'ABCDE')])
```
{{< /tab >}}
{{< /tabs >}}

will produce a Table with a schema as follows:

```
root
|-- f0: BIGINT NOT NULL     // original types INT and BIGINT are generalized to BIGINT
|-- f1: VARCHAR(5) NOT NULL // original types CHAR(3) and CHAR(5) are generalized
                            // to VARCHAR(5). VARCHAR is used instead of CHAR so that
                            // no padding is applied
```

The method will derive the types automatically from the input expressions. If types at a certain position differ, the method will try to find a common super type for all types. If a common super type does not exist, an exception will be thrown.

You can also specify the requested type explicitly. It might be helpful for assigning more generic types like e.g. DECIMAL or naming the columns.

{{< tabs "fromvalueswithtype" >}}
{{< tab "Java" >}}
```java
Table table = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table = t_env.from_elements(
  [(1, 'ABC'), (2, 'ABCDE')],
  schema=DataTypes.Row([DataTypes.FIELD('id', DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD('name', DataTypes.STRING())]))
```
{{< /tab >}}
{{< /tabs >}}

will produce a Table with the following schema: 

```
root
|-- id: DECIMAL(10, 2)
|-- name: STRING
```

#### Select

{{< label "Batch" >}} {{< label "Streaming" >}}

Similar to a SQL `SELECT` statement. 
Performs a select operation.

{{< tabs "select" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.select($("a"), $("c").as("d"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
Table result = orders.select($"a", $"c" as "d");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.select(orders.a, orders.c.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

You can use star `(*)` to act as a wild card, selecting all of the columns in the table.

{{< tabs "selectstar" >}}
{{< tab "Java" >}}
```java
Table result = orders.select($("*"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
Table result = orders.select($"*")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

result = orders.select(col("*"))
```
{{< /tab >}}
{{< /tabs >}}

#### As

{{< label "Batch" >}} {{< label "Streaming" >}}

Renames fields.

{{< tabs "as" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.as("x, y, z, t");
```
{{< /tab >}}
{{< tab "scala" >}}
```java
val orders: Table = tableEnv.from("Orders").as("x", "y", "z", "t")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.alias("x, y, z, t")
```
{{< /tab >}}
{{< /tabs >}}

####  Where / Filter

{{< label "Batch" >}} {{< label "Streaming" >}}

Similar to a SQL `WHERE` clause.
Filters out rows that do not pass the filter predicate.

{{< tabs "where" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.where($("b").isEqual("red"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.filter($"a" % 2 === 0)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.where(orders.a == 'red')
```
{{< /tab >}}
{{< /tabs >}}

Or

{{< tabs "filter" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.filter($("b").isEqual("red"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.filter($"a" % 2 === 0)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.filter(orders.a == 'red')
```
{{< /tab >}}
{{< /tabs >}}


### Column Operations

#### AddColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

Performs a field add operation.
It will throw an exception if the added fields already exist.

{{< tabs "addcolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.addColumns(concat($("c"), "sunny"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.addColumns(concat($"c", "Sunny"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import concat

orders = t_env.from_path("Orders")
result = orders.add_columns(concat(orders.c, 'sunny'))
```
{{< /tab >}}
{{< /tabs >}}

#### AddOrReplaceColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

Performs a field add operation.
Existing fields will be replaced if the added column name is the same as the existing column name.
Moreover, if the added fields have duplicate field name, then the last one is used. 

{{< tabs "addorreplacecolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.addOrReplaceColumns(concat($("c"), "sunny").as("desc"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.addOrReplaceColumns(concat($"c", "Sunny") as "desc")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import concat

orders = t_env.from_path("Orders")
result = orders.add_or_replace_columns(concat(orders.c, 'sunny').alias('desc'))
```
{{< /tab >}}
{{< /tabs >}}

#### DropColumns 

{{< label "Batch" >}} {{< label "Streaming" >}}

{{< tabs "dropcolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.dropColumns($("b"), $("c"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.dropColumns($"b", $"c")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.drop_columns(orders.b, orders.c)
```
{{< /tab >}}
{{< /tabs >}}

#### RenameColumns

{{< label "Batch" >}} {{< label "Streaming" >}}

Performs a field rename operation.
The field expressions should be alias expressions, and only the existing fields can be renamed.

{{< tabs "renamecolumns" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.renameColumns($("b").as("b2"), $("c").as("c2"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders");
val result = orders.renameColumns($"b" as "b2", $"c" as "c2")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.rename_columns(orders.b.alias('b2'), orders.c.alias('c2'))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Aggregations

#### GroupBy Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

Similar to a SQL `GROUP BY` clause.
Groups the rows on the grouping keys with a following running aggregation operator to aggregate rows group-wise.

{{< tabs "groupby" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.groupBy($("a")).select($("a"), $("b").sum().as("d"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.groupBy($"a").select($"a", $"b".sum().as("d"))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.group_by(orders.a).select(orders.a, orders.b.sum.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### GroupBy Window Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}

Groups and aggregates a table on a [group window](#group-windows) and possibly one or more grouping keys.

{{< tabs "groupbywindow" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders
    .window(Tumble.over(lit(5).minutes()).on($("rowtime")).as("w")) // define window
    .groupBy($("a"), $("w")) // group by key and window
    // access window properties and aggregate
    .select(
        $("a"),
        $("w").start(),
        $("w").end(),
        $("w").rowtime(),
        $("b").sum().as("d")
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w") // define window
    .groupBy($"a", $"w") // group by key and window
    .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".sum as "d") // access window properties and aggregate
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col

orders = t_env.from_path("Orders")
result = orders.window(Tumble.over(lit(5).minutes).on(orders.rowtime).alias("w")) \ 
               .group_by(orders.a, col('w')) \
               .select(orders.a, col('w').start, col('w').end, orders.b.sum.alias('d'))
```
{{< /tab >}}
{{< /tabs >}}

#### Over Window Aggregation

Similar to a SQL `OVER` clause.
Over window aggregates are computed for each row, based on a window (range) of preceding and succeeding rows.
See the [over windows section](#over-windows) for more details.

{{< tabs "overwindowagg" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders
    // define window
    .window(
        Over
          .partitionBy($("a"))
          .orderBy($("rowtime"))
          .preceding(UNBOUNDED_RANGE)
          .following(CURRENT_RANGE)
          .as("w"))
    // sliding aggregate
    .select(
        $("a"),
        $("b").avg().over($("w")),
        $("b").max().over($("w")),
        $("b").min().over($("w"))
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    // define window
    .window(
        Over
          partitionBy $"a"
          orderBy $"rowtime"
          preceding UNBOUNDED_RANGE
          following CURRENT_RANGE
          as "w")
    .select($"a", $"b".avg over $"w", $"b".max().over($"w"), $"b".min().over($"w")) // sliding aggregate
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.window import Over
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE

orders = t_env.from_path("Orders")
result = orders.over_window(Over.partition_by(orders.a).order_by(orders.rowtime)
                            .preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE)
                            .alias("w")) \
               .select(orders.a, orders.b.avg.over(col('w')), orders.b.max.over(col('w')), orders.b.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}).

#### Distinct Aggregation

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

Similar to a SQL DISTINCT aggregation clause such as `COUNT(DISTINCT a)`.
Distinct aggregation declares that an aggregation function (built-in or user-defined) is only applied on distinct input values.
Distinct can be applied to **GroupBy Aggregation**, **GroupBy Window Aggregation** and **Over Window Aggregation**.

{{< tabs "distinctagg" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
// Distinct aggregation on group by
Table groupByDistinctResult = orders
    .groupBy($("a"))
    .select($("a"), $("b").sum().distinct().as("d"));
// Distinct aggregation on time window group by
Table groupByWindowDistinctResult = orders
    .window(Tumble
            .over(lit(5).minutes())
            .on($("rowtime"))
            .as("w")
    )
    .groupBy($("a"), $("w"))
    .select($("a"), $("b").sum().distinct().as("d"));
// Distinct aggregation on over window
Table result = orders
    .window(Over
        .partitionBy($("a"))
        .orderBy($("rowtime"))
        .preceding(UNBOUNDED_RANGE)
        .as("w"))
    .select(
        $("a"), $("b").avg().distinct().over($("w")),
        $("b").max().over($("w")),
        $("b").min().over($("w"))
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders");
// Distinct aggregation on group by
val groupByDistinctResult = orders
    .groupBy($"a")
    .select($"a", $"b".sum.distinct as "d")
// Distinct aggregation on time window group by
val groupByWindowDistinctResult = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w").groupBy($"a", $"w")
    .select($"a", $"b".sum.distinct as "d")
// Distinct aggregation on over window
val result = orders
    .window(Over
        partitionBy $"a"
        orderBy $"rowtime"
        preceding UNBOUNDED_RANGE
        as $"w")
    .select($"a", $"b".avg.distinct over $"w", $"b".max over $"w", $"b".min over $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col, lit, UNBOUNDED_RANGE

orders = t_env.from_path("Orders")
# Distinct aggregation on group by
group_by_distinct_result = orders.group_by(orders.a) \
                                 .select(orders.a, orders.b.sum.distinct.alias('d'))
# Distinct aggregation on time window group by
group_by_window_distinct_result = orders.window(
    Tumble.over(lit(5).minutes).on(orders.rowtime).alias("w")).group_by(orders.a, col('w')) \
    .select(orders.a, orders.b.sum.distinct.alias('d'))
# Distinct aggregation on over window
result = orders.over_window(Over
                       .partition_by(orders.a)
                       .order_by(orders.rowtime)
                       .preceding(UNBOUNDED_RANGE)
                       .alias("w")) \
                       .select(orders.a, orders.b.avg.distinct.over(col('w')), orders.b.max.over(col('w')), orders.b.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

User-defined aggregation function can also be used with `DISTINCT` modifiers. To calculate the aggregate results only for distinct values, simply add the distinct modifier towards the aggregation function. 

{{< tabs "distinctudf" >}}
{{< tab "Java" >}}
```java
Table orders = tEnv.from("Orders");

// Use distinct aggregation for user-defined aggregate functions
tEnv.registerFunction("myUdagg", new MyUdagg());
orders.groupBy("users")
    .select(
        $("users"),
        call("myUdagg", $("points")).distinct().as("myDistinctResult")
    );
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tEnv.from("Orders");

// Use distinct aggregation for user-defined aggregate functions
val myUdagg = new MyUdagg();
orders.groupBy($"users").select($"users", myUdagg.distinct($"points") as "myDistinctResult");
```
{{< /tab >}}
{{< tab "Python" >}}
Unsupported
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Distinct

{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

Similar to a SQL `DISTINCT` clause.
Returns records with distinct value combinations.

{{< tabs "distinct" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
Table result = orders.distinct();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.distinct()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
result = orders.distinct()
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

{{< top >}}

### Joins

#### Inner Join

{{< label "Batch" >}} {{< label "Streaming" >}}

Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined through join operator or using a where or filter operator.

{{< tabs "innerjoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.join(right)
    .where($("a").isEqual($("d")))
    .select($("a"), $("b"), $("e"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = ds1.toTable(tableEnv, $"a", $"b", $"c")
val right = ds2.toTable(tableEnv, $"d", $"e", $"f")
val result = left.join(right).where($"a" === $"d").select($"a", $"b", $"e")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'))
result = left.join(right).where(left.a == right.d).select(left.a, left.b, right.e)
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Outer Join
{{< label "Batch" >}} {{< label "Streaming" >}}
{{< label "Result Updating" >}}

Similar to SQL `LEFT`/`RIGHT`/`FULL OUTER JOIN` clauses.
Joins two tables.
Both tables must have distinct field names and at least one equality join predicate must be defined.

{{< tabs "outerjoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");

Table leftOuterResult = left.leftOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
Table rightOuterResult = left.rightOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
Table fullOuterResult = left.fullOuterJoin(right, $("a").isEqual($("d")))
                            .select($("a"), $("b"), $("e"));
```                           
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.fromDataSet(ds1, $"a", $"b", $"c")
val right = tableEnv.fromDataSet(ds2, $"d", $"e", $"f")

val leftOuterResult = left.leftOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val rightOuterResult = left.rightOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val fullOuterResult = left.fullOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'))

left_outer_result = left.left_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
right_outer_result = left.right_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
full_outer_result = left.full_outer_join(right, left.a == right.d).select(left.a, left.b, right.e)
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

#### Interval Join

{{< label "Batch" >}} {{< label "Streaming" >}}

Interval joins are a subset of regular joins that can be processed in a streaming fashion.

An interval join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (`<, <=, >=, >`) or a single equality predicate that compares time attributes of the same type (i.e., processing time or event time) of both input tables.

{{< tabs "intervaljoin" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.fromDataSet(ds1, $("a"), $("b"), $("c"), $("ltime").rowtime());
Table right = tableEnv.fromDataSet(ds2, $("d"), $("e"), $("f"), $("rtime").rowtime()));

Table result = left.join(right)
  .where(
    and(
        $("a").isEqual($("d")),
        $("ltime").isGreaterOrEqual($("rtime").minus(lit(5).minutes())),
        $("ltime").isLess($("rtime").plus(lit(10).minutes()))
    ))
  .select($("a"), $("b"), $("e"), $("ltime"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = ds1.toTable(tableEnv, $"a", $"b", $"c", $"ltime".rowtime)
val right = ds2.toTable(tableEnv, $"d", $"e", $"f", $"rtime".rowtime)

val result = left.join(right)
  .where($"a" === $"d" && $"ltime" >= $"rtime" - 5.minutes && $"ltime" < $"rtime" + 10.minutes)
  .select($"a", $"b", $"e", $"ltime")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'), col('rowtime1'))
right = t_env.from_path("Source2").select(col('d'), col('e'), col('f'), col('rowtime2'))
  
joined_table = left.join(right).where((left.a == right.d) & (left.rowtime1 >= right.rowtime2 - lit(1).second) & (left.rowtime1 <= right.rowtime2 + lit(2).seconds))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.e, joined_table.rowtime1)
```
{{< /tab >}}
{{< /tabs >}}

#### Inner Join with Table Function (UDTF)

{{< label "Batch" >}} {{< label "Streaming" >}}

Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function.
A row of the left (outer) table is dropped, if its table function call returns an empty result. 

{{< tabs "udtf" >}}
{{< tab "Java" >}}
```java
// register User-Defined Table Function
TableFunction<Tuple3<String,String,String>> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.from("Orders");
Table result = orders
    .joinLateral(call("split", $("c")).as("s", "t", "v"))
    .select($("a"), $("b"), $("s"), $("t"), $("v"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .joinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# register User-Defined Table Function
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def split(x):
    return [Row(1, 2, 3)]

# join
orders = t_env.from_path("Orders")
joined_table = orders.join_lateral(split(orders.c).alias("s, t, v"))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.s, joined_table.t, joined_table.v)
```
{{< /tab >}}
{{< /tabs >}}


####  Left Outer Join with Table Function (UDTF)

{{< label "Batch" >}} {{< label "Streaming" >}}

Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. If a table function call returns an empty result, the corresponding outer row is preserved and the result padded with null values.

Currently, the predicate of a table function left outer join can only be empty or literal true.

{{< tabs "outerudtf" >}}
{{< tab "Java" >}}
```java
// register User-Defined Table Function
TableFunction<Tuple3<String,String,String>> split = new MySplitUDTF();
tableEnv.registerFunction("split", split);

// join
Table orders = tableEnv.from("Orders");
Table result = orders
    .leftOuterJoinLateral(call("split", $("c")).as("s", "t", "v"))
    .select($("a"), $("b"), $("s"), $("t"), $("v"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .leftOuterJoinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# register User-Defined Table Function
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def split(x):
    return [Row(1, 2, 3)]

# join
orders = t_env.from_path("Orders")
joined_table = orders.left_outer_join_lateral(split(orders.c).alias("s, t, v"))
result = joined_table.select(joined_table.a, joined_table.b, joined_table.s, joined_table.t, joined_table.v)
```
{{< /tab >}}
{{< /tabs >}}

#### Join with Temporal TAble

Temporal tables are tables that track changes over time.

A temporal table function provides access to the state of a temporal table at a specific point in time. The syntax to join a table with a temporal table function is the same as in Inner Join with Table Function.

Currently only inner joins with temporal tables are supported.

{{< tabs "temporaltablefunc" >}}
{{< tab "Java" >}}
```java
Table ratesHistory = tableEnv.from("RatesHistory");

// register temporal table function with a time attribute and primary key
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
    "r_proctime",
    "r_currency");
tableEnv.registerFunction("rates", rates);

// join with "Orders" based on the time attribute and key
Table orders = tableEnv.from("Orders");
Table result = orders
    .joinLateral(call("rates", $("o_proctime")), $("o_currency").isEqual($("r_currency")));
{{< /tab >}}
{{< tabs "Scala" >}}
```scala
val ratesHistory = tableEnv.from("RatesHistory")

// register temporal table function with a time attribute and primary key
val rates = ratesHistory.createTemporalTableFunction($"r_proctime", $"r_currency")

// join with "Orders" based on the time attribute and key
val orders = tableEnv.from("Orders")
val result = orders
    .joinLateral(rates($"o_rowtime"), $"r_currency" === $"o_currency")
```
{{< /tabs >}}
{{< tab "Python" >}}
Currently not supported in Python Table API.
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Set Operations

#### Union

{{< label Batch >}}

Similar to a SQL `UNION` clause. Unions two tables with duplicate records removed. Both tables must have identical field types.

{{< tabs "union" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.union(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.union(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.union(right)
{{< /tab >}}
{{< /tabs >}}

#### UnionAll

{{< label Batch >}} {{< label Streaming >}}


Similar to a SQL `UNION ALL` clause. Unions two tables.
Both tables must have identical field types.

{{< tabs "unionall" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.unionAl(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.unionAl(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.unionAl(right)
{{< /tab >}}
{{< /tabs >}}

#### Intersect 

{{< label Batch >}}

Similar to a SQL `INTERSECT` clause. Intersect returns records that exist in both tables. If a record is present one or both tables more than once, it is returned just once, i.e., the resulting table has no duplicate records. Both tables must have identical field types.

{{< tabs "intersect" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.intersect(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersect(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.intersect(right)
{{< /tab >}}
{{< /tabs >}}

#### IntersectAll

{{< label Batch >}}

Similar to a SQL `INTERSECT ALL` clause. IntersectAll returns records that exist in both tables. If a record is present in both tables more than once, it is returned as many times as it is present in both tables, i.e., the resulting table might have duplicate records. Both tables must have identical field types.

{{< tabs "intersectAll" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.intersectAll(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersectAll(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.intersectAll(right)
{{< /tab >}}
{{< /tabs >}}

#### Minus 

{{< label Batch  >}}

Similar to a SQL `EXCEPT` clause. Minus returns records from the left table that do not exist in the right table. Duplicate records in the left table are returned exactly once, i.e., duplicates are removed. Both tables must have identical field types.

{{< tabs "minus" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.minus(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minus(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.minus(right)
{{< /tab >}}
{{< /tabs >}}

#### MinusAll
{{< label Batch >}}

Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in the right table. A record that is present n times in the left table and m times in the right table is returned (n - m) times, i.e., as many duplicates as are present in the right table are removed. Both tables must have identical field types.

{{< tabs "minusall" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("orders1");
Table right = tableEnv.from("orders2");

left.minusAll(right);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minusAll(right)
```
{{< /tab >}}
{{< tab >}}
left = tableEnv.from_path("orders1")
right = tableEnv.from_path("orders2")

left.minusAll(right)
{{< /tab >}}
{{< /tabs >}}

#### In 

{{< label Batch >}} {{< label Streaming >}}

Similar to a SQL `IN` clause. In returns true if an expression exists in a given table sub-query. The sub-query table must consist of one column. This column must have the same data type as the expression.

{{< tabs "in" >}}
{{< tab "Java" >}}
```java
Table left = tableEnv.from("Orders1")
Table right = tableEnv.from("Orders2");

Table result = left.select($("a"), $("b"), $("c")).where($("a").in(right));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val left = tableEnv.from("Orders1")
val right = tableEnv.from("Orders2");

val result = left.select($"a", $"b", $"c").where($"a".in(right))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
left = t_env.from_path("Source1").select(col('a'), col('b'), col('c'))
right = t_env.from_path("Source2").select(col('a'))

result = left.select(left.a, left.b, left.c).where(left.a.in_(right))
```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

{{< top >}}

### OrderBy, Offset & Fetch

#### Order By 

{{< label Batch >}} {{< label Streaming >}}

Similar to a SQL `ORDER BY` clause. Returns records globally sorted across all parallel partitions. For unbounded tables, this operation requires a sorting on a time attribute or a subsequent fetch operation.

{{< tabs "orderby" >}}
{{< tab "Java" >}}
```java
Table result = in.orderBy($("a").asc());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val result = in.orderBy($"a".asc)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
result = in.order_by(in.a.asc)
```
{{< /tab >}}
{{< /tabs >}}

#### Offset & Fetch 

{{< label Batch >}} {{< label Streaming >}}

Similar to the SQL `OFFSET` and `FETCH` clauses. The offset operation limits a (possibly sorted) result from an offset position. The fetch operation limits a (possibly sorted) result to the first n rows. Usually, the two operations are preceded by an ordering operator. For unbounded tables, a fetch operation is required for an offset operation.

{{< tabs "offsetfetch" >}}
{{< tab "Java" >}}
```java
// returns the first 5 records from the sorted result
Table result1 = in.orderBy($("a").asc()).fetch(5);

// skips the first 3 records and returns all following records from the sorted result
Table result2 = in.orderBy($("a").asc()).offset(3);

// skips the first 10 records and returns the next 5 records from the sorted result
Table result3 = in.orderBy($("a").asc()).offset(10).fetch(5);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

// returns the first 5 records from the sorted result
val result1: Table = in.orderBy($"a".asc).fetch(5)

// skips the first 3 records and returns all following records from the sorted result
val result2: Table = in.orderBy($"a".asc).offset(3)

// skips the first 10 records and returns the next 5 records from the sorted result
val result3: Table = in.orderBy($"a".asc).offset(10).fetch(5)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# returns the first 5 records from the sorted result
result1 = table.order_by(table.a.asc).fetch(5)

# skips the first 3 records and returns all following records from the sorted result
result2 = table.order_by(table.a.asc).offset(3)

# skips the first 10 records and returns the next 5 records from the sorted result
result3 = table.order_by(table.a.asc).offset(10).fetch(5)
```
{{< /tab >}}
{{< /tabs >}}

### Insert

{{< label Batch >}} {{< label Streaming >}}

Similar to the `INSERT INTO` clause in a SQL query, the method performs an insertion into a registered output table. The `executeInsert()` method will immediately submit a Flink job which execute the insert operation.

Output tables must be registered in the TableEnvironment (see Connector tables). Moreover, the schema of the registered table must match the schema of the query.

{{< tabs "insertinto" >}}
{{< tab "Java" >}}
```java
Table orders = tableEnv.from("Orders");
orders.executeInsert("OutOrders");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val orders = tableEnv.from("Orders")
orders.executeInsert("OutOrders")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
orders = t_env.from_path("Orders")
orders.execute_insert("OutOrders")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Group Windows

Group window aggregates group rows into finite groups based on time or row-count intervals and evaluate aggregation functions once per group. For batch tables, windows are a convenient shortcut to group records by time intervals.

{{< tabs "248a1eb3-c75a-404e-957e-08a012cbed51" >}}
{{< tab "Java" >}}
Windows are defined using the `window(GroupWindow w)` clause and require an alias, which is specified using the `as` clause. In order to group a table by a window, the window alias must be referenced in the `groupBy(...)` clause like a regular grouping attribute. 
The following example shows how to define a window aggregation on a table.

```java
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy($("w"))  // group the table by window w
  .select($("b").sum());  // aggregate
```
{{< /tab >}}
{{< tab "Scala" >}}
Windows are defined using the `window(w: GroupWindow)` clause and require an alias, which is specified using the `as` clause. In order to group a table by a window, the window alias must be referenced in the `groupBy(...)` clause like a regular grouping attribute. 
The following example shows how to define a window aggregation on a table.

```scala
val table = input
  .window([w: GroupWindow] as $"w")  // define window with alias w
  .groupBy($"w")   // group the table by window w
  .select($"b".sum)  // aggregate
```
{{< /tab >}}
{{< tab "Python" >}}
Windows are defined using the `window(w: GroupWindow)` clause and require an alias, which is specified using the `alias` clause. In order to group a table by a window, the window alias must be referenced in the `group_by(...)` clause like a regular grouping attribute. 
The following example shows how to define a window aggregation on a table.

```python
# define window with alias w, group the table by window w, then aggregate
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w')).select(input.b.sum)
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "3a855a09-96d3-4dd5-9cbe-07b7f3dc4af9" >}}
{{< tab "Java" >}}
In streaming environments, window aggregates can only be computed in parallel if they group on one or more attributes in addition to the window, i.e., the `groupBy(...)` clause references a window alias and at least one additional attribute. A `groupBy(...)` clause that only references a window alias (such as in the example above) can only be evaluated by a single, non-parallel task.
The following example shows how to define a window aggregation with additional grouping attributes.

```java
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy($("w"), $("a"))  // group the table by attribute a and window w
  .select($("a"), $("b").sum());  // aggregate
```
{{< /tab >}}
{{< tab "Scala" >}}
In streaming environments, window aggregates can only be computed in parallel if they group on one or more attributes in addition to the window, i.e., the `groupBy(...)` clause references a window alias and at least one additional attribute. A `groupBy(...)` clause that only references a window alias (such as in the example above) can only be evaluated by a single, non-parallel task.
The following example shows how to define a window aggregation with additional grouping attributes.

```scala
val table = input
  .window([w: GroupWindow] as $"w") // define window with alias w
  .groupBy($"w", $"a")  // group the table by attribute a and window w
  .select($"a", $"b".sum)  // aggregate
```
{{< /tab >}}
{{< tab "Python" >}}
In streaming environments, window aggregates can only be computed in parallel if they group on one or more attributes in addition to the window, i.e., the `group_by(...)` clause references a window alias and at least one additional attribute. A `group_by(...)` clause that only references a window alias (such as in the example above) can only be evaluated by a single, non-parallel task.
The following example shows how to define a window aggregation with additional grouping attributes.

```python
# define window with alias w, group the table by attribute a and window w,
# then aggregate
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w'), input.a).select(input.b.sum)
```
{{< /tab >}}
{{< /tabs >}}

Window properties such as the start, end, or rowtime timestamp of a time window can be added in the select statement as a property of the window alias as `w.start`, `w.end`, and `w.rowtime`, respectively. The window start and rowtime timestamps are the inclusive lower and upper window boundaries. In contrast, the window end timestamp is the exclusive upper window boundary. For example a tumbling window of 30 minutes that starts at 2pm would have `14:00:00.000` as start timestamp, `14:29:59.999` as rowtime timestamp, and `14:30:00.000` as end timestamp.

{{< tabs "1397cfe2-8ed8-4a39-938c-f2c066c2bdcf" >}}
{{< tab "Java" >}}
```java
Table table = input
  .window([GroupWindow w].as("w"))  // define window with alias w
  .groupBy($("w"), $("a"))  // group the table by attribute a and window w
  .select($("a"), $("w").start(), $("w").end(), $("w").rowtime(), $("b").count()); // aggregate and add window start, end, and rowtime timestamps
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = input
  .window([w: GroupWindow] as $"w")  // define window with alias w
  .groupBy($"w", $"a")  // group the table by attribute a and window w
  .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".count) // aggregate and add window start, end, and rowtime timestamps
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# define window with alias w, group the table by attribute a and window w,
# then aggregate and add window start, end, and rowtime timestamps
table = input.window([w: GroupWindow].alias("w")) \
             .group_by(col('w'), input.a) \
             .select(input.a, col('w').start, col('w').end, col('w').rowtime, input.b.count)
```
{{< /tab >}}
{{< /tabs >}}

The `Window` parameter defines how rows are mapped to windows. `Window` is not an interface that users can implement. Instead, the Table API provides a set of predefined `Window` classes with specific semantics, which are translated into underlying `DataStream` or `DataSet` operations. The supported window definitions are listed below.

#### Tumble (Tumbling Windows)

A tumbling window assigns rows to non-overlapping, continuous windows of fixed length. For example, a tumbling window of 5 minutes groups rows in 5 minutes intervals. Tumbling windows can be defined on event-time, processing-time, or on a row-count.

{{< tabs "96f964cc-d78c-493b-b190-19cab37a8031" >}}
{{< tab "Java" >}}
Tumbling windows are defined by using the `Tumble` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```java
// Tumbling Event-time Window
.window(Tumble.over(lit(10).minutes()).on($("rowtime")).as("w"));

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(lit(10).minutes()).on($("proctime")).as("w"));

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(rowInterval(10)).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
Tumbling windows are defined by using the `Tumble` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```scala
// Tumbling Event-time Window
.window(Tumble over 10.minutes on $"rowtime" as $"w")

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.minutes on $"proctime" as $"w")

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.rows on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
Tumbling windows are defined by using the `Tumble` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>group_by()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```python
# Tumbling Event-time Window
.window(Tumble.over(lit(10).minutes).on(col('rowtime')).alias("w"))

# Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(lit(10).minutes).on(col('proctime')).alias("w"))

# Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble.over(row_interval(10)).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Slide (Sliding Windows)

A sliding window has a fixed size and slides by a specified slide interval. If the slide interval is smaller than the window size, sliding windows are overlapping. Thus, rows can be assigned to multiple windows. For example, a sliding window of 15 minutes size and 5 minute slide interval assigns each row to 3 different windows of 15 minute size, which are evaluated in an interval of 5 minutes. Sliding windows can be defined on event-time, processing-time, or on a row-count.

{{< tabs "8a408c78-2a76-4193-a457-e95af384edb5" >}}
{{< tab "Java" >}}
Sliding windows are defined by using the `Slide` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length of the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```java
// Sliding Event-time Window
.window(Slide.over(lit(10).minutes())
            .every(lit(5).minutes())
            .on($("rowtime"))
            .as("w"));

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over(lit(10).minutes())
            .every(lit(5).minutes())
            .on($("proctime"))
            .as("w"));

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over(rowInterval(10)).every(rowInterval(5)).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
Sliding windows are defined by using the `Slide` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length of the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```scala
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on $"rowtime" as $"w")

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide over 10.minutes every 5.minutes on $"proctime" as $"w")

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide over 10.rows every 5.rows on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
Sliding windows are defined by using the `Slide` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Defines the length of the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>group_by()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```python
# Sliding Event-time Window
.window(Slide.over(lit(10).minutes).every(lit(5).minutes).on(col('rowtime')).alias("w"))

# Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide.over(lit(10).minutes).every(lit(5).minutes).on(col('proctime')).alias("w"))

# Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide.over(row_interval(10)).every(row_interval(5)).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Session (Session Windows)

Session windows do not have a fixed size but their bounds are defined by an interval of inactivity, i.e., a session window is closes if no event appears for a defined gap period. For example a session window with a 30 minute gap starts when a row is observed after 30 minutes inactivity (otherwise the row would be added to an existing window) and is closed if no row is added within 30 minutes. Session windows can work on event-time or processing-time.

{{< tabs "58943253-807b-4e4c-b068-0dc1b783b7b5" >}}
{{< tab "Java" >}}
A session window is defined by using the `Session` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>Defines the gap between two windows as time interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```java
// Session Event-time Window
.window(Session.withGap(lit(10).minutes()).on($("rowtime")).as("w"));

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.withGap(lit(10).minutes()).on($("proctime")).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
A session window is defined by using the `Session` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>Defines the gap between two windows as time interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```scala
// Session Event-time Window
.window(Session withGap 10.minutes on $"rowtime" as $"w")

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session withGap 10.minutes on $"proctime" as $"w")
```
{{< /tab >}}
{{< tab "Python" >}}
A session window is defined by using the `Session` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>with_gap</code></td>
      <td>Defines the gap between two windows as time interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>The time attribute to group (time interval) or sort (row count) on. For batch queries this might be any Long or Timestamp attribute. For streaming queries this must be a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">declared event-time or processing-time time attribute</a>.</td>
    </tr>
    <tr>
      <td><code>alias</code></td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>group_by()</code> clause and optionally to select window properties such as window start, end, or rowtime timestamps in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

```python
# Session Event-time Window
.window(Session.with_gap(lit(10).minutes).on(col('rowtime')).alias("w"))

# Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session.with_gap(lit(10).minutes).on(col('proctime')).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Over Windows

Over window aggregates are known from standard SQL (`OVER` clause) and defined in the `SELECT` clause of a query. Unlike group windows, which are specified in the `GROUP BY` clause, over windows do not collapse rows. Instead over window aggregates compute an aggregate for each input row over a range of its neighboring rows.

Over windows are defined using the `window(w: OverWindow*)` clause (using `over_window(*OverWindow)` in Python API) and referenced via an alias in the `select()` method. The following example shows how to define an over window aggregation on a table.

{{< tabs "92e08076-6823-451b-b54f-8e58c1b54dc3" >}}
{{< tab "Java" >}}
```java
Table table = input
  .window([OverWindow w].as("w"))           // define over window with alias w
  .select($("a"), $("b").sum().over($("w")), $("c").min().over($("w"))); // aggregate over the over window w
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val table = input
  .window([w: OverWindow] as $"w")              // define over window with alias w
  .select($"a", $"b".sum over $"w", $"c".min over $"w") // aggregate over the over window w
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# define over window with alias w and aggregate over the over window w
table = input.over_window([w: OverWindow].alias("w")) \
    .select(input.a, input.b.sum.over(col('w')), input.c.min.over(col('w')))
```
{{< /tab >}}
{{< /tabs >}}

The `OverWindow` defines a range of rows over which aggregates are computed. `OverWindow` is not an interface that users can implement. Instead, the Table API provides the `Over` class to configure the properties of the over window. Over windows can be defined on event-time or processing-time and on ranges specified as time interval or row-count. The supported over window definitions are exposed as methods on `Over` (and other classes) and are listed below:

#### Partition By 

**Optional**

Defines a partitioning of the input on one or more attributes. Each partition is individually sorted and aggregate functions are applied to each partition separately.

Note: In streaming environments, over window aggregates can only be computed in parallel if the window includes a partition by clause. Without partitionBy(...) the stream is processed by a single, non-parallel task.

#### Order By 

**Required**

Defines the order of rows within each partition and thereby the order in which the aggregate functions are applied to rows.

Note: For streaming queries this must be a declared event-time or processing-time time attribute. Currently, only a single sort attribute is supported.

#### Preceding

**Optional**

Defines the interval of rows that are included in the window and precede the current row. The interval can either be specified as time or row-count interval.

Bounded over windows are specified with the size of the interval, e.g., 10.minutes for a time interval or 10.rows for a row-count interval.

Unbounded over windows are specified using a constant, i.e., UNBOUNDED_RANGE for a time interval or UNBOUNDED_ROW for a row-count interval. Unbounded over windows start with the first row of a partition.

If the preceding clause is omitted, UNBOUNDED_RANGE and CURRENT_RANGE are used as the default preceding and following for the window.

#### Following

**Optional**

Defines the window interval of rows that are included in the window and follow the current row. The interval must be specified in the same unit as the preceding interval (time or row-count).

At the moment, over windows with rows following the current row are not supported. Instead you can specify one of two constants:

* `CURRENT_ROW` sets the upper bound of the window to the current row.
* `CURRENT_RANGE` sets the upper bound of the window to sort key of the current row, i.e., all rows with the same sort key as the current row are included in the window.

If the following clause is omitted, the upper bound of a time interval window is defined as `CURRENT_RANGE` and the upper bound of a row-count interval window is defined as CURRENT_ROW.

#### As

**Required**

Assigns an alias to the over window. The alias is used to reference the over window in the following `select()` clause.

Note: Currently, all aggregation functions in the same select() call must be computed of the same over window.

#### Unbounded Over Windows

{{< tabs "ff010e04-fbb2-461b-823f-13c186c0df4a" >}}
{{< tab "Java" >}}
```java
// Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_RANGE).as("w"));

// Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy($("a")).orderBy("proctime").preceding(UNBOUNDED_RANGE).as("w"));

// Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(UNBOUNDED_ROW).as("w"));
 
// Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(UNBOUNDED_ROW).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_RANGE as "w")

// Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_RANGE as "w")

// Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_ROW as "w")
 
// Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_ROW as "w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(UNBOUNDED_RANGE).alias("w"))

# Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(UNBOUNDED_RANGE).alias("w"))

# Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(UNBOUNDED_ROW).alias("w"))
 
# Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(UNBOUNDED_ROW).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

#### Bounded Over Windows
{{< tabs "221fc3e4-a1b0-4d0a-a745-bd1e89e926f7" >}}
{{< tab "Java" >}}
```java
// Bounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(lit(1).minutes()).as("w"));

// Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(lit(1).minutes()).as("w"));

// Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over.partitionBy($("a")).orderBy($("rowtime")).preceding(rowInterval(10)).as("w"));
 
// Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over.partitionBy($("a")).orderBy($("proctime")).preceding(rowInterval(10)).as("w"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// Bounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 1.minutes as "w")

// Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding 1.minutes as "w")

// Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 10.rows as "w")
  
// Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding 10.rows as "w")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# Bounded Event-time over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(lit(1).minutes).alias("w"))

# Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(lit(1).minutes).alias("w"))

# Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.over_window(Over.partition_by(col('a')).order_by(col('rowtime')).preceding(row_interval(10)).alias("w"))
 
# Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.over_window(Over.partition_by(col('a')).order_by(col('proctime')).preceding(row_interval(10)).alias("w"))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Row-based Operations

The row-based operations generate outputs with multiple columns.

#### Map

{{< label Batch >}} {{< label Streaming >}}

{{< tabs "map" >}}
{{< tab "Java" >}}

Performs a map operation with a user-defined scalar function or built-in scalar function. The output will be flattened if the output type is a composite type.

```java
public class MyMapFunction extends ScalarFunction {
    public Row eval(String a) {
        return Row.of(a, "pre-" + a);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.ROW(Types.STRING, Types.STRING);
    }
}

ScalarFunction func = new MyMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .map(call("func", $("c"))).as("a", "b");
```
{{< /tab >}}
{{< tab "Scala" >}}

Performs a map operation with a user-defined scalar function or built-in scalar function. The output will be flattened if the output type is a composite type.

```scala
class MyMapFunction extends ScalarFunction {
  def eval(a: String): Row = {
    Row.of(a, "pre-" + a)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.STRING)
}

val func = new MyMapFunction()
val table = input
  .map(func($"c")).as("a", "b")
```
{{< /tab >}}
{{< tab "Python" >}}

Performs a map operation with a python [general scalar function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#scalar-functions) or [vectorized scalar function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-scalar-functions). The output will be flattened if the output type is a composite type.

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import udf

def map_function(a: Row) -> Row:
    return Row(a[0] + 1, a[1] * a[1])

# map operation with a python general scalar function
func = udf(map_function, result_type=DataTypes.ROW(
                                     [DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.BIGINT())]))
table = input.map(func).alias('a', 'b')

# map operation with a python vectorized scalar function
pandas_func = udf(lambda x: x * 2, result_type=DataTypes.ROW(
                                                    [DataTypes.FIELD("a", DataTypes.BIGINT()),
                                                    DataTypes.FIELD("b", DataTypes.BIGINT()))]),
                  func_type='pandas')

table = input.map(pandas_func).alias('a', 'b')
```

{{< /tab >}}
{{< /tabs >}}

#### FlatMap

{{< label Batch >}} {{< label Streaming >}}

{{< tabs "flatmap" >}}
{{< tab "Java" >}}

Performs a `flatMap` operation with a table function.

```java
public class MyFlatMapFunction extends TableFunction<Row> {

    public void eval(String str) {
        if (str.contains("#")) {
            String[] array = str.split("#");
            for (int i = 0; i < array.length; ++i) {
                collect(Row.of(array[i], array[i].length()));
            }
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.INT);
    }
}

TableFunction func = new MyFlatMapFunction();
tableEnv.registerFunction("func", func);

Table table = input
  .flatMap(call("func", $("c"))).as("a", "b");
```
{{< /tab >}}
{{< tab "Scala" >}}

Performs a `flatMap` operation with a python table function.

```scala
class MyFlatMapFunction extends TableFunction[Row] {
  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}

val func = new MyFlatMapFunction
val table = input
  .flatMap(func($"c")).as("a", "b")
```
{{< /tab >}}
{{< tab "Python" >}}

Performs a `flat_map` operation with a python [table function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-functions).

```python
from pyflink.table.udf import udtf
from pyflink.table import DataTypes
from pyflink.common import Row

@udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
def split(x: Row) -> Row:
    for s in x[1].split(","):
        yield x[0], s

input.flat_map(split)
```
{{< /tab >}}
{{< /tabs >}}

#### Aggregate

{{< label Batch >}} {{< label Streaming >}} {{< label Result Updating >}}

{{< tabs "aggregate" >}}
{{< tab "Java" >}}

Performs an aggregate operation with an aggregate function. You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions. The output of aggregate will be flattened if the output type is a composite type.

```java
public class MyMinMaxAcc {
    public int min = 0;
    public int max = 0;
}

public class MyMinMax extends AggregateFunction<Row, MyMinMaxAcc> {

    public void accumulate(MyMinMaxAcc acc, int value) {
        if (value < acc.min) {
            acc.min = value;
        }
        if (value > acc.max) {
            acc.max = value;
        }
    }

    @Override
    public MyMinMaxAcc createAccumulator() {
        return new MyMinMaxAcc();
    }

    public void resetAccumulator(MyMinMaxAcc acc) {
        acc.min = 0;
        acc.max = 0;
    }

    @Override
    public Row getValue(MyMinMaxAcc acc) {
        return Row.of(acc.min, acc.max);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.INT, Types.INT);
    }
}

AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);
Table table = input
  .groupBy($("key"))
  .aggregate(call("myAggFunc", $("a")).as("x", "y"))
  .select($("key"), $("x"), $("y"));
```
{{< /tab >}}
{{< tab "Scala" >}}

Performs an aggregate operation with an aggregate function. You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions. The output of aggregate will be flattened if the output type is a composite type.

```scala
case class MyMinMaxAcc(var min: Int, var max: Int)

class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

  def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
    if (value < acc.min) {
      acc.min = value
    }
    if (value > acc.max) {
      acc.max = value
    }
  }

  override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

  def resetAccumulator(acc: MyMinMaxAcc): Unit = {
    acc.min = 0
    acc.max = 0
  }

  override def getValue(acc: MyMinMaxAcc): Row = {
    Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.INT, Types.INT)
  }
}

val myAggFunc = new MyMinMax
val table = input
  .groupBy($"key")
  .aggregate(myAggFunc($"a") as ("x", "y"))
  .select($"key", $"x", $"y")
```
{{< /tab >}}
{{< tab "Python" >}}

Performs an aggregate operation with a python [general aggregate function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#aggregate-functions) or [vectorized aggregate function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-aggregate-functions). You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions. The output of aggregate will be flattened if the output type is a composite type.

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf

class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, *args):
        accumulator[0] += 1
        accumulator[1] += args[0][1]

    def retract(self, accumulator, *args):
        accumulator[0] -= 1
        accumulator[1] -= args[0][1]

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

function = CountAndSumAggregateFunction()
agg = udaf(function,
           result_type=function.get_result_type(),
           accumulator_type=function.get_accumulator_type(),
           name=str(function.__class__.__name__))

# aggregate with a python general aggregate function
result = t.group_by(t.a) \
    .aggregate(agg.alias("c", "d")) \
    .select("a, c, d")
    
# aggregate with a python vectorized aggregate function
pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
t.aggregate(pandas_udaf.alias("a", "b")) \
    .select("a, b")

```

{{< /tab >}}
{{< /tabs >}}

####  Group Window Aggregate

{{< label Batch >}} {{< label Streaming >}}

Groups and aggregates a table on a [group window](#group-window) and possibly one or more grouping keys. You have to close the "aggregate" with a select statement. And the select statement does not support "*" or aggregate functions.

{{< tabs "group-window-agg" >}}
{{< tab "Java" >}}
```java
AggregateFunction myAggFunc = new MyMinMax();
tableEnv.registerFunction("myAggFunc", myAggFunc);

Table table = input
    .window(Tumble.over(lit(5).minutes())
                  .on($("rowtime"))
                  .as("w")) // define window
    .groupBy($("key"), $("w")) // group by key and window
    .aggregate(call("myAggFunc", $("a")).as("x", "y"))
    .select($("key"), $("x"), $("y"), $("w").start(), $("w").end()); // access window properties and aggregate results
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val myAggFunc = new MyMinMax
val table = input
    .window(Tumble over 5.minutes on $"rowtime" as "w") // define window
    .groupBy($"key", $"w") // group by key and window
    .aggregate(myAggFunc($"a") as ("x", "y"))
    .select($"key", $"x", $"y", $"w".start, $"w".end) // access window properties and aggregate results
```
{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf

pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
tumble_window = Tumble.over(expr.lit(1).hours) \
    .on(expr.col("rowtime")) \
    .alias("w")
t.select(t.b, t.rowtime) \
    .window(tumble_window) \
    .group_by("w") \
    .aggregate(pandas_udaf.alias("d", "e")) \
    .select("w.rowtime, d, e")
```
{{< /tab >}}
{{< /tabs >}}

#### FlatAggregate

{{< tabs "flataggregate" >}}
{{< tab "Java" >}}

Similar to a **GroupBy Aggregation**. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.

Instead of using emitValue to output results, you can also use the emitUpdateWithRetract method. Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated. This method outputs data incrementally in retract mode, i.e., once there is an update, we have to retract old records before sending new updated ones. The emitUpdateWithRetract method will be used in preference to the emitValue method if both methods are defined in the table aggregate function, because the method is treated to be more efficient than emitValue as it can output values incrementally. 

```java
/**
 * Accumulator for Top2.
 */
public class Top2Accum {
    public Integer first;
    public Integer second;
}

/**
 * The top2 user-defined table aggregate function.
 */
public class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }


    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
        for (Top2Accum otherAcc : iterable) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }

    public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
        // emit the value and rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}

tEnv.registerFunction("top2", new Top2());
Table orders = tableEnv.from("Orders");
Table result = orders
    .groupBy($("key"))
    .flatAggregate(call("top2", $("a")).as("v", "rank"))
    .select($("key"), $("v"), $("rank");
```
{{< /tab >}}
{{< tab "Scala" >}}

Similar to a **GroupBy Aggregation**. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flatAggregate" with a select statement. And the select statement does not support aggregate functions.

Instead of using emitValue to output results, you can also use the emitUpdateWithRetract method. Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated. This method outputs data incrementally in retract mode, i.e., once there is an update, we have to retract old records before sending new updated ones. The emitUpdateWithRetract method will be used in preference to the emitValue method if both methods are defined in the table aggregate function, because the method is treated to be more efficient than emitValue as it can output values incrementally. 

```scala
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}

val top2 = new Top2
val orders: Table = tableEnv.from("Orders")
val result = orders
    .groupBy($"key")
    .flatAggregate(top2($"a") as ($"v", $"rank"))
    .select($"key", $"v", $"rank")
```
{{< /tab >}}
{{< tab "Python" >}}

Performs a flat_aggregate operation with a python general [Table Aggregate Function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-aggregate-functions)

Similar to a **GroupBy Aggregation**. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flat_aggregate" with a select statement. And the select statement does not support aggregate functions.

```python
from pyflink.common import Row
from pyflink.table.udf import TableAggregateFunction, udtaf
from pyflink.table import DataTypes

class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, *args):
        if args[0][0] is not None:
            if accumulator[0] is None or args[0][0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = args[0][0]
            elif accumulator[1] is None or args[0][0] > accumulator[1]:
                accumulator[1] = args[0][0]

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            self.accumulate(accumulator, other_acc[0])
            self.accumulate(accumulator, other_acc[1])

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])

mytop = udtaf(Top2())
t = t_env.from_elements([(1, 'Hi', 'Hello'),
                              (3, 'Hi', 'hi'),
                              (5, 'Hi2', 'hi'),
                              (7, 'Hi', 'Hello'),
                              (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
result = t.select(t.a, t.c) \
    .group_by(t.c) \
    .flat_aggregate(mytop) \
    .select(t.a) \
    .flat_aggregate(mytop.alias("b"))

```
{{< /tab >}}
{{< /tabs >}}

{{< query_state_warning >}}

Data Types
----------

Please see the dedicated page about [data types]({{< ref "docs/dev/table/types" >}}).

Generic types and (nested) composite types (e.g., POJOs, tuples, rows, Scala case classes) can be fields of a row as well.

Fields of composite types with arbitrary nesting can be accessed with [value access functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}#value-access-functions).

Generic types are treated as a black box and can be passed on or processed by [user-defined functions]({{< ref "docs/dev/table/functions/udfs" >}}).

{{< top >}}

