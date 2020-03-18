---
title: "CREATE Statements"
nav-parent_id: sql
nav-pos: 2
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

* This will be replaced by the TOC
{:toc}

CREATE statements are used to register a table/view/function into current or specified [Catalog]({{ site.baseurl }}/dev/table/catalogs.html). A registered table/view/function can be used in SQL queries.

Flink SQL supports the following CREATE statements for now:

- CREATE TABLE
- CREATE DATABASE
- CREATE FUNCTION

## Run a CREATE statement

CREATE statements can be executed with the `sqlUpdate()` method of the `TableEnvironment`, or executed in [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html). The `sqlUpdate()` method returns nothing for a successful CREATE operation, otherwise will throw an exception.

The following examples show how to run a CREATE statement in `TableEnvironment` and in SQL CLI.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// SQL query with a registered table
// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)");
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// SQL query with a registered table
// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product STRING, amount INT) WITH ('connector.path'='/path/to/file' ...)");
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# SQL query with a registered table
# register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
# run a SQL query on the Table and retrieve the result as a new Table
result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

# SQL update with a registered table
# register a TableSink
table_env.sql_update("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")
# run a SQL update query on the Table and emit the result to the TableSink
table_env \
    .sql_update("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders (product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
{% endhighlight %}
</div>
</div>

{% top %}

## CREATE TABLE

{% highlight sql %}
CREATE TABLE [catalog_name.][db_name.]table_name
  (
    { <column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)

<column_definition>:
  column_name column_type [COMMENT column_comment]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

{% endhighlight %}

Creates a table with the given name. If a table with the same name already exists in the catalog, an exception is thrown.

**COMPUTED COLUMN**

A computed column is a virtual column that is generated using the syntax  "`column_name AS computed_column_expression`". It is generated from a non-query expression that uses other columns in the same table and is not physically stored within the table. For example, a computed column could be defined as `cost AS price * quantity`. The expression may contain any combination of physical column, constant, function, or variable. The expression cannot contain a subquery.

Computed columns are commonly used in Flink for defining [time attributes]({{ site.baseurl}}/dev/table/streaming/time_attributes.html) in CREATE TABLE statements.
A [processing time attribute]({{ site.baseurl}}/dev/table/streaming/time_attributes.html#processing-time) can be defined easily via `proc AS PROCTIME()` using the system `PROCTIME()` function.
On the other hand, computed column can be used to derive event time column because an event time column may need to be derived from existing fields, e.g. the original field is not `TIMESTAMP(3)` type or is nested in a JSON string.

Notes:

- A computed column defined on a source table is computed after reading from the source, it can be used in the following SELECT query statements.
- A computed column cannot be the target of an INSERT statement. In INSERT statements, the schema of SELECT clause should match the schema of the target table without computed columns.

**WATERMARK**

The `WATERMARK` defines the event time attributes of a table and takes the form `WATERMARK FOR rowtime_column_name  AS watermark_strategy_expression`.

The  `rowtime_column_name` defines an existing column that is marked as the event time attribute of the table. The column must be of type `TIMESTAMP(3)` and be a top-level column in the schema. It may be a computed column.

The `watermark_strategy_expression` defines the watermark generation strategy. It allows arbitrary non-query expression, including computed columns, to calculate the watermark. The expression return type must be TIMESTAMP(3), which represents the timestamp since the Epoch.
The returned watermark will be emitted only if it is non-null and its value is larger than the previously emitted local watermark (to preserve the contract of ascending watermarks). The watermark generation expression is evaluated by the framework for every record.
The framework will periodically emit the largest generated watermark. If the current watermark is still identical to the previous one, or is null, or the value of the returned watermark is smaller than that of the last emitted one, then no new watermark will be emitted.
Watermark is emitted in an interval defined by [`pipeline.auto-watermark-interval`]({{ site.baseurl }}/ops/config.html#pipeline-auto-watermark-interval) configuration.
If watermark interval is `0ms`, the generated watermarks will be emitted per-record if it is not null and greater than the last emitted one.

When using event time semantics, tables must contain an event time attribute and watermarking strategy.

Flink provides several commonly used watermark strategies.

- Strictly ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column`.

  Emits a watermark of the maximum observed timestamp so far. Rows that have a timestamp smaller to the max timestamp are not late.

- Ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND`.

  Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a timestamp equal and smaller to the max timestamp are not late.

- Bounded out of orderness timestamps: `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit`.

  Emits watermarks, which are the maximum observed timestamp minus the specified delay, e.g., `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND` is a 5 seconds delayed watermark strategy.

{% highlight sql %}
CREATE TABLE Orders (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ( . . . );
{% endhighlight %}

**PARTITIONED BY**

Partition the created table by the specified columns. A directory is created for each partition if this table is used as a filesystem sink.

**WITH OPTIONS**

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector.

The key and value of expression `key1=val1` should both be string literal. See details in [Connect to External Systems]({{ site.baseurl }}/dev/table/connect.html) for all the supported table properties of different connectors.

**Notes:** The table name can be of three formats: 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`. For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name"; for `db_name.table_name`, the table would be registered into the current catalog of the execution table environment and database named "db_name"; for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

**Notes:** The table registered with `CREATE TABLE` statement can be used as both table source and table sink, we can not decide if it is used as a source or sink until it is referenced in the DMLs.

{% top %}

## CREATE DATABASE

{% highlight sql %}
CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...)
{% endhighlight %}

Create a database with the given database properties. If a database with the same name already exists in the catalog, an exception is thrown.

**IF NOT EXISTS**

If the database already exists, nothing happens.

**WITH OPTIONS**

Database properties used to store extra information related to this database.
The key and value of expression `key1=val1` should both be string literal.

{% top %}

## CREATE FUNCTION
{% highlight sql%}
CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION 
  [IF NOT EXISTS] [catalog_name.][db_name.]function_name 
  AS identifier [LANGUAGE JAVA|SCALA]
{% endhighlight %}

Create a catalog function that has catalog and database namespaces with the identifier which is full classpath for JAVA/SCALA and optional language tag. If a function with the same name already exists in the catalog, an exception is thrown.

**TEMPORARY**

Create temporary catalog function that has catalog and database namespaces and overrides catalog functions.

**TEMPORARY SYSTEM**

Create temporary system function that has no namespace and overrides built-in functions

**IF NOT EXISTS**

If the function already exists, nothing happens.

**LANGUAGE JAVA\|SCALA**

Language tag to instruct Flink runtime how to execute the function. Currently only JAVA and SCALA are supported, the default language for a function is JAVA.
