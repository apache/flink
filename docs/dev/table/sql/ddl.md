---
title: "Data Definition Language (DDL)"
nav-title: "Data Definition Language"
nav-parent_id: sql
nav-pos: 1
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

DDLs are specified with the `sqlUpdate()` method of the `TableEnvironment`. The method returns nothing for a success create/drop/alter database or table operation. A catalog table will be registered into the [Catalog]({{ site.baseurl }}/dev/table/catalogs.html) with a `CREATE TABLE` statement, then can be referenced in SQL queries.

Flink SQL DDL statements are documented here, including:

- CREATE TABLE, VIEW, DATABASE, FUNCTION
- DROP TABLE, VIEW, DATABASE, FUNCTION
- ALTER TABLE, DATABASE

## Run a DDL

The following examples show how to run a SQL DDL in `TableEnvironment`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// SQL query with a registered table
// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");
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
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ('connector.path'='/path/to/file' ...)");
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# SQL update with a registered table
# register a TableSink
table_env.sql_update("CREATE TABLE RubberOrders(product VARCHAR, amount INT) with (...)")
# run a SQL update query on the Table and emit the result to the TableSink
table_env \
    .sql_update("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

## Table DDL

### CREATE TABLE

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

Column declared with syntax "`column_name AS computed_column_expression`" is a computed column. A computed column is a virtual column that is not physically stored in the table. The column is computed from an non-query expression that uses other columns in the same table. For example, a computed column can have the definition: `cost AS price * qty`. The expression can be a noncomputed column name, constant, (user-defined/system) function, variable, and any combination of these connected by one or more operators. The expression cannot be a subquery.

Computed column is introduced to Flink for defining [time attributes]({{ site.baseurl}}/dev/table/streaming/time_attributes.html) in CREATE TABLE DDL.
A [processing time attribute]({{ site.baseurl}}/dev/table/streaming/time_attributes.html#processing-time) can be defined easily via `proc AS PROCTIME()` using the system `PROCTIME()` function.
On the other hand, computed column can be used to derive event time column because an event time column may need to be derived from existing fields, e.g. the original field is not `TIMESTAMP(3)` type or is nested in a JSON string.

Notes:

- A computed column defined on a source table is computed after reading from the source, it can be used in the following SELECT query statements.
- A computed column cannot be the target of an INSERT statement, INSERT statement should match SELECT clause's schema with sink table's schema without computed column.


**WATERMARK**

The WATERMARK definition is used to define [event time attribute]({{ site.baseurl }}/dev/table/streaming/time_attributes.html#event-time) in CREATE TABLE DDL.

The “`FOR rowtime_column_name`” statement defines which existing column is marked as event time attribute, the column must be `TIMESTAMP(3)` type and top-level in the schema and can be a computed column.

The “`AS watermark_strategy_expression`” statement defines watermark generation strategy. It allows arbitrary non-query expression (can reference computed columns) to calculate watermark. The expression return type must be `TIMESTAMP(3)` which represents the timestamp since Epoch.

The returned watermark will be emitted only if it is non-null and its value is larger than the previously emitted local watermark (to preserve the contract of ascending watermarks). The watermark generation expression is called by the framework for every record.
The framework will periodically emit the largest generated watermark. If the current watermark is still identical to the previous one, or is null, or the value of the returned watermark is smaller than that of the last emitted one, then no new watermark will be emitted.
Watermark is emitted in an interval defined by [`pipeline.auto-watermark-interval`]({{ site.baseurl }}/ops/config.html#pipeline-auto-watermark-interval) configuration.
If watermark interval is `0ms`, the generated watermarks will be emitted per-record if it is not null and greater than the last emitted one.

For common cases, Flink provide some suggested and easy-to-use ways to define commonly used watermark strategies. Such as:

- Strictly ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column`

  Emits a watermark of the maximum observed timestamp so far. Rows that have a timestamp smaller to the max timestamp are not late.

- Ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND`

  Emits a watermark of the maximum observed timestamp so far minus 1 (the smallest unit of watermark is millisecond). Rows that have a timestamp equal to the max timestamp are not late.

- Bounded out of orderness timestamps: `WATERMARK FOR rowtime_column AS rowtimeField - INTERVAL 'string' timeUnit`

  Emits watermarks which are the maximum observed timestamp minus the specified delay, e.g. `WATERMARK FOR rowtime_column AS rowtimeField - INTERVAL '5' SECOND` is a 5 seconds delayed watermark strategy.

- Preserves assigned watermarks from source (**Not supported yet**): `WATERMARK FOR rowtime_column AS SYSTEM_WATERMARK()`

  Indicates the watermarks are preserved from the source (e.g. some sources can generate watermarks themselves).

**PARTITIONED BY**

Partition the created table by the specified columns. A directory is created for each partition if this table is used as a filesystem sink.

**WITH OPTIONS**

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector.

The key and value of expression `key1=val1` should both be string literal. See details in [Connect to External Systems](connect.html) for all the supported table properties of different connectors.

**Notes:** The table name can be of three formats: 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`. For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name"; for `db_name.table_name`, the table would be registered into the current catalog of the execution table environment and database named "db_name"; for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

**Notes:** The table registered with `CREATE TABLE` statement can be used as both table source and table sink, we can not decide if it is used as a source or sink until it is referenced in the DMLs.

{% top %}

### DROP TABLE

{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

{% top %}

### ALTER TABLE

* Rename Table

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
{% endhighlight %}

Rename the given table name to another new table name.

* Set or Alter Table Properties

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

Set one or more properties in the specified table. If a particular property is already set in the table, override the old value with the new one.

{% top %}

## View DDL

### CREATE VIEW

*TODO: should add descriptions.*

### DROP VIEW

*TODO: should add descriptions.*

## Database DDL

### CREATE DATABASE

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

### DROP DATABASE

{% highlight sql %}
DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
{% endhighlight %}

Drop a database with the given database name. If the database to drop does not exist, an exception is thrown.

**IF EXISTS**

If the database does not exist, nothing happens.

**RESTRICT**

Dropping a non-empty database triggers an exception. Enabled by default.

**CASCADE**

Dropping a non-empty database also drops all associated tables and functions.

{% top %}

### ALTER DATABASE

{% highlight sql %}
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

Set one or more properties in the specified database. If a particular property is already set in the database, override the old value with the new one.

## Function DDL

### CREATE FUNCTION

*TODO: should add descriptions.*

### DROP FUNCTION

*TODO: should add descriptions.*

{% top %}

