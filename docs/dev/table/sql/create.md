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
  [ LIKE source_table [( <like_options> )] ]
   
<column_definition>:
  column_name column_type [COMMENT column_comment]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression
  
<like_options>:
{
   { INCLUDING | EXCLUDING } { ALL | CONSTRAINTS | PARTITIONS }
 | { INCLUDING | EXCLUDING | OVERWRITING } { GENERATED | OPTIONS | WATERMARKS } 
}[, ...]

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

**LIKE clause**

The `LIKE` clause is a variant/combination of SQL features (Feature T171, “LIKE clause in table definition” and Feature T173, “Extended LIKE clause in table definition”). The clause can be used to create a table based on a definition of an existing table. Additionally, users
can extend the original table or exclude certain parts of it. In contrast to the SQL standard the clause must be defined at the top-level of a CREATE statement. That is because the clause applies to multiple parts of the definition and not only to the schema part.

You can use the clause to reuse (and potentially overwrite) certain connector properties or add watermarks to tables defined externally. For example, you can add a watermark to a table defined in Apache Hive. 

Consider the example statement below:
{% highlight sql %}
CREATE TABLE Orders (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3)
) WITH ( 
    'connector' = 'kafka',
    'startup-mode' = 'earliest-offset'
);

CREATE TABLE Orders_with_watermark (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    -- Overwrite the startup-mode
    'startup-mode' = 'latest-offset'
)
LIKE Orders;
{% endhighlight %}

The resulting table `Orders_with_watermark` will be equivalent to a table created with a following statement:
{% highlight sql %}
CREATE TABLE Orders_with_watermark (
    user BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    'startup-mode' = 'latest-offset'
);
{% endhighlight %}

The merging logic of table features can be controlled with `like options`.

You can control the merging behavior of:

* CONSTRAINTS - constraints such as primary and unique keys
* GENERATED - computed columns
* OPTIONS - connector options that describe connector and format properties
* PARTITIONS - partition of the tables
* WATERMARKS - watermark declarations

with three different merging strategies:

* INCLUDING - Includes the feature of the source table, fails on duplicate entries, e.g. if an option with the same key exists in both tables.
* EXCLUDING - Does not include the given feature of the source table.
* OVERWRITING - Includes the feature of the source table, overwrites duplicate entries of the source table with properties of the new table, e.g. if an option with the same key exists in both tables, the one from the current statement will be used.

Additionally, you can use the `INCLUDING/EXCLUDING ALL` option to specify what should be the strategy if there was no specific strategy defined, i.e. if you use `EXCLUDING ALL INCLUDING WATERMARKS` only the watermarks will be included from the source table.

Example:
{% highlight sql %}
-- A source table stored in a filesystem
CREATE TABLE Orders_in_file (
    user BIGINT,
    product STRING,
    order_time_string STRING,
    order_time AS to_timestamp(order_time)
    
)
PARTITIONED BY user 
WITH ( 
    'connector' = 'filesystem'
    'path' = '...'
);

-- A corresponding table we want to store in kafka
CREATE TABLE Orders_in_kafka (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector': 'kafka'
    ...
)
LIKE Orders_in_file (
    -- Exclude everything besides the computed columns which we need to generate the watermark for.
    -- We do not want to have the partitions or filesystem options as those do not apply to kafka. 
    EXCLUDING ALL
    INCLUDING GENERATED
);
{% endhighlight %}

If you provide no like options, `INCLUDING ALL OVERWRITING OPTIONS` will be used as a default.

**NOTE** You cannot control the behavior of merging physical fields. Those will be merged as if you applied the `INCLUDING` strategy.

{% top %}

## CREATE CATALOG

{% highlight sql %}
CREATE CATALOG catalog_name
  WITH (key1=val1, key2=val2, ...)
{% endhighlight %}

Create a catalog with the given catalog properties. If a catalog with the same name already exists, an exception is thrown.

**WITH OPTIONS**

Catalog properties used to store extra information related to this catalog.
The key and value of expression `key1=val1` should both be string literal.

Check out more details at [Catalogs]({{ site.baseurl }}/dev/table/catalogs.html).

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
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
{% endhighlight %}

Create a catalog function that has catalog and database namespaces with the identifier and optional language tag. If a function with the same name already exists in the catalog, an exception is thrown.

If the language tag is JAVA/SCALA, the identifier is the full classpath of the UDF. For the implementation of Java/Scala UDF, please refer to [User-defined Functions]({{ site.baseurl }}/dev/table/functions/udfs.html) for more details.

If the language tag is PYTHON, the identifier is the fully qualified name of the UDF, e.g. `pyflink.table.tests.test_udf.add`. For the implementation of Python UDF, please refer to [Python UDFs]({{ site.baseurl }}/dev/table/python/python_udfs.html) for more details.

**TEMPORARY**

Create temporary catalog function that has catalog and database namespaces and overrides catalog functions.

**TEMPORARY SYSTEM**

Create temporary system function that has no namespace and overrides built-in functions

**IF NOT EXISTS**

If the function already exists, nothing happens.

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag to instruct Flink runtime how to execute the function. Currently only JAVA, SCALA and PYTHON are supported, the default language for a function is JAVA. 
