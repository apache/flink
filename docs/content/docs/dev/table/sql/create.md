---
title: "CREATE Statements"
weight: 4
type: docs
aliases:
  - /dev/table/sql/create.html
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

# CREATE Statements

CREATE statements are used to register a table/view/function into current or specified [Catalog]({{< ref "docs/dev/table/catalogs" >}}). A registered table/view/function can be used in SQL queries.

Flink SQL supports the following CREATE statements for now:

- CREATE TABLE
- CREATE DATABASE
- CREATE VIEW
- CREATE FUNCTION

## Run a CREATE statement

{{< tabs "create" >}}
{{< tab "Java" >}}

CREATE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful CREATE operation, otherwise will throw an exception.

The following examples show how to run a CREATE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}
CREATE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful CREATE operation, otherwise will throw an exception.

The following examples show how to run a CREATE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

CREATE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns 'OK' for a successful CREATE operation, otherwise will throw an exception.

The following examples show how to run a CREATE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

CREATE statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a CREATE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "0b1b298a-b92f-4f95-8d06-49544b487b75" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// SQL query with a registered table
// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// Execute insert SQL with a registered table
// register a TableSink
tableEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)");
// run an insert SQL on the Table and emit the result to the TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// SQL query with a registered table
// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// Execute insert SQL with a registered table
// register a TableSink
tableEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH ('connector.path'='/path/to/file' ...)");
// run an insert SQL on the Table and emit the result to the TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
settings = EnvironmentSettings.new_instance()...
table_env = TableEnvironment.create(settings)

# SQL query with a registered table
# register a table named "Orders"
table_env.execute_sql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
# run a SQL query on the Table and retrieve the result as a new Table
result = table_env.sql_query(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

# Execute an INSERT SQL with a registered table
# register a TableSink
table_env.execute_sql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")
# run an INSERT SQL on the Table and emit the result to the TableSink
table_env \
    .execute_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders (product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## CREATE TABLE

The following grammar gives an overview about the available syntax:

```text
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
  [ LIKE source_table [( <like_options> )] ]
   
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

<source_table>:
  [catalog_name.][db_name.]table_name

<like_options>:
{
   { INCLUDING | EXCLUDING } { ALL | CONSTRAINTS | PARTITIONS }
 | { INCLUDING | EXCLUDING | OVERWRITING } { GENERATED | OPTIONS | WATERMARKS } 
}[, ...]

```

The statement above creates a table with the given name. If a table with the same name already exists
in the catalog, an exception is thrown.

### Columns

**Physical / Regular Columns**

Physical columns are regular columns known from databases. They define the names, the types, and the
order of fields in the physical data. Thus, physical columns represent the payload that is read from
and written to an external system. Connectors and formats use these columns (in the defined order)
to configure themselves. Other kinds of columns can be declared between physical columns but will not
influence the final physical schema.

The following statement creates a table with only regular columns:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING
) WITH (
  ...
);
```

**Metadata Columns**

Metadata columns are an extension to the SQL standard and allow to access connector and/or format specific
fields for every row of a table. A metadata column is indicated by the `METADATA` keyword. For example,
a metadata column can be be used to read and write the timestamp from and to Kafka records for time-based
operations. The [connector and format documentation]({{< ref "docs/connectors/table/overview" >}}) lists the
available metadata fields for every component. However, declaring a metadata column in a table's schema
is optional.

The following statement creates a table with an additional metadata column that references the metadata field `timestamp`:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `record_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'    -- reads and writes a Kafka record's timestamp
) WITH (
  'connector' = 'kafka'
  ...
);
```

Every metadata field is identified by a string-based key and has a documented data type. For example,
the Kafka connector exposes a metadata field with key `timestamp` and data type `TIMESTAMP_LTZ(3)`
that can be used for both reading and writing records.

In the example above, the metadata column `record_time` becomes part of the table's schema and can be
transformed and stored like a regular column:

```sql
INSERT INTO MyTable SELECT user_id, name, record_time + INTERVAL '1' SECOND FROM MyTable;
```

For convenience, the `FROM` clause can be omitted if the column name should be used as the identifying metadata key:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `timestamp` TIMESTAMP_LTZ(3) METADATA    -- use column name as metadata key
) WITH (
  'connector' = 'kafka'
  ...
);
```

For convenience, the runtime will perform an explicit cast if the data type of the column differs from
the data type of the metadata field. Of course, this requires that the two data types are compatible.

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `timestamp` BIGINT METADATA    -- cast the timestamp as BIGINT
) WITH (
  'connector' = 'kafka'
  ...
);
```

By default, the planner assumes that a metadata column can be used for both reading and writing. However,
in many cases an external system provides more read-only metadata fields than writable fields. Therefore,
it is possible to exclude metadata columns from persisting using the `VIRTUAL` keyword.

```sql
CREATE TABLE MyTable (
  `timestamp` BIGINT METADATA,       -- part of the query-to-sink schema
  `offset` BIGINT METADATA VIRTUAL,  -- not part of the query-to-sink schema
  `user_id` BIGINT,
  `name` STRING,
) WITH (
  'connector' = 'kafka'
  ...
);
```

In the example above, the `offset` is a read-only metadata column and excluded from the query-to-sink
schema. Thus, source-to-query schema (for `SELECT`) and query-to-sink (for `INSERT INTO`) schema differ:

```text
source-to-query schema:
MyTable(`timestamp` BIGINT, `offset` BIGINT, `user_id` BIGINT, `name` STRING)

query-to-sink schema:
MyTable(`timestamp` BIGINT, `user_id` BIGINT, `name` STRING)
```

**Computed Columns**

Computed columns are virtual columns that are generated using the syntax `column_name AS computed_column_expression`.

A computed column evaluates an expression that can reference other columns declared in the same table.
Both physical columns and metadata columns can be accessed. The column itself is not physically stored
within the table. The column's data type is derived automatically from the given expression and does
not have to be declared manually.

The planner will transform computed columns into a regular projection after the source. For optimization
or [watermark strategy push down]({{< ref "docs/dev/table/sourcesSinks" >}}), the evaluation might be spread
across operators, performed multiple times, or skipped if not needed for the given query.

For example, a computed column could be defined as:
```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `price` DOUBLE,
  `quantity` DOUBLE,
  `cost` AS price * quanitity,  -- evaluate expression and supply the result to queries
) WITH (
  'connector' = 'kafka'
  ...
);
```

The expression may contain any combination of columns, constants, or functions. The expression cannot
contain a subquery.

Computed columns are commonly used in Flink for defining [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}})
in `CREATE TABLE` statements.
- A [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time)
can be defined easily via `proc AS PROCTIME()` using the system's `PROCTIME()` function.
- An [event time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time) timestamp
can be pre-processed before the `WATERMARK` declaration. For example, the computed column can be used
if the original field is not `TIMESTAMP(3)` type or is nested in a JSON string.

Similar to virtual metadata columns, computed columns are excluded from persisting. Therefore, a computed
column cannot be the target of an `INSERT INTO` statement. Thus, source-to-query schema (for `SELECT`)
and query-to-sink (for `INSERT INTO`) schema differ:

```text
source-to-query schema:
MyTable(`user_id` BIGINT, `price` DOUBLE, `quantity` DOUBLE, `cost` DOUBLE)

query-to-sink schema:
MyTable(`user_id` BIGINT, `price` DOUBLE, `quantity` DOUBLE)
```

### `WATERMARK`

The `WATERMARK` clause defines the event time attributes of a table and takes the form `WATERMARK FOR rowtime_column_name AS watermark_strategy_expression`.

The  `rowtime_column_name` defines an existing column that is marked as the event time attribute of the table. The column must be of type `TIMESTAMP(3)` and be a top-level column in the schema. It may be a computed column.

The `watermark_strategy_expression` defines the watermark generation strategy. It allows arbitrary non-query expression, including computed columns, to calculate the watermark. The expression return type must be TIMESTAMP(3), which represents the timestamp since the Epoch.
The returned watermark will be emitted only if it is non-null and its value is larger than the previously emitted local watermark (to preserve the contract of ascending watermarks). The watermark generation expression is evaluated by the framework for every record.
The framework will periodically emit the largest generated watermark. If the current watermark is still identical to the previous one, or is null, or the value of the returned watermark is smaller than that of the last emitted one, then no new watermark will be emitted.
Watermark is emitted in an interval defined by [`pipeline.auto-watermark-interval`]({{< ref "docs/deployment/config" >}}#pipeline-auto-watermark-interval) configuration.
If watermark interval is `0ms`, the generated watermarks will be emitted per-record if it is not null and greater than the last emitted one.

When using event time semantics, tables must contain an event time attribute and watermarking strategy.

Flink provides several commonly used watermark strategies.

- Strictly ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column`.

  Emits a watermark of the maximum observed timestamp so far. Rows that have a timestamp bigger to the max timestamp are not late.

- Ascending timestamps: `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND`.

  Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a timestamp bigger or equal to the max timestamp are not late.

- Bounded out of orderness timestamps: `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit`.

  Emits watermarks, which are the maximum observed timestamp minus the specified delay, e.g., `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND` is a 5 seconds delayed watermark strategy.

```sql
CREATE TABLE Orders (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ( . . . );
```

### `PRIMARY KEY`

Primary key constraint is a hint for Flink to leverage for optimizations. It tells that a column or a set of columns of a table or a view are unique and they **do not** contain null.
Neither of columns in a primary can be nullable. Primary key therefore uniquely identify a row in a table.

Primary key constraint can be either declared along with a column definition (a column constraint) or as a single line (a table constraint).
For both cases, it should only be declared as a singleton. If you define multiple primary key constraints at the same time, an exception would be thrown.

**Validity Check**

SQL standard specifies that a constraint can either be `ENFORCED` or `NOT ENFORCED`. This controls if the constraint checks are performed on the incoming/outgoing data.
Flink does not own the data therefore the only mode we want to support is the `NOT ENFORCED` mode.
It is up to the user to ensure that the query enforces key integrity.

Flink will assume correctness of the primary key by assuming that the columns nullability is aligned with the columns in primary key. Connectors should ensure those are aligned.

**Notes:** In a CREATE TABLE statement, creating a primary key constraint will alter the columns nullability, that means, a column with primary key constraint is not nullable.

### `PARTITIONED BY`

Partition the created table by the specified columns. A directory is created for each partition if this table is used as a filesystem sink.

### `WITH` Options

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector.

The key and value of expression `key1=val1` should both be string literal. See details in [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}) for all the supported table properties of different connectors.

**Notes:** The table name can be of three formats: 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`. For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name"; for `db_name.table_name`, the table would be registered into the current catalog of the execution table environment and database named "db_name"; for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

**Notes:** The table registered with `CREATE TABLE` statement can be used as both table source and table sink, we can not decide if it is used as a source or sink until it is referenced in the DMLs.

### `LIKE`

The `LIKE` clause is a variant/combination of SQL features (Feature T171, “LIKE clause in table definition” and Feature T173, “Extended LIKE clause in table definition”). The clause can be used to create a table based on a definition of an existing table. Additionally, users
can extend the original table or exclude certain parts of it. In contrast to the SQL standard the clause must be defined at the top-level of a CREATE statement. That is because the clause applies to multiple parts of the definition and not only to the schema part.

You can use the clause to reuse (and potentially overwrite) certain connector properties or add watermarks to tables defined externally. For example, you can add a watermark to a table defined in Apache Hive. 

Consider the example statement below:
```sql
CREATE TABLE Orders (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3)
) WITH ( 
    'connector' = 'kafka',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE Orders_with_watermark (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    -- Overwrite the startup-mode
    'scan.startup.mode' = 'latest-offset'
)
LIKE Orders;
```

The resulting table `Orders_with_watermark` will be equivalent to a table created with a following statement:
```sql
CREATE TABLE Orders_with_watermark (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    'scan.startup.mode' = 'latest-offset'
);
```

The merging logic of table features can be controlled with `like options`.

You can control the merging behavior of:

* CONSTRAINTS - constraints such as primary and unique keys
* GENERATED - computed columns
* METADATA - metadata columns
* OPTIONS - connector options that describe connector and format properties
* PARTITIONS - partition of the tables
* WATERMARKS - watermark declarations

with three different merging strategies:

* INCLUDING - Includes the feature of the source table, fails on duplicate entries, e.g. if an option with the same key exists in both tables.
* EXCLUDING - Does not include the given feature of the source table.
* OVERWRITING - Includes the feature of the source table, overwrites duplicate entries of the source table with properties of the new table, e.g. if an option with the same key exists in both tables, the one from the current statement will be used.

Additionally, you can use the `INCLUDING/EXCLUDING ALL` option to specify what should be the strategy if there was no specific strategy defined, i.e. if you use `EXCLUDING ALL INCLUDING WATERMARKS` only the watermarks will be included from the source table.

Example:
```sql
-- A source table stored in a filesystem
CREATE TABLE Orders_in_file (
    `user` BIGINT,
    product STRING,
    order_time_string STRING,
    order_time AS to_timestamp(order_time)
    
)
PARTITIONED BY (`user`) 
WITH ( 
    'connector' = 'filesystem',
    'path' = '...'
);

-- A corresponding table we want to store in kafka
CREATE TABLE Orders_in_kafka (
    -- Add watermark definition
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    ...
)
LIKE Orders_in_file (
    -- Exclude everything besides the computed columns which we need to generate the watermark for.
    -- We do not want to have the partitions or filesystem options as those do not apply to kafka. 
    EXCLUDING ALL
    INCLUDING GENERATED
);
```

If you provide no like options, `INCLUDING ALL OVERWRITING OPTIONS` will be used as a default.

**NOTE** You cannot control the behavior of merging physical columns. Those will be merged as if you applied the `INCLUDING` strategy.

**NOTE** The `source_table` can be a compound identifier. Thus, it can be a table from a different catalog or database: e.g. `my_catalog.my_db.MyTable` specifies table `MyTable` from catalog `MyCatalog` and database `my_db`; `my_db.MyTable` specifies table `MyTable` from current catalog and database `my_db`.

{{< top >}}

## CREATE CATALOG

```sql
CREATE CATALOG catalog_name
  WITH (key1=val1, key2=val2, ...)
```

Create a catalog with the given catalog properties. If a catalog with the same name already exists, an exception is thrown.

**WITH OPTIONS**

Catalog properties used to store extra information related to this catalog.
The key and value of expression `key1=val1` should both be string literal.

Check out more details at [Catalogs]({{< ref "docs/dev/table/catalogs" >}}).

{{< top >}}

## CREATE DATABASE

```sql
CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...)
```

Create a database with the given database properties. If a database with the same name already exists in the catalog, an exception is thrown.

**IF NOT EXISTS**

If the database already exists, nothing happens.

**WITH OPTIONS**

Database properties used to store extra information related to this database.
The key and value of expression `key1=val1` should both be string literal.

{{< top >}}

## CREATE VIEW
```sql
CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name
  [( columnName [, columnName ]* )] [COMMENT view_comment]
  AS query_expression
```

Create a view with the given query expression. If a view with the same name already exists in the catalog, an exception is thrown.

**TEMPORARY**

Create temporary view that has catalog and database namespaces and overrides views.

**IF NOT EXISTS**

If the view already exists, nothing happens.

{{< top >}}

## CREATE FUNCTION
```sql
CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION 
  [IF NOT EXISTS] [catalog_name.][db_name.]function_name 
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
```

Create a catalog function that has catalog and database namespaces with the identifier and optional language tag. If a function with the same name already exists in the catalog, an exception is thrown.

If the language tag is JAVA/SCALA, the identifier is the full classpath of the UDF. For the implementation of Java/Scala UDF, please refer to [User-defined Functions]({{< ref "docs/dev/table/functions/udfs" >}}) for more details.

If the language tag is PYTHON, the identifier is the fully qualified name of the UDF, e.g. `pyflink.table.tests.test_udf.add`. For the implementation of Python UDF, please refer to [Python UDFs]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}) for more details.

If the language tag is PYTHON, however the current program is written in Java/Scala or pure SQL, then you need to [configure the Python dependencies]({{< ref "docs/dev/python/dependency_management" >}}#python-dependency-in-javascala-program).

**TEMPORARY**

Create temporary catalog function that has catalog and database namespaces and overrides catalog functions.

**TEMPORARY SYSTEM**

Create temporary system function that has no namespace and overrides built-in functions

**IF NOT EXISTS**

If the function already exists, nothing happens.

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag to instruct Flink runtime how to execute the function. Currently only JAVA, SCALA and PYTHON are supported, the default language for a function is JAVA. 
