---
title: "Concepts & Common API"
weight: 2
type: docs
aliases:
  - /dev/table/common.html
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

# Concepts & Common API

The Table API and SQL are integrated in a joint API.
The central concept of this API is a `Table` which serves as input and output of queries.
This document shows the common structure of programs with Table API and SQL queries, how to register a `Table`, how to query a `Table`, and how to emit a `Table`.

Structure of Table API and SQL Programs
---------------------------------------

The following code example shows the common structure of Table API and SQL programs.

{{< hint warning >}}
All Flink Scala APIs are deprecated and will be removed in a future Flink version. You can still build your application in Scala, but you should move to the Java version of either the DataStream and/or Table API.

See <a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-265+Deprecate+and+remove+Scala+API+support">FLIP-265 Deprecate and remove Scala API support</a>
{{< /hint >}}

{{< tabs "0727d1e7-3f22-4eba-a25f-6a554b6a1359" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.api.*;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;

// Create a TableEnvironment for batch or streaming execution.
// See the "Create a TableEnvironment" section for details.
TableEnvironment tableEnv = TableEnvironment.create(/*…*/);

// Create a source table
tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
    .schema(Schema.newBuilder()
      .column("f0", DataTypes.STRING())
      .build())
    .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
    .build());

// Create a sink table (using SQL DDL)
tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");

// Create a Table object from a Table API query
Table table1 = tableEnv.from("SourceTable");

// Create a Table object from a SQL query
Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

// Emit a Table API result Table to a TableSink, same for SQL result
TableResult tableResult = table1.insertInto("SinkTable").execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.api._
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions

// Create a TableEnvironment for batch or streaming execution.
// See the "Create a TableEnvironment" section for details.
val tableEnv = TableEnvironment.create(/*…*/)

// Create a source table
tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
  .schema(Schema.newBuilder()
    .column("f0", DataTypes.STRING())
    .build())
  .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
  .build())

// Create a sink table (using SQL DDL)
tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ")

// Create a Table object from a Table API query
val table1 = tableEnv.from("SourceTable")

// Create a Table object from a SQL query
val table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable")

// Emit a Table API result Table to a TableSink, same for SQL result
val tableResult = table1.insertInto("SinkTable").execute()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *

# Create a TableEnvironment for batch or streaming execution
table_env = ... # see "Create a TableEnvironment" section

# Create a source table
table_env.executeSql("""CREATE TEMPORARY TABLE SourceTable (
  f0 STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '100'
)
""")

# Create a sink table
table_env.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ")

# Create a Table from a Table API query
table1 = table_env.from_path("SourceTable").select(...)

# Create a Table from a SQL query
table2 = table_env.sql_query("SELECT ... FROM SourceTable ...")

# Emit a Table API result Table to a TableSink, same for SQL result
table_result = table1.execute_insert("SinkTable")
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
Table API and SQL queries can be easily integrated with and embedded into DataStream programs.
Have a look at the [DataStream API Integration]({{< ref "docs/dev/table/data_stream_api" >}}) page
to learn how DataStreams can be converted into Tables and vice versa.
{{< /hint >}}

{{< top >}}

Create a TableEnvironment
-------------------------

The `TableEnvironment` is the entrypoint for Table API and SQL integration and is responsible for:

* Registering a `Table` in the internal catalog
* Registering catalogs
* Loading pluggable modules
* Executing SQL queries
* Registering a user-defined (scalar, table, or aggregation) function
* Converting between `DataStream` and `Table` (in case of `StreamTableEnvironment`)

A `Table` is always bound to a specific `TableEnvironment`.
It is not possible to combine tables of different TableEnvironments in the same query, e.g., to join or union them.
A `TableEnvironment` is created by calling the static `TableEnvironment.create()` method.

{{< tabs "e013e0d9-f0d0-4280-ac0f-bb984caffa4c" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    //.inBatchMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(settings);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    //.inBatchMode()
    .build()

val tEnv = TableEnvironment.create(settings)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

```
{{< /tab >}}
{{< /tabs >}}

Alternatively, users can create a `StreamTableEnvironment` from an existing `StreamExecutionEnvironment`
to interoperate with the `DataStream` API.

{{< tabs "c91b91ec-7197-4305-827d-9f91dedadce5" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(s_env)
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

Create Tables in the Catalog
-------------------------------

A `TableEnvironment` maintains a map of catalogs of tables which are created with an identifier. Each
identifier consists of 3 parts: catalog name, database name and object name. If a catalog or database is not
specified, the current default value will be used (see examples in the [Table identifier expanding](#table-identifier-expanding) section).

Tables can be either virtual (`VIEWS`) or regular (`TABLES`). `VIEWS` can be created from an
existing `Table` object, usually the result of a Table API or SQL query. `TABLES` describe
external data, such as a file, database table, or message queue.

### Temporary vs Permanent tables.

Tables may either be temporary, and tied to the lifecycle of a single Flink session, or permanent,
and visible across multiple Flink sessions and clusters.

Permanent tables require a [catalog]({{< ref "docs/dev/table/catalogs" >}}) (such as Hive Metastore)
to maintain metadata about the table. Once a permanent table is created, it is visible to any Flink
session that is connected to the catalog and will continue to exist until the table is explicitly
dropped.

On the other hand, temporary tables are always stored in memory and only exist for the duration of
the Flink session they are created within. These tables are not visible to other sessions. They are
not bound to any catalog or database but can be created in the namespace of one. Temporary tables
are not dropped if their corresponding database is removed.

#### Shadowing

It is possible to register a temporary table with the same identifier as an existing permanent
table. The temporary table shadows the permanent one and makes the permanent table inaccessible as
long as the temporary one exists. All queries with that identifier will be executed against the
temporary table.

This might be useful for experimentation. It allows running exactly the same query first against a
temporary table that e.g. has just a subset of data, or the data is obfuscated. Once verified that
the query is correct it can be run against the real production table.

### Create a Table

#### Virtual Tables

A `Table` API object corresponds to a `VIEW` (virtual table) in SQL terms. It encapsulates a logical
query plan. It can be created in a catalog as follows:

{{< tabs "180b1bfe-5749-4b96-a120-e186fd361c8d" >}}
{{< tab "Java" >}}
```java
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
Table projTable = tableEnv.from("X").select(...);

// register the Table projTable as table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
val projTable: Table = tableEnv.from("X").select(...)

// register the Table projTable as table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# table is the result of a simple projection query 
proj_table = table_env.from_path("X").select(...)

# register the Table projTable as table "projectedTable"
table_env.register_table("projectedTable", proj_table)
```
{{< /tab >}}
{{< /tabs >}}

**Note:** `Table` objects are similar to `VIEW`'s from relational database
systems, i.e., the query that defines the `Table` is not optimized but will be inlined when another
query references the registered `Table`. If multiple queries reference the same registered `Table`,
it will be inlined for each referencing query and executed multiple times, i.e., the result of the
registered `Table` will *not* be shared.

{{< top >}}

#### Connector Tables

It is also possible to create a `TABLE` as known from relational databases from a [connector]({{< ref "docs/connectors/table/overview" >}}) declaration.
The connector describes the external system that stores the data of a table. Storage systems such as Apache Kafka or a regular file system can be declared here.

Such tables can either be created using the Table API directly, or by switching to SQL DDL.

{{< tabs "059e9a56-282c-5e78-98d3-85be5abd04a2" >}}
{{< tab "Java" >}}
```java
// Using table descriptors
final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
    .schema(Schema.newBuilder()
    .column("f0", DataTypes.STRING())
    .build())
    .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
    .build();

tableEnv.createTable("SourceTableA", sourceDescriptor);
tableEnv.createTemporaryTable("SourceTableB", sourceDescriptor);

// Using SQL DDL
tableEnv.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# Using table descriptors
source_descriptor = TableDescriptor.for_connector("datagen") \
    .schema(Schema.new_builder()
            .column("f0", DataTypes.STRING())
            .build()) \
    .option("rows-per-second", "100") \
    .build()

t_env.create_table("SourceTableA", source_descriptor)
t_env.create_temporary_table("SourceTableB", source_descriptor)

# Using SQL DDL
t_env.execute_sql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)")
```
{{< /tab >}}
{{< /tabs >}}

### Expanding Table identifiers

Tables are always registered with a 3-part identifier consisting of catalog, database, and table name.

Users can set one catalog and one database inside it to be the “current catalog” and “current database”.
With them, the first two parts in the 3-parts identifier mentioned above can be optional - if they are not provided,
the current catalog and current database will be referred. Users can switch the current catalog and current database via
table API or SQL.

Identifiers follow SQL requirements which means that they can be escaped with a backtick character (`` ` ``).

{{< tabs "059e9a56-282c-4e69-98d3-85be9abd06a3" >}}
{{< tab "Java" >}}
```java
TableEnvironment tEnv = ...;
tEnv.useCatalog("custom_catalog");
tEnv.useDatabase("custom_database");

Table table = ...;

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("exampleView", table);

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_database.exampleView", table);

// register the view named 'example.View' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("`example.View`", table);

// register the view named 'exampleView' in the catalog named 'other_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_catalog.other_database.exampleView", table);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tEnv: TableEnvironment = ...
tEnv.useCatalog("custom_catalog")
tEnv.useDatabase("custom_database")

val table: Table = ...

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("exampleView", table)

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_database.exampleView", table)

// register the view named 'example.View' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("`example.View`", table)

// register the view named 'exampleView' in the catalog named 'other_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_catalog.other_database.exampleView", table)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
t_env = TableEnvironment.create(...)
t_env.use_catalog("custom_catalog")
t_env.use_database("custom_database")

table = ...

# register the view named 'exampleView' in the catalog named 'custom_catalog'
# in the database named 'custom_database'
t_env.create_temporary_view("other_database.exampleView", table)

# register the view named 'example.View' in the catalog named 'custom_catalog'
# in the database named 'custom_database'
t_env.create_temporary_view("`example.View`", table)

# register the view named 'exampleView' in the catalog named 'other_catalog'
# in the database named 'other_database'
t_env.create_temporary_view("other_catalog.other_database.exampleView", table)
```
{{< /tab >}}
{{< /tabs >}}

Query a Table
-------------

### Table API

The Table API is a language-integrated query API for Scala and Java. In contrast to SQL, queries are not specified as Strings but are composed step-by-step in the host language. 

The API is based on the `Table` class which represents a table (streaming or batch) and offers methods to apply relational operations. These methods return a new `Table` object, which represents the result of applying the relational operation on the input `Table`. Some relational operations are composed of multiple method calls such as `table.groupBy(...).select()`, where `groupBy(...)` specifies a grouping of `table`, and `select(...)` the projection on the grouping of `table`.

The [Table API]({{< ref "docs/dev/table/tableApi" >}}) document describes all Table API operations that are supported on streaming and batch tables.

The following example shows a simple Table API aggregation query:

{{< tabs "53400a89-4d54-4c67-a731-f3ca25aaf1f4" >}}
{{< tab "Java" >}}
```java
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// register Orders table

// scan registered Orders table
Table orders = tableEnv.from("Orders");
// compute revenue for all customers from France
Table revenue = orders
  .filter($("cCountry").isEqual("FRANCE"))
  .groupBy($("cID"), $("cName"))
  .select($("cID"), $("cName"), $("revenue").sum().as("revSum"));

// emit or convert Table
// execute query
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// scan registered Orders table
val orders = tableEnv.from("Orders")
// compute revenue for all customers from France
val revenue = orders
  .filter($"cCountry" === "FRANCE")
  .groupBy($"cID", $"cName")
  .select($"cID", $"cName", $"revenue".sum AS "revSum")

// emit or convert Table
// execute query
```

**Note:** The Scala Table API uses Scala String interpolation that starts with a dollar sign (`$`) to reference the attributes of a `Table`. The Table API uses Scala implicits. Make sure to import
* `org.apache.flink.table.api._` - for implicit expression conversions 
* `org.apache.flink.api.scala._` and `org.apache.flink.table.api.bridge.scala._` if you want to convert from/to DataStream.
{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
table_env = # see "Create a TableEnvironment" section

# register Orders table

# scan registered Orders table
orders = table_env.from_path("Orders")
# compute revenue for all customers from France
revenue = orders \
    .filter(col('cCountry') == 'FRANCE') \
    .group_by(col('cID'), col('cName')) \
    .select(col('cID'), col('cName'), col('revenue').sum.alias('revSum'))

# emit or convert Table
# execute query
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### SQL

Flink's SQL integration is based on [Apache Calcite](https://calcite.apache.org), which implements the SQL standard. SQL queries are specified as regular Strings.

The [SQL]({{< ref "docs/dev/table/sql/overview" >}}) document describes Flink's SQL support for streaming and batch tables.

The following example shows how to specify a query and return the result as a `Table`.

{{< tabs "8ae3f8c2-315a-4941-a8a7-bffa677b4404" >}}
{{< tab "Java" >}}
```java
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// register Orders table

// compute revenue for all customers from France
Table revenue = tableEnv.sqlQuery(
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );

// emit or convert Table
// execute query
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// compute revenue for all customers from France
val revenue = tableEnv.sqlQuery("""
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

// emit or convert Table
// execute query
```

{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# register Orders table

# compute revenue for all customers from France
revenue = table_env.sql_query(
    "SELECT cID, cName, SUM(revenue) AS revSum "
    "FROM Orders "
    "WHERE cCountry = 'FRANCE' "
    "GROUP BY cID, cName"
)

# emit or convert Table
# execute query
```
{{< /tab >}}
{{< /tabs >}}

The following example shows how to specify an update query that inserts its result into a registered table.

{{< tabs "3dad7016-6707-4218-98f2-785635c88cde" >}}
{{< tab "Java" >}}
```java
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// register "Orders" table
// register "RevenueFrance" output table

// compute revenue for all customers from France and emit to "RevenueFrance"
tableEnv.executeSql(
    "INSERT INTO RevenueFrance " +
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register "Orders" table
// register "RevenueFrance" output table

// compute revenue for all customers from France and emit to "RevenueFrance"
tableEnv.executeSql("""
  |INSERT INTO RevenueFrance
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

```

{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# register "Orders" table
# register "RevenueFrance" output table

# compute revenue for all customers from France and emit to "RevenueFrance"
table_env.execute_sql(
    "INSERT INTO RevenueFrance "
    "SELECT cID, cName, SUM(revenue) AS revSum "
    "FROM Orders "
    "WHERE cCountry = 'FRANCE' "
    "GROUP BY cID, cName"
)

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Mixing Table API and SQL

Table API and SQL queries can be easily mixed because both return `Table` objects:

* A Table API query can be defined on the `Table` object returned by a SQL query.
* A SQL query can be defined on the result of a Table API query by [registering the resulting Table](#register-a-table) in the `TableEnvironment` and referencing it in the `FROM` clause of the SQL query.

{{< top >}}

Emit a Table 
------------

A `Table` is emitted by writing it to a `TableSink`. A `TableSink` is a generic interface to support a wide variety of file formats (e.g. CSV, Apache Parquet, Apache Avro), storage systems (e.g., JDBC, Apache HBase, Apache Cassandra, Elasticsearch), or messaging systems (e.g., Apache Kafka, RabbitMQ). 

A batch `Table` can only be written to a `BatchTableSink`, while a streaming `Table` requires either an `AppendStreamTableSink`, a `RetractStreamTableSink`, or an `UpsertStreamTableSink`. 

Please see the documentation about [Table Sources & Sinks]({{< ref "docs/dev/table/sourcesSinks" >}}) for details about available sinks and instructions for how to implement a custom `DynamicTableSink`.

The `Table.insertInto(String tableName)` method defines a complete end-to-end pipeline emitting the source table to a registered sink table.
The method looks up the table sink from the catalog by the name and validates that the schema of the `Table` is identical to the schema of the sink.
A pipeline can be explained with `TablePipeline.explain()` and executed invoking `TablePipeline.execute()`.

The following examples shows how to emit a `Table`:

{{< tabs "08af6f8e-246c-451f-939b-96dc9b886b37" >}}
{{< tab "Java" >}}
```java
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// create an output Table
final Schema schema = Schema.newBuilder()
    .column("a", DataTypes.INT())
    .column("b", DataTypes.STRING())
    .column("c", DataTypes.BIGINT())
    .build();

tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/path/to/file")
    .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", "|")
        .build())
    .build());

// compute a result Table using Table API operators and/or SQL queries
Table result = ...;

// Prepare the insert into pipeline
TablePipeline pipeline = result.insertInto("CsvSinkTable");

// Print explain details
pipeline.printExplain();

// emit the result Table to the registered TableSink
pipeline.execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// create an output Table
val schema = Schema.newBuilder()
    .column("a", DataTypes.INT())
    .column("b", DataTypes.STRING())
    .column("c", DataTypes.BIGINT())
    .build()

tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/path/to/file")
    .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", "|")
        .build())
    .build())

// compute a result Table using Table API operators and/or SQL queries
val result: Table = ...

// Prepare the insert into pipeline
val pipeline = result.insertInto("CsvSinkTable")

// Print explain details
pipeline.printExplain()

// emit the result Table to the registered TableSink
pipeline.execute()

```
{{< /tab >}}
{{< tab "Python" >}}
```python
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# create a TableSink
schema = Schema.new_builder()
    .column("a", DataTypes.INT())
    .column("b", DataTypes.STRING())
    .column("c", DataTypes.BIGINT())
    .build()
    
table_env.create_temporary_table("CsvSinkTable", TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", "/path/to/file")
    .format(FormatDescriptor.for_format("csv")
        .option("field-delimiter", "|")
        .build())
    .build())

# compute a result Table using Table API operators and/or SQL queries
result = ...

# emit the result Table to the registered TableSink
result.execute_insert("CsvSinkTable")

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}


Translate and Execute a Query
-----------------------------

Table API and SQL queries are translated into [DataStream]({{< ref "docs/dev/datastream/overview" >}}) programs whether their input is streaming or batch.
A query is internally represented as a logical query plan and is translated in two phases:

1. Optimization of the logical plan,
2. Translation into a DataStream program.

A Table API or SQL query is translated when:

* `TableEnvironment.executeSql()` is called. This method is used for executing a given statement, and the sql query is translated immediately once this method is called.
* `TablePipeline.execute()` is called. This method is used for executing a source-to-sink pipeline, and the Table API program is translated immediately once this method is called.
* `Table.execute()` is called. This method is used for collecting the table content to the local client, and the Table API is translated immediately once this method is called.
* `StatementSet.execute()` is called. A `TablePipeline` (emitted to a sink through `StatementSet.add()`) or an INSERT statement (specified through `StatementSet.addInsertSql()`) will be buffered in `StatementSet` first. They are transformed once `StatementSet.execute()` is called. All sinks will be optimized into one DAG.
* A `Table` is translated when it is converted into a `DataStream` (see [Integration with DataStream](#integration-with-datastream)). Once translated, it's a regular DataStream program and is executed when `StreamExecutionEnvironment.execute()` is called.

{{< top >}}

Query Optimization
------------------

Apache Flink leverages and extends Apache Calcite to perform sophisticated query optimization.
This includes a series of rule and cost-based optimizations such as:

* Subquery decorrelation based on Apache Calcite
* Project pruning
* Partition pruning
* Filter push-down
* Sub-plan deduplication to avoid duplicate computation
* Special subquery rewriting, including two parts:
    * Converts IN and EXISTS into left semi-joins
    * Converts NOT IN and NOT EXISTS into left anti-join
* Optional join reordering
    * Enabled via `table.optimizer.join-reorder-enabled`

**Note:** IN/EXISTS/NOT IN/NOT EXISTS are currently only supported in conjunctive conditions in subquery rewriting.

The optimizer makes intelligent decisions, based not only on the plan but also rich statistics available from the data sources and fine-grain costs for each operator such as io, cpu, network, and memory.

Advanced users may provide custom optimizations via a `CalciteConfig` object that can be provided to the table environment by calling `TableEnvironment#getConfig#setPlannerConfig`.


Explaining a Table
------------------

The Table API provides a mechanism to explain the logical and optimized query plans to compute a `Table`. 
This is done through the `Table.explain()` method or `StatementSet.explain()` method. `Table.explain()`returns the plan of a `Table`. `StatementSet.explain()` returns the plan of multiple sinks. It returns a String describing three plans:

1. the Abstract Syntax Tree of the relational query, i.e., the unoptimized logical query plan,
2. the optimized logical query plan, and
3. the physical execution plan.

`TableEnvironment.explainSql()` and `TableEnvironment.executeSql()` support execute a `EXPLAIN` statement to get the plans, Please refer to [EXPLAIN]({{< ref "docs/dev/table/sql/explain" >}}) page.

The following code shows an example and the corresponding output for given `Table` using `Table.explain()` method:

{{< tabs "152e6feb-1fa8-42b4-9c2f-993442487a5c" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

// explain Table API
Table table1 = tEnv.fromDataStream(stream1, $("count"), $("word"));
Table table2 = tEnv.fromDataStream(stream2, $("count"), $("word"));
Table table = table1
  .where($("word").like("F%"))
  .unionAll(table2);

System.out.println(table.explain());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table2 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table = table1
  .where($"word".like("F%"))
  .unionAll(table2)

println(table.explain())
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

table1 = t_env.from_elements([(1, "hello")], ["count", "word"])
table2 = t_env.from_elements([(1, "hello")], ["count", "word"])
table = table1 \
    .where(col('word').like('F%')) \
    .union_all(table2)
print(table.explain())

```
{{< /tab >}}
{{< /tabs >}}

The result of the above example is

{{< details "Explain" >}}
```text

== Abstract Syntax Tree ==
LogicalUnion(all=[true])
:- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
:  +- LogicalTableScan(table=[[Unregistered_DataStream_1]])
+- LogicalTableScan(table=[[Unregistered_DataStream_2]])

== Optimized Physical Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
+- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])

== Optimized Execution Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
+- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])

```
{{< /details >}}

The following code shows an example and the corresponding output for multiple-sinks plan using `StatementSet.explain()` method:

{{< tabs "46971f29-7db6-46d2-b9a1-53ee0918752f" >}}
{{< tab "Java" >}}
```java

EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tEnv = TableEnvironment.create(settings);

final Schema schema = Schema.newBuilder()
    .column("count", DataTypes.INT())
    .column("word", DataTypes.STRING())
    .build();

tEnv.createTemporaryTable("MySource1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path1")
    .format("csv")
    .build());
tEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path2")
    .format("csv")
    .build());
tEnv.createTemporaryTable("MySink1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path1")
    .format("csv")
    .build());
tEnv.createTemporaryTable("MySink2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path2")
    .format("csv")
    .build());

StatementSet stmtSet = tEnv.createStatementSet();

Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
stmtSet.add(table1.insertInto("MySink1"));

Table table2 = table1.unionAll(tEnv.from("MySource2"));
stmtSet.add(table2.insertInto("MySink2"));

String explanation = stmtSet.explain();
System.out.println(explanation);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings.inStreamingMode()
val tEnv = TableEnvironment.create(settings)

val schema = Schema.newBuilder()
    .column("count", DataTypes.INT())
    .column("word", DataTypes.STRING())
    .build()

tEnv.createTemporaryTable("MySource1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path1")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path2")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySink1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path1")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySink2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path2")
    .format("csv")
    .build())
    
val stmtSet = tEnv.createStatementSet()

val table1 = tEnv.from("MySource1").where($"word".like("F%"))
stmtSet.add(table1.insertInto("MySink1"))

val table2 = table1.unionAll(tEnv.from("MySource2"))
stmtSet.add(table2.insertInto("MySink2"))

val explanation = stmtSet.explain()
println(explanation)

```
{{< /tab >}}
{{< tab "Python" >}}
```python
settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=settings)

schema = Schema.new_builder()
    .column("count", DataTypes.INT())
    .column("word", DataTypes.STRING())
    .build()

t_env.create_temporary_table("MySource1", TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", "/source/path1")
    .format("csv")
    .build())
t_env.create_temporary_table("MySource2", TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", "/source/path2")
    .format("csv")
    .build())
t_env.create_temporary_table("MySink1", TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", "/sink/path1")
    .format("csv")
    .build())
t_env.create_temporary_table("MySink2", TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", "/sink/path2")
    .format("csv")
    .build())

stmt_set = t_env.create_statement_set()

table1 = t_env.from_path("MySource1").where(col('word').like('F%'))
stmt_set.add_insert("MySink1", table1)

table2 = table1.union_all(t_env.from_path("MySource2"))
stmt_set.add_insert("MySink2", table2)

explanation = stmt_set.explain()
print(explanation)

```
{{< /tab >}}
{{< /tabs >}}

the result of multiple-sinks plan is

{{< details "MultiTable Explain" >}}
```text

== Abstract Syntax Tree ==
LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])

LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- LogicalUnion(all=[true])
   :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]])

== Optimized Physical Plan ==
LegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

LegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
   :  +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

== Optimized Execution Plan ==
Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])(reuse_id=[1])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

LegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- Reused(reference_id=[1])

LegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Reused(reference_id=[1])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

```
{{< /details >}}

{{< top >}}


