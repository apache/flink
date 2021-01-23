---
title: "Concepts & Common API"
nav-parent_id: tableapi
nav-pos: 0
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

The Table API and SQL are integrated in a joint API.
The central concept of this API is a `Table` which serves as input and output of queries.
This document shows the common structure of programs with Table API and SQL queries, how to register a `Table`, how to query a `Table`, and how to emit a `Table`.

* This will be replaced by the TOC
{:toc}

Structure of Table API and SQL Programs
---------------------------------------

The following code example shows the common structure of Table API and SQL programs.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// create a TableEnvironment for batch or streaming execution
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// create an input Table
tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )");
// register an output Table
tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");

// create a Table object from a Table API query
Table table2 = tableEnv.from("table1").select(...);
// create a Table object from a SQL query
Table table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ... ");

// emit a Table API result Table to a TableSink, same for SQL result
TableResult tableResult = table2.executeInsert("outputTable");
tableResult...

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

// create a TableEnvironment for batch or streaming execution
val tableEnv = ... // see "Create a TableEnvironment" section

// create an input Table
tableEnv.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )")
// register an output Table
tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )")

// create a Table from a Table API query
val table2 = tableEnv.from("table1").select(...)
// create a Table from a SQL query
val table3 = tableEnv.sqlQuery("SELECT ... FROM table1 ...")

// emit a Table API result Table to a TableSink, same for SQL result
val tableResult = table2.executeInsert("outputTable")
tableResult...

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

# create a TableEnvironment for batch or streaming execution
table_env = ... # see "Create a TableEnvironment" section

# register an input Table
table_env.executeSql("CREATE TEMPORARY TABLE table1 ... WITH ( 'connector' = ... )")
# register an output Table
table_env.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )")

# create a Table from a Table API query
table2 = table_env.from_path("table1").select(...)
# create a Table from a SQL query
table3 = table_env.sql_query("SELECT ... FROM table1 ...")

# emit a Table API result Table to a TableSink, same for SQL result
table_result = table3.execute_insert("outputTable")
table_result...

{% endhighlight %}
</div>
</div>

**Note:** Table API and SQL queries can be easily integrated with and embedded into DataStream programs.
Have a look at the [Integration with DataStream](#integration-with-datastream) section to learn how DataStreams can be converted into Tables and vice versa.

{% top %}

Create a TableEnvironment
-------------------------

The `TableEnvironment` is the entrypoint for Table API and SQL integration and is responsible for:

* Registering a `Table` in the internal catalog
* Registering catalogs
* Loading pluggable modules
* Executing SQL queries
* Registering a user-defined (scalar, table, or aggregation) function
* Converting a `DataStream` into a `Table`
* In case of `StreamTableEnvironment` holding a reference to a `StreamExecutionEnvironment`

A `Table` is always bound to a specific `TableEnvironment`.
It is not possible to combine tables of different TableEnvironments in the same query, e.g., to join or union them.
A `TableEnvironment` is created by calling the static `TableEnvironment.create()` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

EnvironmentSettings settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    //.inBatchMode()
    .build();

TableEnvironment tEnv = TableEnvironment.create(setting);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    //.inBatchMode()
    .build()

val tEnv = TableEnvironment.create(setting)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, BatchTableEnvironment

# create a blink streaming TableEnvironment
env_settings = EnvironmentSettings.new_instance().use_blink_planner().build()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# create a blink batch TableEnvironment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = BatchTableEnvironment.create(environment_settings=env_settings)

{% endhighlight %}
</div>
</div>

Alternatively, users can create a `StreamTableEnvironment` from an existing `StreamExecutionEnvironment`
to interoperate with the `DataStream` API.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

s_env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(s_env, environment_settings=settings)
{% endhighlight %}
</div>
</div>

{% top %}

Create Tables in the Catalog
-------------------------------

A `TableEnvironment` maintains a map of catalogs of tables which are created with an identifier. Each
identifier consists of 3 parts: catalog name, database name and object name. If a catalog or database is not
specified, the current default value will be used (see examples in the [Table identifier expanding]({% link dev/table/common.md %}#table-identifier-expanding) section).

Tables can be either virtual (`VIEWS`) or regular (`TABLES`). `VIEWS` can be created from an
existing `Table` object, usually the result of a Table API or SQL query. `TABLES` describe
external data, such as a file, database table, or message queue.

### Temporary vs Permanent tables.

Tables may either be temporary, and tied to the lifecycle of a single Flink session, or permanent,
and visible across multiple Flink sessions and clusters.

Permanent tables require a [catalog]({% link dev/table/catalogs.md %}) (such as Hive Metastore)
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

A `Table` API object corresponds to a `VIEW` (virtual table) in a SQL terms. It encapsulates a logical
query plan. It can be created in a catalog as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
Table projTable = tableEnv.from("X").select(...);

// register the Table projTable as table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
val projTable: Table = tableEnv.from("X").select(...)

// register the Table projTable as table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# table is the result of a simple projection query 
proj_table = table_env.from_path("X").select(...)

# register the Table projTable as table "projectedTable"
table_env.register_table("projectedTable", proj_table)
{% endhighlight %}
</div>
</div>

**Note:** `Table` objects are similar to `VIEW`'s from relational database
systems, i.e., the query that defines the `Table` is not optimized but will be inlined when another
query references the registered `Table`. If multiple queries reference the same registered `Table`,
it will be inlined for each referencing query and executed multiple times, i.e., the result of the
registered `Table` will *not* be shared.

{% top %}

#### Connector Tables

It is also possible to create a `TABLE` as known from relational databases from a [connector]({% link dev/table/connect.md %}) declaration.
The connector describes the external system that stores the data of a table. Storage systems such as Apache Kafka or a regular file system can be declared here.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tableEnvironment
  .connect(...)
  .withFormat(...)
  .withSchema(...)
  .inAppendMode()
  .createTemporaryTable("MyTable")
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tableEnvironment
  .connect(...)
  .withFormat(...)
  .withSchema(...)
  .inAppendMode()
  .createTemporaryTable("MyTable")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
table_environment \
    .connect(...) \
    .with_format(...) \
    .with_schema(...) \
    .in_append_mode() \
    .create_temporary_table("MyTable")
{% endhighlight %}
</div>

<div data-lang="DDL" markdown="1">
{% highlight sql %}
tableEnvironment.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)")
{% endhighlight %}
</div>
</div>

### Expanding Table identifiers

Tables are always registered with a 3-part identifier consisting of catalog, database, and table name.

Users can set one catalog and one database inside it to be the “current catalog” and “current database”.
With them, the first two parts in the 3-parts identifier mentioned above can be optional - if they are not provided,
the current catalog and current database will be referred. Users can switch the current catalog and current database via
table API or SQL.

Identifiers follow SQL requirements which means that they can be escaped with a backtick character (`` ` ``).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tEnv: TableEnvironment = ...;
tEnv.useCatalog("custom_catalog")
tEnv.useDatabase("custom_database")

val table: Table = ...;

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
{% endhighlight %}
</div>

</div>

Query a Table
-------------

### Table API

The Table API is a language-integrated query API for Scala and Java. In contrast to SQL, queries are not specified as Strings but are composed step-by-step in the host language. 

The API is based on the `Table` class which represents a table (streaming or batch) and offers methods to apply relational operations. These methods return a new `Table` object, which represents the result of applying the relational operation on the input `Table`. Some relational operations are composed of multiple method calls such as `table.groupBy(...).select()`, where `groupBy(...)` specifies a grouping of `table`, and `select(...)` the projection on the grouping of `table`.

The [Table API]({% link dev/table/tableApi.md %}) document describes all Table API operations that are supported on streaming and batch tables.

The following example shows a simple Table API aggregation query:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// register Orders table

// scan registered Orders table
Table orders = tableEnv.from("Orders");
// compute revenue for all customers from France
Table revenue = orders
  .filter($("cCountry").isEqual("FRANCE"))
  .groupBy($("cID"), $("cName")
  .select($("cID"), $("cName"), $("revenue").sum().as("revSum"));

// emit or convert Table
// execute query
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
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
{% endhighlight %}

**Note:** The Scala Table API uses Scala String interpolation that starts with a dollar sign (`$`) to reference the attributes of a `Table`. The Table API uses Scala implicits. Make sure to import
* `org.apache.flink.table.api._` - for implicit expression conversions 
* `org.apache.flink.api.scala._` and `org.apache.flink.table.api.bridge.scala._` if you want to convert from/to DataStream.
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = # see "Create a TableEnvironment" section

# register Orders table

# scan registered Orders table
orders = table_env.from_path("Orders")
# compute revenue for all customers from France
revenue = orders \
    .filter(orders.cCountry == 'FRANCE') \
    .group_by(orders.cID, orders.cName) \
    .select(orders.cID, orders.cName, orders.revenue.sum.alias('revSum'))

# emit or convert Table
# execute query
{% endhighlight %}
</div>
</div>

{% top %}

### SQL

Flink's SQL integration is based on [Apache Calcite](https://calcite.apache.org), which implements the SQL standard. SQL queries are specified as regular Strings.

The [SQL]({% link dev/table/sql/index.md %}) document describes Flink's SQL support for streaming and batch tables.

The following example shows how to specify a query and return the result as a `Table`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
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
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">
{% highlight python %}
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
{% endhighlight %}
</div>
</div>

The following example shows how to specify an update query that inserts its result into a registered table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
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

{% endhighlight %}

</div>

<div data-lang="python" markdown="1">
{% highlight python %}
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

{% endhighlight %}
</div>
</div>

{% top %}

### Mixing Table API and SQL

Table API and SQL queries can be easily mixed because both return `Table` objects:

* A Table API query can be defined on the `Table` object returned by a SQL query.
* A SQL query can be defined on the result of a Table API query by [registering the resulting Table](#register-a-table) in the `TableEnvironment` and referencing it in the `FROM` clause of the SQL query.

{% top %}

Emit a Table 
------------

A `Table` is emitted by writing it to a `TableSink`. A `TableSink` is a generic interface to support a wide variety of file formats (e.g. CSV, Apache Parquet, Apache Avro), storage systems (e.g., JDBC, Apache HBase, Apache Cassandra, Elasticsearch), or messaging systems (e.g., Apache Kafka, RabbitMQ). 

A batch `Table` can only be written to a `BatchTableSink`, while a streaming `Table` requires either an `AppendStreamTableSink`, a `RetractStreamTableSink`, or an `UpsertStreamTableSink`. 

Please see the documentation about [Table Sources & Sinks]({% link dev/table/sourceSinks.md %}) for details about available sinks and instructions for how to implement a custom `TableSink`.

The `Table.executeInsert(String tableName)` method emits the `Table` to a registered `TableSink`. The method looks up the `TableSink` from the catalog by the name and validates that the schema of the `Table` is identical to the schema of the `TableSink`. 

The following examples shows how to emit a `Table`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a TableEnvironment
TableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// create an output Table
final Schema schema = new Schema()
    .field("a", DataTypes.INT())
    .field("b", DataTypes.STRING())
    .field("c", DataTypes.BIGINT());

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("CsvSinkTable");

// compute a result Table using Table API operators and/or SQL queries
Table result = ...

// emit the result Table to the registered TableSink
result.executeInsert("CsvSinkTable");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// create an output Table
val schema = new Schema()
    .field("a", DataTypes.INT())
    .field("b", DataTypes.STRING())
    .field("c", DataTypes.BIGINT())

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("CsvSinkTable")

// compute a result Table using Table API operators and/or SQL queries
val result: Table = ...

// emit the result Table to the registered TableSink
result.executeInsert("CsvSinkTable")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = ... # see "Create a TableEnvironment" section

# create a TableSink
table_env.connect(FileSystem().path("/path/to/file")))
    .with_format(Csv()
                 .field_delimiter(',')
                 .deriveSchema())
    .with_schema(Schema()
                 .field("a", DataTypes.INT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.BIGINT()))
    .create_temporary_table("CsvSinkTable")

# compute a result Table using Table API operators and/or SQL queries
result = ...

# emit the result Table to the registered TableSink
result.execute_insert("CsvSinkTable")

{% endhighlight %}
</div>
</div>

{% top %}


Translate and Execute a Query
-----------------------------

The behavior of translating and executing a query is different for the two planners.
Table API and SQL queries are translated into [DataStream]({% link dev/datastream_api.md %}) programs whether their input is streaming or batch.
A query is internally represented as a logical query plan and is translated in two phases:

1. Optimization of the logical plan,
2. Translation into a DataStream program.

a Table API or SQL query is translated when:

* `TableEnvironment.executeSql()` is called. This method is used for executing a given statement, and the sql query is translated immediately once this method is called.
* `Table.executeInsert()` is called. This method is used for inserting the table content to the given sink path, and the Table API is translated immediately once this method is called.
* `Table.execute()` is called. This method is used for collecting the table content to local client, and the Table API is translated immediately once this method is called.
* `StatementSet.execute()` is called. A `Table` (emitted to a sink through `StatementSet.addInsert()`) or an INSERT statement (specified through `StatementSet.addInsertSql()`) will be buffered in `StatementSet` first. They are translated once `StatementSet.execute()` is called. All sinks will be optimized into one DAG.
* A `Table` is translated when it is converted into a `DataStream` (see [Integration with DataStream](#integration-with-datastream)). Once translated, it's a regular DataStream program and is executed when `StreamExecutionEnvironment.execute()` is called.

<span class="label label-danger">Attention</span> **Since 1.11 version, `sqlUpdate()` method and `insertInto()` method are deprecated. If the Table program is built from these two methods, we must use `StreamTableEnvironment.execute()` method instead of `StreamExecutionEnvironment.execute()` method to execute it.**

{% top %}

Integration with DataStream 
---------------------------

Table API and SQL queries can be easily integrated with and embedded into [DataStream]({% link dev/datastream_api.md %}) programs.
For instance, it is possible to query an external table (for example from a RDBMS), do some pre-processing, such as filtering, projecting, aggregating, or joining with metadata,
and then further process the data with the DataStream API.
Inversely, a Table API or SQL query can also be applied on the result of a DataStream program.

### Implicit Conversion for Scala

The Scala Table API features implicit conversions for `DataStream` and `Table` classes.
These conversions are enabled by importing the package `org.apache.flink.table.api.bridge.scala._` in addition to `org.apache.flink.api.scala._` for the Scala DataStream API.

### Create a View from a DataStream

A `DataStream` can be registered in a `TableEnvironment` as a View and the schema is dependent only the `DataStream`s underlying data type.
Please see the section about [mapping of data types to table schema](#mapping-of-data-types-to-table-schema) for details.

**Note:** Views created from a `DataStream` can only be registered as temporary views.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

DataStream<Tuple2<Long, String>> stream = ...

// register the DataStream as View "myTable" with fields "f0", "f1"
tableEnv.createTemporaryView("myTable", stream);

// register the DataStream as View "myTable2" with fields "myLong", "myString"
tableEnv.createTemporaryView("myTable2", stream, $("myLong"), $("myString"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = ???

val stream: DataStream[(Long, String)] = ...

// Register the DataStream as View "myTable" with fields "f0", "f1"
tableEnv.createTemporaryView("myTable", stream)

// Register the DataStream as View "myTable2" with fields "myLong", "myString"
tableEnv.createTemporaryView("myTable2", stream, $"myLong", $"myString");
{% endhighlight %}
</div>
</div>

{% top %}

### Convert a DataStream into a Table

A `DataStream` can be directly converted to a `Table` in a `StreamTableEnvironment`.
The schema of the resulting view depends on the data type of the registered collection.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; 
DataStream<Tuple2<Long, String>> stream = ...

Table table2 = tableEnv.fromDataStream(stream, $("myLong"), $("myString"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = ???
val stream: DataStream[(Long, String)] = ???

val table2: Table = tableEnv.fromDataStream(stream, $"myLong", $"myString")
{% endhighlight %}
</div>
</div>

{% top %}

### Convert a Table into a DataStream 

The results of a `Table` can be converted into a `DataStream`.
In this way, custom `DataStream` programs can be run on the result of a Table API or SQL query.

When converting a `Table` into a `DataStream` you need to specify the data type of the resulting records, i.e., the data type into which the rows of the `Table` are to be converted.
Often the most convenient conversion type is `Row`.
The following list gives an overview of the features of the different options:

- **Row**: fields are mapped by position, arbitrary number of fields, support for `null` values, no type-safe access.
- **POJO**: fields are mapped by name (POJO fields must be named as `Table` fields), arbitrary number of fields, support for `null` values, type-safe access.
- **Case Class**: fields are mapped by position, no support for `null` values, type-safe access.
- **Tuple**: fields are mapped by position, limitation to 22 (Scala) or 25 (Java) fields, no support for `null` values, type-safe access.
- **Atomic Type**: `Table` must have a single field, no support for `null` values, type-safe access.

#### Convert a Table into a DataStream

A `Table` that is the result of a streaming query will be updated dynamically, i.e., it is changing as new records arrive on the query's input streams. Hence, the `DataStream` into which such a dynamic query is converted needs to encode the updates of the table. 

There are two modes to convert a `Table` into a `DataStream`:

1. **Append Mode**: This mode can only be used if the dynamic `Table` is only modified by `INSERT` changes, i.e, it is append-only and previously emitted results are never updated.
2. **Retract Mode**: This mode can always be used. It encodes `INSERT` and `DELETE` changes with a `boolean` flag.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; 

Table table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into an append DataStream of Row by specifying the class
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

// Convert the Table into an append DataStream of Tuple2<String, Integer> with TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple = tableEnv.toAppendStream(table, tupleType);

// Convert the Table into a retract DataStream of Row.
// A retract stream of type X is a DataStream<Tuple2<Boolean, X>>. 
// The boolean field indicates the type of the change. 
// True is INSERT, false is DELETE.
DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = ???

// Table with two fields (String name, Integer age)
val table: Table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into an append DataStream of Row by specifying the class
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// Convert the Table into an append DataStream of (String, Integer) with TypeInformation
val dsTuple: DataStream[(String, Int)] dsTuple = 
  tableEnv.toAppendStream[(String, Int)](table)

// Convert the Table into a retract DataStream of Row.
// A retract stream of type X is a DataStream<Tuple2<Boolean, X>>. 
// The boolean field indicates the type of the change. 
// True is INSERT, false is DELETE.
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
{% endhighlight %}
</div>
</div>

**Note:** A detailed discussion about dynamic tables and their properties is given in the [Dynamic Tables](streaming/dynamic_tables.html) document. 

<span class="label label-danger">Attention</span> **Once the Table is converted to a DataStream, please use the `StreamExecutionEnvironment.execute()` method to execute the DataStream program.**

{% top %}

### Mapping of Data Types to Table Schema

Flink's DataStream API support many diverse types.
Composite types such as Tuples (built-in Scala and Flink Java tuples), POJOs, Scala case classes, and Flink's Row type allow for nested data structures with multiple fields that can be accessed in table expressions. Other types are treated as atomic types. In the following, we describe how the Table API converts these types into an internal row representation and show examples of converting a `DataStream` into a `Table`.

The mapping of a data type to a table schema can happen in two ways: **based on the field positions** or **based on the field names**.

**Position-based Mapping**

Position-based mapping can be used to give fields a more meaningful name while keeping the field order. This mapping is available for composite data types *with a defined field order* as well as atomic types. Composite data types such as tuples, rows, and case classes have such a field order. However, fields of a POJO must be mapped based on the field names (see next section). Fields can be projected out but can't be renamed using an alias `as`.

When defining a position-based mapping, the specified names must not exist in the input data type, otherwise the API will assume that the mapping should happen based on the field names. If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section;

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with default field names "f0" and "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with field "myLong" only
Table table = tableEnv.fromDataStream(stream, $("myLong"));

// convert DataStream into Table with field names "myLong" and "myInt"
Table table = tableEnv.fromDataStream(stream, $("myLong"), $("myInt"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with default field names "_1" and "_2"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field "myLong" only
val table: Table = tableEnv.fromDataStream(stream, $"myLong")

// convert DataStream into Table with field names "myLong" and "myInt"
val table: Table = tableEnv.fromDataStream(stream, $"myLong", $"myInt")
{% endhighlight %}
</div>
</div>

**Name-based Mapping**

Name-based mapping can be used for any data type including POJOs. It is the most flexible way of defining a table schema mapping. All fields in the mapping are referenced by name and can be possibly renamed using an alias `as`. Fields can be reordered and projected out.

If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with default field names "f0" and "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with field "f1" only
Table table = tableEnv.fromDataStream(stream, $("f1"));

// convert DataStream into Table with swapped fields
Table table = tableEnv.fromDataStream(stream, $("f1"), $("f0"));

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
Table table = tableEnv.fromDataStream(stream, $("f1").as("myInt"), $("f0").as("myLong"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with default field names "_1" and "_2"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field "_2" only
val table: Table = tableEnv.fromDataStream(stream, $"_2")

// convert DataStream into Table with swapped fields
val table: Table = tableEnv.fromDataStream(stream, $"_2", $"_1")

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
val table: Table = tableEnv.fromDataStream(stream, $"_2" as "myInt", $"_1" as "myLong")
{% endhighlight %}
</div>
</div>

#### Atomic Types

Flink treats primitives (`Integer`, `Double`, `String`) or generic types (types that cannot be analyzed and decomposed) as atomic types.
A `DataStream` of an atomic type is converted into a `Table` with a single column.
The type of the column is inferred from the atomic type and the name of the column can be specified.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...;

DataStream<Long> stream = ...

// Convert DataStream into Table with default field name "f0"
Table table = tableEnv.fromDataStream(stream);

// Convert DataStream into Table with field name "myLong"
Table table = tableEnv.fromDataStream(stream, $("myLong"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = ???

val stream: DataStream[Long] = ...

// Convert DataStream into Table with default field name "f0"
val table: Table = tableEnv.fromDataStream(stream)

// Convert DataStream into Table with field name "myLong"
val table: Table = tableEnv.fromDataStream(stream, $"myLong")
{% endhighlight %}
</div>
</div>

#### Tuples (Scala and Java) and Case Classes (Scala only)

Flink supports Scala's built-in tuples and provides its own tuple classes for Java.
DataStreams of both kinds of tuples can be converted into tables.
Fields can be renamed by providing names for all fields (mapping based on position).
If no field names are specified, the default field names are used.
If the original field names (`f0`, `f1`, ... for Flink Tuples and `_1`, `_2`, ... for Scala Tuples) are referenced, the API assumes that the mapping is name-based instead of position-based.
Name-based mapping allows for reordering fields and projection with alias (`as`).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

DataStream<Tuple2<Long, String>> stream = ...

// convert DataStream into Table with default field names "f0", "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with renamed field names "myLong", "myString" (position-based)
Table table = tableEnv.fromDataStream(stream, $("myLong"), $("myString"));

// convert DataStream into Table with reordered fields "f1", "f0" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1"), $("f0"));

// convert DataStream into Table with projected field "f1" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1"));

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1").as("myString"), $("f0").as("myLong"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, String)] = ...

// convert DataStream into Table with renamed default field names '_1, '_2
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field names "myLong", "myString" (position-based)
val table: Table = tableEnv.fromDataStream(stream, $"myLong", $"myString")

// convert DataStream into Table with reordered fields "_2", "_1" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2", $"_1")

// convert DataStream into Table with projected field "_2" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2")

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2" as "myString", $"_1" as "myLong")

// define case class
case class Person(name: String, age: Int)
val streamCC: DataStream[Person] = ...

// convert DataStream into Table with default field names 'name, 'age
val table = tableEnv.fromDataStream(streamCC)

// convert DataStream into Table with field names 'myName, 'myAge (position-based)
val table = tableEnv.fromDataStream(streamCC, $"myName", $"myAge")

// convert DataStream into Table with reordered and aliased fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"age" as "myAge", $"name" as "myName")

{% endhighlight %}
</div>
</div>

#### POJO (Java and Scala)

Flink supports POJOs as composite types. The rules for what determines a POJO are documented [here]({% link dev/types_serialization.md %}#pojos).

When converting a POJO `DataStream` into a `Table` without specifying field names, the names of the original POJO fields are used. The name mapping requires the original names and cannot be done by positions. Fields can be renamed using an alias (with the `as` keyword), reordered, and projected.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// Person is a POJO with fields "name" and "age"
DataStream<Person> stream = ...

// convert DataStream into Table with default field names "age", "name" (fields are ordered by name!)
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("age").as("myAge"), $("name").as("myName"));

// convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name"));

// convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

// Person is a POJO with field names "name" and "age"
val stream: DataStream[Person] = ...

// convert DataStream into Table with default field names "age", "name" (fields are ordered by name!)
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"age" as "myAge", $"name" as "myName")

// convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name")

// convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName")
{% endhighlight %}
</div>
</div>

#### Row

The `Row` data type supports an arbitrary number of fields and fields with `null` values. Field names can be specified via a `RowTypeInfo` or when converting a `Row` `DataStream` into a `Table`.
The row type supports mapping of fields by position and by name.
Fields can be renamed by providing names for all fields (mapping based on position) or selected individually for projection/ordering/renaming (mapping based on name).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = ...; 

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
DataStream<Row> stream = ...

// Convert DataStream into Table with default field names "name", "age"
Table table = tableEnv.fromDataStream(stream);

// Convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
Table table = tableEnv.fromDataStream(stream, $("myName"), $("myAge"));

// Convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"), $("age").as("myAge"));

// Convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name"));

// Convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = ???

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
val stream: DataStream[Row] = ...

// Convert DataStream into Table with default field names "name", "age"
val table: Table = tableEnv.fromDataStream(stream)

// Convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
val table: Table = tableEnv.fromDataStream(stream, $"myName", $"myAge")

// Convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName", $"age" as "myAge")

// Convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name")

// Convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName")
{% endhighlight %}
</div>
</div>

{% top %}


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

`TableEnvironment.explainSql()` and `TableEnvironment.executeSql()` support execute a `EXPLAIN` statement to get the plans, Please refer to [EXPLAIN]({% link dev/table/sql/explain.md %}) page.

The following code shows an example and the corresponding output for given `Table` using `Table.explain()` method:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table2 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table = table1
  .where($"word".like("F%"))
  .unionAll(table2)
println(table.explain())

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

table1 = t_env.from_elements([(1, "hello")], ["count", "word"])
table2 = t_env.from_elements([(1, "hello")], ["count", "word"])
table = table1 \
    .where(table1.word.like('F%')) \
    .union_all(table2)
print(table.explain())

{% endhighlight %}
</div>
</div>

The result of the above exmaple is
<div>
{% highlight text %}

== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[1], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[2], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[1], fields=[count, word])
  DataStreamScan(id=[2], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

Stage 2 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 3 : Operator
		content : from: (count, word)
		ship_strategy : REBALANCE

		Stage 4 : Operator
			content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
			ship_strategy : FORWARD

			Stage 5 : Operator
				content : from: (count, word)
				ship_strategy : REBALANCE

{% endhighlight %}
</div>

The following code shows an example and the corresponding output for multiple-sinks plan using `StatementSet.explain()` method:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);

final Schema schema = new Schema()
    .field("count", DataTypes.INT())
    .field("word", DataTypes.STRING());

tEnv.connect(new FileSystem().path("/source/path1"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySource1");
tEnv.connect(new FileSystem().path("/source/path2"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySource2");
tEnv.connect(new FileSystem().path("/sink/path1"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySink1");
tEnv.connect(new FileSystem().path("/sink/path2"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySink2");
    
StatementSet stmtSet = tEnv.createStatementSet();

Table table1 = tEnv.from("MySource1").where($("word").like("F%"));
stmtSet.addInsert("MySink1", table1);

Table table2 = table1.unionAll(tEnv.from("MySource2"));
stmtSet.addInsert("MySink2", table2);

String explanation = stmtSet.explain();
System.out.println(explanation);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
val tEnv = TableEnvironment.create(settings)

val schema = new Schema()
    .field("count", DataTypes.INT())
    .field("word", DataTypes.STRING())

tEnv.connect(new FileSystem().path("/source/path1"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySource1")
tEnv.connect(new FileSystem().path("/source/path2"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySource2")
tEnv.connect(new FileSystem().path("/sink/path1"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySink1")
tEnv.connect(new FileSystem().path("/sink/path2"))
    .withFormat(new Csv().deriveSchema())
    .withSchema(schema)
    .createTemporaryTable("MySink2")
    
val stmtSet = tEnv.createStatementSet()

val table1 = tEnv.from("MySource1").where($"word".like("F%"))
stmtSet.addInsert("MySink1", table1)

val table2 = table1.unionAll(tEnv.from("MySource2"))
stmtSet.addInsert("MySink2", table2)

val explanation = stmtSet.explain()
println(explanation)

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
t_env = TableEnvironment.create(environment_settings=settings)

schema = Schema()
    .field("count", DataTypes.INT())
    .field("word", DataTypes.STRING())

t_env.connect(FileSystem().path("/source/path1")))
    .with_format(Csv().deriveSchema())
    .with_schema(schema)
    .create_temporary_table("MySource1")
t_env.connect(FileSystem().path("/source/path2")))
    .with_format(Csv().deriveSchema())
    .with_schema(schema)
    .create_temporary_table("MySource2")
t_env.connect(FileSystem().path("/sink/path1")))
    .with_format(Csv().deriveSchema())
    .with_schema(schema)
    .create_temporary_table("MySink1")
t_env.connect(FileSystem().path("/sink/path2")))
    .with_format(Csv().deriveSchema())
    .with_schema(schema)
    .create_temporary_table("MySink2")
    
stmt_set = t_env.create_statement_set()

table1 = t_env.from_path("MySource1").where(col('word').like('F%'))
stmt_set.add_insert("MySink1", table1)

table2 = table1.union_all(t_env.from_path("MySource2"))
stmt_set.add_insert("MySink2", table2)

explanation = stmt_set.explain()
print(explanation)

{% endhighlight %}
</div>
</div>

the result of multiple-sinks plan is
<div>
{% highlight text %}

== Abstract Syntax Tree ==
LogicalLegacySink(name=[MySink1], fields=[count, word])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])

LogicalLegacySink(name=[MySink2], fields=[count, word])
+- LogicalUnion(all=[true])
   :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]])

== Optimized Logical Plan ==
Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')], reuse_id=[1])
+- TableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

LegacySink(name=[MySink1], fields=[count, word])
+- Reused(reference_id=[1])

LegacySink(name=[MySink2], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Reused(reference_id=[1])
   +- TableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 2 : Operator
		content : CsvTableSource(read fields: count, word)
		ship_strategy : REBALANCE

		Stage 3 : Operator
			content : SourceConversion(table:Buffer(default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]), fields:(count, word))
			ship_strategy : FORWARD

			Stage 4 : Operator
				content : Calc(where: (word LIKE _UTF-16LE'F%'), select: (count, word))
				ship_strategy : FORWARD

				Stage 5 : Operator
					content : SinkConversionToRow
					ship_strategy : FORWARD

					Stage 6 : Operator
						content : Map
						ship_strategy : FORWARD

Stage 8 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 9 : Operator
		content : CsvTableSource(read fields: count, word)
		ship_strategy : REBALANCE

		Stage 10 : Operator
			content : SourceConversion(table:Buffer(default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]), fields:(count, word))
			ship_strategy : FORWARD

			Stage 12 : Operator
				content : SinkConversionToRow
				ship_strategy : FORWARD

				Stage 13 : Operator
					content : Map
					ship_strategy : FORWARD

					Stage 7 : Data Sink
						content : Sink: CsvTableSink(count, word)
						ship_strategy : FORWARD

						Stage 14 : Data Sink
							content : Sink: CsvTableSink(count, word)
							ship_strategy : FORWARD

{% endhighlight %}
</div>

{% top %}


