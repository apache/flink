---
title: "概念与通用 API"
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

The Table API and SQL are integrated in a joint API. The central concept of this API is a `Table` which serves as input and output of queries. This document shows the common structure of programs with Table API and SQL queries, how to register a `Table`, how to query a `Table`, and how to emit a `Table`.

* This will be replaced by the TOC
{:toc}

Structure of Table API and SQL Programs
---------------------------------------

All Table API and SQL programs for batch and streaming follow the same pattern. The following code example shows the common structure of Table API and SQL programs.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// create a TableEnvironment
// for batch programs use BatchTableEnvironment instead of StreamTableEnvironment
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// register a Table
tableEnv.registerTable("table1", ...)            // or
tableEnv.registerTableSource("table2", ...);     // or
tableEnv.registerExternalCatalog("extCat", ...);
// register an output Table
tableEnv.registerTableSink("outputTable", ...);

// create a Table from a Table API query
Table tapiResult = tableEnv.scan("table1").select(...);
// create a Table from a SQL query
Table sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");

// emit a Table API result Table to a TableSink, same for SQL result
tapiResult.insertInto("outputTable");

// execute
env.execute();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment

// create a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// register a Table
tableEnv.registerTable("table1", ...)           // or
tableEnv.registerTableSource("table2", ...)     // or
tableEnv.registerExternalCatalog("extCat", ...)
// register an output Table
tableEnv.registerTableSink("outputTable", ...);

// create a Table from a Table API query
val tapiResult = tableEnv.scan("table1").select(...)
// create a Table from a SQL query
val sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ...")

// emit a Table API result Table to a TableSink, same for SQL result
tapiResult.insertInto("outputTable")

// execute
env.execute()

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# create a TableEnvironment
table_env = StreamTableEnvironment.create(env)

# register a Table
table_env.register_table("table1", ...)           # or
table_env.register_table_source("table2", ...)

# register an output Table
table_env.register_table_sink("outputTable", ...);

# create a Table from a Table API query
tapi_result = table_env.scan("table1").select(...)
# create a Table from a SQL query
sql_result  = table_env.sql_query("SELECT ... FROM table2 ...")

# emit a Table API result Table to a TableSink, same for SQL result
tapi_result.insert_into("outputTable")

# execute
table_env.execute("python_job")

{% endhighlight %}
</div>
</div>

**Note:** Table API and SQL queries can be easily integrated with and embedded into DataStream or DataSet programs. Have a look at the [Integration with DataStream and DataSet API](#integration-with-datastream-and-dataset-api) section to learn how DataStreams and DataSets can be converted into Tables and vice versa.

{% top %}

Create a TableEnvironment
-------------------------

The `TableEnvironment` is a central concept of the Table API and SQL integration. It is responsible for:

* Registering a `Table` in the internal catalog
* Registering an external catalog 
* Executing SQL queries
* Registering a user-defined (scalar, table, or aggregation) function
* Converting a `DataStream` or `DataSet` into a `Table`
* Holding a reference to an `ExecutionEnvironment` or `StreamExecutionEnvironment`

A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine tables of different TableEnvironments in the same query, e.g., to join or union them.

A `TableEnvironment` is created by calling the static `BatchTableEnvironment.create()` or `StreamTableEnvironment.create()` method with a `StreamExecutionEnvironment` or an `ExecutionEnvironment` and an optional `TableConfig`. The `TableConfig` can be used to configure the `TableEnvironment` or to customize the query optimization and translation process (see [Query Optimization](#query-optimization)).

Make sure to choose the `BatchTableEnvironment`/`StreamTableEnvironment` that matches your programming language.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// ***************
// STREAMING QUERY
// ***************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
// create a TableEnvironment for streaming queries
StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sEnv);

// ***********
// BATCH QUERY
// ***********
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.api.java.BatchTableEnvironment;

ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
// create a TableEnvironment for batch queries
BatchTableEnvironment bTableEnv = BatchTableEnvironment.create(bEnv);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// ***************
// STREAMING QUERY
// ***************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
// create a TableEnvironment for streaming queries
val sTableEnv = StreamTableEnvironment.create(sEnv)

// ***********
// BATCH QUERY
// ***********
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

val bEnv = ExecutionEnvironment.getExecutionEnvironment
// create a TableEnvironment for batch queries
val bTableEnv = BatchTableEnvironment.create(bEnv)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# ***************
# STREAMING QUERY
# ***************
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
# create a TableEnvironment for streaming queries
s_table_env = StreamTableEnvironment.create(s_env)

# ***********
# BATCH QUERY
# ***********
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment

b_env = ExecutionEnvironment.get_execution_environment()
# create a TableEnvironment for batch queries
b_table_env = BatchTableEnvironment.create(b_env)
{% endhighlight %}
</div>
</div>

{% top %}

Register Tables in the Catalog
-------------------------------

A `TableEnvironment` maintains a catalog of tables which are registered by name. There are two types of tables, *input tables* and *output tables*. Input tables can be referenced in Table API and SQL queries and provide input data. Output tables can be used to emit the result of a Table API or SQL query to an external system.

An input table can be registered from various sources:

* an existing `Table` object, usually the result of a Table API or SQL query.
* a `TableSource`, which accesses external data, such as a file, database, or messaging system. 
* a `DataStream` or `DataSet` from a DataStream or DataSet program. Registering a `DataStream` or `DataSet` is discussed in the [Integration with DataStream and DataSet API](#integration-with-datastream-and-dataset-api) section.

An output table can be registered using a `TableSink`.

### Register a Table

A `Table` is registered in a `TableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Table is the result of a simple projection query 
Table projTable = tableEnv.scan("X").select(...);

// register the Table projTable as table "projectedTable"
tableEnv.registerTable("projectedTable", projTable);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// Table is the result of a simple projection query 
val projTable: Table = tableEnv.scan("X").select(...)

// register the Table projTable as table "projectedTable"
tableEnv.registerTable("projectedTable", projTable)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = StreamTableEnvironment.create(env)

# Table is the result of a simple projection query 
proj_table = table_env.scan("X").select(...)

# register the Table projTable as table "projectedTable"
table_env.register_table("projectedTable", proj_table)
{% endhighlight %}
</div>
</div>

**Note:** A registered `Table` is treated similarly to a `VIEW` as known from relational database systems, i.e., the query that defines the `Table` is not optimized but will be inlined when another query references the registered `Table`. If multiple queries reference the same registered `Table`, it will be inlined for each referencing query and executed multiple times, i.e., the result of the registered `Table` will *not* be shared.

{% top %}

### Register a TableSource

A `TableSource` provides access to external data which is stored in a storage system such as a database (MySQL, HBase, ...), a file with a specific encoding (CSV, Apache \[Parquet, Avro, ORC\], ...), or a messaging system (Apache Kafka, RabbitMQ, ...). 

Flink aims to provide TableSources for common data formats and storage systems. Please have a look at the [Table Sources and Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) page for a list of supported TableSources and instructions for how to build a custom `TableSource`.

A `TableSource` is registered in a `TableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a TableSource
TableSource csvSource = new CsvTableSource("/path/to/file", ...);

// register the TableSource as table "CsvTable"
tableEnv.registerTableSource("CsvTable", csvSource);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a TableSource
val csvSource: TableSource = new CsvTableSource("/path/to/file", ...)

// register the TableSource as table "CsvTable"
tableEnv.registerTableSource("CsvTable", csvSource)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = StreamTableEnvironment.create(env)

# create a TableSource
csv_source = CsvTableSource("/path/to/file", ...)

# register the TableSource as table "csvTable"
table_env.register_table_source("csvTable", csv_source)
{% endhighlight %}
</div>
</div>

{% top %}

### Register a TableSink

A registered `TableSink` can be used to [emit the result of a Table API or SQL query](common.html#emit-a-table) to an external storage system, such as a database, key-value store, message queue, or file system (in different encodings, e.g., CSV, Apache \[Parquet, Avro, ORC\], ...).

Flink aims to provide TableSinks for common data formats and storage systems. Please see the documentation about [Table Sources and Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) page for details about available sinks and instructions for how to implement a custom `TableSink`.

A `TableSink` is registered in a `TableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a TableSink
TableSink csvSink = new CsvTableSink("/path/to/file", ...);

// define the field names and types
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};

// register the TableSink as table "CsvSinkTable"
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a TableSink
val csvSink: TableSink = new CsvTableSink("/path/to/file", ...)

// define the field names and types
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.LONG)

// register the TableSink as table "CsvSinkTable"
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = StreamTableEnvironment.create(env)

# define the field names and types
field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# create a TableSink
csv_sink = CsvTableSink(field_names, field_types, "/path/to/file", ...)

# register the TableSink as table "CsvSinkTable"
table_env.register_table_sink("CsvSinkTable", csv_sink)
{% endhighlight %}
</div>
</div>

{% top %}

Register an External Catalog
----------------------------

An external catalog can provide information about external databases and tables such as their name, schema, statistics, and information for how to access data stored in an external database, table, or file.

An external catalog can be created by implementing the `ExternalCatalog` interface and is registered in a `TableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create an external catalog
ExternalCatalog catalog = new InMemoryExternalCatalog();

// register the ExternalCatalog catalog
tableEnv.registerExternalCatalog("InMemCatalog", catalog);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create an external catalog
val catalog: ExternalCatalog = new InMemoryExternalCatalog

// register the ExternalCatalog catalog
tableEnv.registerExternalCatalog("InMemCatalog", catalog)
{% endhighlight %}
</div>
</div>

Once registered in a `TableEnvironment`, all tables defined in a `ExternalCatalog` can be accessed from Table API or SQL queries by specifying their full path, such as `catalog.database.table`.

Currently, Flink provides an `InMemoryExternalCatalog` for demo and testing purposes. However, the `ExternalCatalog` interface can also be used to connect catalogs like HCatalog or Metastore to the Table API.

{% top %}

Query a Table 
-------------

### Table API

The Table API is a language-integrated query API for Scala and Java. In contrast to SQL, queries are not specified as Strings but are composed step-by-step in the host language. 

The API is based on the `Table` class which represents a table (streaming or batch) and offers methods to apply relational operations. These methods return a new `Table` object, which represents the result of applying the relational operation on the input `Table`. Some relational operations are composed of multiple method calls such as `table.groupBy(...).select()`, where `groupBy(...)` specifies a grouping of `table`, and `select(...)` the projection on the grouping of `table`.

The [Table API]({{ site.baseurl }}/dev/table/tableApi.html) document describes all Table API operations that are supported on streaming and batch tables.

The following example shows a simple Table API aggregation query:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// register Orders table

// scan registered Orders table
Table orders = tableEnv.scan("Orders");
// compute revenue for all customers from France
Table revenue = orders
  .filter("cCountry === 'FRANCE'")
  .groupBy("cID, cName")
  .select("cID, cName, revenue.sum AS revSum");

// emit or convert Table
// execute query
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// register Orders table

// scan registered Orders table
val orders = tableEnv.scan("Orders")
// compute revenue for all customers from France
val revenue = orders
  .filter('cCountry === "FRANCE")
  .groupBy('cID, 'cName)
  .select('cID, 'cName, 'revenue.sum AS 'revSum)

// emit or convert Table
// execute query
{% endhighlight %}

**Note:** The Scala Table API uses Scala Symbols, which start with a single tick (`'`) to reference the attributes of a `Table`. The Table API uses Scala implicits. Make sure to import `org.apache.flink.api.scala._` and `org.apache.flink.table.api.scala._` in order to use Scala implicit conversions.
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
table_env = StreamTableEnvironment.create(env)

# register Orders table

# scan registered Orders table
orders = table_env.scan("Orders")
# compute revenue for all customers from France
revenue = orders \
    .filter("cCountry === 'FRANCE'") \
    .group_by("cID, cName") \
    .select("cID, cName, revenue.sum AS revSum")

# emit or convert Table
# execute query
{% endhighlight %}
</div>
</div>

{% top %}

### SQL

Flink's SQL integration is based on [Apache Calcite](https://calcite.apache.org), which implements the SQL standard. SQL queries are specified as regular Strings.

The [SQL]({{ site.baseurl }}/dev/table/sql.html) document describes Flink's SQL support for streaming and batch tables.

The following example shows how to specify a query and return the result as a `Table`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
val tableEnv = StreamTableEnvironment.create(env)

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
# get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
table_env = StreamTableEnvironment.create(env)

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
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// register "Orders" table
// register "RevenueFrance" output table

// compute revenue for all customers from France and emit to "RevenueFrance"
tableEnv.sqlUpdate(
    "INSERT INTO RevenueFrance " +
    "SELECT cID, cName, SUM(revenue) AS revSum " +
    "FROM Orders " +
    "WHERE cCountry = 'FRANCE' " +
    "GROUP BY cID, cName"
  );

// execute query
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// register "Orders" table
// register "RevenueFrance" output table

// compute revenue for all customers from France and emit to "RevenueFrance"
tableEnv.sqlUpdate("""
  |INSERT INTO RevenueFrance
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

// execute query
{% endhighlight %}

</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
table_env = StreamTableEnvironment.create(env)

# register "Orders" table
# register "RevenueFrance" output table

# compute revenue for all customers from France and emit to "RevenueFrance"
table_env.sql_update(
    "INSERT INTO RevenueFrance "
    "SELECT cID, cName, SUM(revenue) AS revSum "
    "FROM Orders "
    "WHERE cCountry = 'FRANCE' "
    "GROUP BY cID, cName"
)

# execute query
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

Please see the documentation about [Table Sources & Sinks]({{ site.baseurl }}/dev/table/sourceSinks.html) for details about available sinks and instructions for how to implement a custom `TableSink`.

The `Table.insertInto(String tableName)` method emits the `Table` to a registered `TableSink`. The method looks up the `TableSink` from the catalog by the name and validates that the schema of the `Table` is identical to the schema of the `TableSink`. 

The following examples shows how to emit a `Table`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a TableSink
TableSink sink = new CsvTableSink("/path/to/file", fieldDelim = "|");

// register the TableSink with a specific schema
String[] fieldNames = {"a", "b", "c"};
TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);

// compute a result Table using Table API operators and/or SQL queries
Table result = ...
// emit the result Table to the registered TableSink
result.insertInto("CsvSinkTable");

// execute the program
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a TableSink
val sink: TableSink = new CsvTableSink("/path/to/file", fieldDelim = "|")

// register the TableSink with a specific schema
val fieldNames: Array[String] = Array("a", "b", "c")
val fieldTypes: Array[TypeInformation] = Array(Types.INT, Types.STRING, Types.LONG)
tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink)

// compute a result Table using Table API operators and/or SQL queries
val result: Table = ...

// emit the result Table to the registered TableSink
result.insertInto("CsvSinkTable")

// execute the program
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# get a TableEnvironment
table_env = StreamTableEnvironment.create(env)

field_names = ["a", "b", "c"]
field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()]

# create a TableSink
sink = CsvTableSink(field_names, field_types, "/path/to/file", "|")

table_env.register_table_sink("CsvSinkTable", sink)

# compute a result Table using Table API operators and/or SQL queries
result = ...

# emit the result Table to the registered TableSink
result.insert_into("CsvSinkTable")

# execute the program
{% endhighlight %}
</div>
</div>

{% top %}


Translate and Execute a Query
-----------------------------

Table API and SQL queries are translated into [DataStream]({{ site.baseurl }}/dev/datastream_api.html) or [DataSet]({{ site.baseurl }}/dev/batch) programs depending on whether their input is a streaming or batch input. A query is internally represented as a logical query plan and is translated in two phases: 

1. optimization of the logical plan, 
2. translation into a DataStream or DataSet program.

A Table API or SQL query is translated when:

* a `Table` is emitted to a `TableSink`, i.e., when `Table.insertInto()` is called.
* a SQL update query is specified, i.e., when `TableEnvironment.sqlUpdate()` is called.
* a `Table` is converted into a `DataStream` or `DataSet` (see [Integration with DataStream and DataSet API](#integration-with-datastream-and-dataset-api)).

Once translated, a Table API or SQL query is handled like a regular DataStream or DataSet program and is executed when `StreamExecutionEnvironment.execute()` or `ExecutionEnvironment.execute()` is called.

{% top %}

Integration with DataStream and DataSet API
-------------------------------------------

Table API and SQL queries can be easily integrated with and embedded into [DataStream]({{ site.baseurl }}/dev/datastream_api.html) and [DataSet]({{ site.baseurl }}/dev/batch) programs. For instance, it is possible to query an external table (for example from a RDBMS), do some pre-processing, such as filtering, projecting, aggregating, or joining with meta data, and then further process the data with either the DataStream or DataSet API (and any of the libraries built on top of these APIs, such as CEP or Gelly). Inversely, a Table API or SQL query can also be applied on the result of a DataStream or DataSet program.

This interaction can be achieved by converting a `DataStream` or `DataSet` into a `Table` and vice versa. In this section, we describe how these conversions are done.

### Implicit Conversion for Scala

The Scala Table API features implicit conversions for the `DataSet`, `DataStream`, and `Table` classes. These conversions are enabled by importing the package `org.apache.flink.table.api.scala._` in addition to `org.apache.flink.api.scala._` for the Scala DataStream API.

### Register a DataStream or DataSet as Table

A `DataStream` or `DataSet` can be registered in a `TableEnvironment` as a Table. The schema of the resulting table depends on the data type of the registered `DataStream` or `DataSet`. Please check the section about [mapping of data types to table schema](#mapping-of-data-types-to-table-schema) for details.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get StreamTableEnvironment
// registration of a DataSet in a BatchTableEnvironment is equivalent
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Long, String>> stream = ...

// register the DataStream as Table "myTable" with fields "f0", "f1"
tableEnv.registerDataStream("myTable", stream);

// register the DataStream as table "myTable2" with fields "myLong", "myString"
tableEnv.registerDataStream("myTable2", stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get TableEnvironment 
// registration of a DataSet is equivalent
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[(Long, String)] = ...

// register the DataStream as Table "myTable" with fields "f0", "f1"
tableEnv.registerDataStream("myTable", stream)

// register the DataStream as table "myTable2" with fields "myLong", "myString"
tableEnv.registerDataStream("myTable2", stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

**Note:** The name of a `DataStream` `Table` must not match the `^_DataStreamTable_[0-9]+` pattern and the name of a `DataSet` `Table` must not match the `^_DataSetTable_[0-9]+` pattern. These patterns are reserved for internal use only.

{% top %}

### Convert a DataStream or DataSet into a Table

Instead of registering a `DataStream` or `DataSet` in a `TableEnvironment`, it can also be directly converted into a `Table`. This is convenient if you want to use the Table in a Table API query. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get StreamTableEnvironment
// registration of a DataSet in a BatchTableEnvironment is equivalent
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Long, String>> stream = ...

// Convert the DataStream into a Table with default fields "f0", "f1"
Table table1 = tableEnv.fromDataStream(stream);

// Convert the DataStream into a Table with fields "myLong", "myString"
Table table2 = tableEnv.fromDataStream(stream, "myLong, myString");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get TableEnvironment
// registration of a DataSet is equivalent
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[(Long, String)] = ...

// convert the DataStream into a Table with default fields '_1, '_2
val table1: Table = tableEnv.fromDataStream(stream)

// convert the DataStream into a Table with fields 'myLong, 'myString
val table2: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)
{% endhighlight %}
</div>
</div>

{% top %}

### Convert a Table into a DataStream or DataSet

A `Table` can be converted into a `DataStream` or `DataSet`. In this way, custom DataStream or DataSet programs can be run on the result of a Table API or SQL query.

When converting a `Table` into a `DataStream` or `DataSet`, you need to specify the data type of the resulting `DataStream` or `DataSet`, i.e., the data type into which the rows of the `Table` are to be converted. Often the most convenient conversion type is `Row`. The following list gives an overview of the features of the different options:

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
// get StreamTableEnvironment. 
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Table with two fields (String name, Integer age)
Table table = ...

// convert the Table into an append DataStream of Row by specifying the class
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

// convert the Table into an append DataStream of Tuple2<String, Integer> 
//   via a TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple = 
  tableEnv.toAppendStream(table, tupleType);

// convert the Table into a retract DataStream of Row.
//   A retract stream of type X is a DataStream<Tuple2<Boolean, X>>. 
//   The boolean field indicates the type of the change. 
//   True is INSERT, false is DELETE.
DataStream<Tuple2<Boolean, Row>> retractStream = 
  tableEnv.toRetractStream(table, Row.class);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get TableEnvironment. 
// registration of a DataSet is equivalent
val tableEnv = StreamTableEnvironment.create(env)

// Table with two fields (String name, Integer age)
val table: Table = ...

// convert the Table into an append DataStream of Row
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// convert the Table into an append DataStream of Tuple2[String, Int]
val dsTuple: DataStream[(String, Int)] dsTuple = 
  tableEnv.toAppendStream[(String, Int)](table)

// convert the Table into a retract DataStream of Row.
//   A retract stream of type X is a DataStream[(Boolean, X)]. 
//   The boolean field indicates the type of the change. 
//   True is INSERT, false is DELETE.
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
{% endhighlight %}
</div>
</div>

**Note:** A detailed discussion about dynamic tables and their properties is given in the [Dynamic Tables](streaming/dynamic_tables.html) document.

#### Convert a Table into a DataSet

A `Table` is converted into a `DataSet` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get BatchTableEnvironment
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

// Table with two fields (String name, Integer age)
Table table = ...

// convert the Table into a DataSet of Row by specifying a class
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

// convert the Table into a DataSet of Tuple2<String, Integer> via a TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
  Types.STRING(),
  Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple = 
  tableEnv.toDataSet(table, tupleType);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get TableEnvironment 
// registration of a DataSet is equivalent
val tableEnv = BatchTableEnvironment.create(env)

// Table with two fields (String name, Integer age)
val table: Table = ...

// convert the Table into a DataSet of Row
val dsRow: DataSet[Row] = tableEnv.toDataSet[Row](table)

// convert the Table into a DataSet of Tuple2[String, Int]
val dsTuple: DataSet[(String, Int)] = tableEnv.toDataSet[(String, Int)](table)
{% endhighlight %}
</div>
</div>

{% top %}

### Mapping of Data Types to Table Schema

Flink's DataStream and DataSet APIs support very diverse types. Composite types such as Tuples (built-in Scala and Flink Java tuples), POJOs, Scala case classes, and Flink's Row type allow for nested data structures with multiple fields that can be accessed in table expressions. Other types are treated as atomic types. In the following, we describe how the Table API converts these types into an internal row representation and show examples of converting a `DataStream` into a `Table`.

The mapping of a data type to a table schema can happen in two ways: **based on the field positions** or **based on the field names**.

**Position-based Mapping**

Position-based mapping can be used to give fields a more meaningful name while keeping the field order. This mapping is available for composite data types *with a defined field order* as well as atomic types. Composite data types such as tuples, rows, and case classes have such a field order. However, fields of a POJO must be mapped based on the field names (see next section). Fields can be projected out but can't be renamed using an alias `as`.

When defining a position-based mapping, the specified names must not exist in the input data type, otherwise the API will assume that the mapping should happen based on the field names. If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with default field names "f0" and "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with field "myLong" only
Table table = tableEnv.fromDataStream(stream, "myLong");

// convert DataStream into Table with field names "myLong" and "myInt"
Table table = tableEnv.fromDataStream(stream, "myLong, myInt");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with default field names "_1" and "_2"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field "myLong" only
val table: Table = tableEnv.fromDataStream(stream, 'myLong)

// convert DataStream into Table with field names "myLong" and "myInt"
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myInt)
{% endhighlight %}
</div>
</div>

**Name-based Mapping**

Name-based mapping can be used for any data type including POJOs. It is the most flexible way of defining a table schema mapping. All fields in the mapping are referenced by name and can be possibly renamed using an alias `as`. Fields can be reordered and projected out.

If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with default field names "f0" and "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with field "f1" only
Table table = tableEnv.fromDataStream(stream, "f1");

// convert DataStream into Table with swapped fields
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
Table table = tableEnv.fromDataStream(stream, "f1 as myInt, f0 as myLong");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with default field names "_1" and "_2"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field "_2" only
val table: Table = tableEnv.fromDataStream(stream, '_2)

// convert DataStream into Table with swapped fields
val table: Table = tableEnv.fromDataStream(stream, '_2, '_1)

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
val table: Table = tableEnv.fromDataStream(stream, '_2 as 'myInt, '_1 as 'myLong)
{% endhighlight %}
</div>
</div>

#### Atomic Types

Flink treats primitives (`Integer`, `Double`, `String`) or generic types (types that cannot be analyzed and decomposed) as atomic types. A `DataStream` or `DataSet` of an atomic type is converted into a `Table` with a single attribute. The type of the attribute is inferred from the atomic type and the name of the attribute can be specified.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Long> stream = ...

// convert DataStream into Table with default field name "f0"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with field name "myLong"
Table table = tableEnv.fromDataStream(stream, "myLong");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[Long] = ...

// convert DataStream into Table with default field name "f0"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field name "myLong"
val table: Table = tableEnv.fromDataStream(stream, 'myLong)
{% endhighlight %}
</div>
</div>

#### Tuples (Scala and Java) and Case Classes (Scala only)

Flink supports Scala's built-in tuples and provides its own tuple classes for Java. DataStreams and DataSets of both kinds of tuples can be converted into tables. Fields can be renamed by providing names for all fields (mapping based on position). If no field names are specified, the default field names are used. If the original field names (`f0`, `f1`, ... for Flink Tuples and `_1`, `_2`, ... for Scala Tuples) are referenced, the API assumes that the mapping is name-based instead of position-based. Name-based mapping allows for reordering fields and projection with alias (`as`).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Long, String>> stream = ...

// convert DataStream into Table with default field names "f0", "f1"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with renamed field names "myLong", "myString" (position-based)
Table table = tableEnv.fromDataStream(stream, "myLong, myString");

// convert DataStream into Table with reordered fields "f1", "f0" (name-based)
Table table = tableEnv.fromDataStream(stream, "f1, f0");

// convert DataStream into Table with projected field "f1" (name-based)
Table table = tableEnv.fromDataStream(stream, "f1");

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
Table table = tableEnv.fromDataStream(stream, "f1 as 'myString', f0 as 'myLong'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val stream: DataStream[(Long, String)] = ...

// convert DataStream into Table with renamed default field names '_1, '_2
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with field names "myLong", "myString" (position-based)
val table: Table = tableEnv.fromDataStream(stream, 'myLong, 'myString)

// convert DataStream into Table with reordered fields "_2", "_1" (name-based)
val table: Table = tableEnv.fromDataStream(stream, '_2, '_1)

// convert DataStream into Table with projected field "_2" (name-based)
val table: Table = tableEnv.fromDataStream(stream, '_2)

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
val table: Table = tableEnv.fromDataStream(stream, '_2 as 'myString, '_1 as 'myLong)

// define case class
case class Person(name: String, age: Int)
val streamCC: DataStream[Person] = ...

// convert DataStream into Table with default field names 'name, 'age
val table = tableEnv.fromDataStream(streamCC)

// convert DataStream into Table with field names 'myName, 'myAge (position-based)
val table = tableEnv.fromDataStream(streamCC, 'myName, 'myAge)

// convert DataStream into Table with reordered and aliased fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

{% endhighlight %}
</div>
</div>

#### POJO (Java and Scala)

Flink supports POJOs as composite types. The rules for what determines a POJO are documented [here]({{ site.baseurl }}/dev/api_concepts.html#pojos).

When converting a POJO `DataStream` or `DataSet` into a `Table` without specifying field names, the names of the original POJO fields are used. The name mapping requires the original names and cannot be done by positions. Fields can be renamed using an alias (with the `as` keyword), reordered, and projected.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Person is a POJO with fields "name" and "age"
DataStream<Person> stream = ...

// convert DataStream into Table with default field names "age", "name" (fields are ordered by name!)
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, "age as myAge, name as myName");

// convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, "name");

// convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// Person is a POJO with field names "name" and "age"
val stream: DataStream[Person] = ...

// convert DataStream into Table with default field names "age", "name" (fields are ordered by name!)
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'age as 'myAge, 'name as 'myName)

// convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'name)

// convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

#### Row

The `Row` data type supports an arbitrary number of fields and fields with `null` values. Field names can be specified via a `RowTypeInfo` or when converting a `Row` `DataStream` or `DataSet` into a `Table`. The row type supports mapping of fields by position and by name. Fields can be renamed by providing names for all fields (mapping based on position) or selected individually for projection/ordering/renaming (mapping based on name).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get a StreamTableEnvironment, works for BatchTableEnvironment equivalently
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
DataStream<Row> stream = ...

// convert DataStream into Table with default field names "name", "age"
Table table = tableEnv.fromDataStream(stream);

// convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
Table table = tableEnv.fromDataStream(stream, "myName, myAge");

// convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
Table table = tableEnv.fromDataStream(stream, "name as myName, age as myAge");

// convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, "name");

// convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, "name as myName");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// get a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
val stream: DataStream[Row] = ...

// convert DataStream into Table with default field names "name", "age"
val table: Table = tableEnv.fromDataStream(stream)

// convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
val table: Table = tableEnv.fromDataStream(stream, 'myName, 'myAge)

// convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName, 'age as 'myAge)

// convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'name)

// convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, 'name as 'myName)
{% endhighlight %}
</div>
</div>

{% top %}


Query Optimization
------------------

Apache Flink leverages Apache Calcite to optimize and translate queries. The optimization currently performed include projection and filter push-down, subquery decorrelation, and other kinds of query rewriting. Flink does not yet optimize the order of joins, but executes them in the same order as defined in the query (order of Tables in the `FROM` clause and/or order of join predicates in the `WHERE` clause).

It is possible to tweak the set of optimization rules which are applied in different phases by providing a `CalciteConfig` object. This can be created via a builder by calling `CalciteConfig.createBuilder())` and is provided to the TableEnvironment by calling `tableEnv.getConfig.setCalciteConfig(calciteConfig)`. 

### Explaining a Table

The Table API provides a mechanism to explain the logical and optimized query plans to compute a `Table`. 
This is done through the `TableEnvironment.explain(table)` method. It returns a String describing three plans: 

1. the Abstract Syntax Tree of the relational query, i.e., the unoptimized logical query plan,
2. the optimized logical query plan, and
3. the physical execution plan.

The following code shows an example and the corresponding output:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

Table table1 = tEnv.fromDataStream(stream1, "count, word");
Table table2 = tEnv.fromDataStream(stream2, "count, word");
Table table = table1
  .where("LIKE(word, 'F%')")
  .unionAll(table2);

String explanation = tEnv.explain(table);
System.out.println(explanation);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table = table1
  .where('word.like("F%"))
  .unionAll(table2)

val explanation: String = tEnv.explain(table)
println(explanation)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

table1 = t_env.from_elements([(1, "hello")], ["count", "word"])
table2 = t_env.from_elements([(1, "hello")], ["count", "word"])
table = table1 \
    .where("LIKE(word, 'F%')") \
    .union_all(table2)

explanation = t_env.explain(table)
print(explanation)
{% endhighlight %}
</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
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

<div data-lang="scala" markdown="1">
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

<div data-lang="python" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[3], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[6], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[3], fields=[count, word])
  DataStreamScan(id=[6], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 2 : Operator
		content : Flat Map
		ship_strategy : FORWARD

		Stage 3 : Operator
			content : Map
			ship_strategy : FORWARD

Stage 4 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 5 : Operator
		content : Flat Map
		ship_strategy : FORWARD

		Stage 6 : Operator
			content : Map
			ship_strategy : FORWARD

			Stage 7 : Operator
				content : Map
				ship_strategy : FORWARD

				Stage 8 : Operator
					content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
					ship_strategy : FORWARD

					Stage 9 : Operator
						content : Map
						ship_strategy : FORWARD
{% endhighlight %}
</div>
</div>

{% top %}


