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

The Table API and SQL are integrated API and share many concepts and much of their API.

**TODO: Extend**

* This will be replaced by the TOC
{:toc}

Structure of Table API and SQL Programs
---------------------------------------

All Table API and SQL programs for batch and streaming have the same structure.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Create a TableEnvironment
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// Register a Table
tableEnv.registerTable("yourTable", ...)              // or
tableEnv.registerTableSource("yourTableSrc", ...);    // or
tableEnv.registerDataStream("yourTableStream", ...);  // or
tableEnv.registerDataSet("yourTableSet", ...);        // or 
tableEnv.registerExternalCatalog("yourCatalog", ...);

// Create a table from a Table API query
Table tapiResult = tableEnv.scan("yourTableSrc").select(...);
// Or create a table from a SQL query
Table sqlResult  = tableEnv.sql("SELECT ... FROM yourTableSrc ... ");

// Emit a Table to a TableSink / DataStream / DataSet
resultTable.writeToSink(...);     // or
resultTable.toAppendStream(...);  // or
resultTable.toRetractStream(...); // or
resultTable.toDataSet(...);

// Execute
env.execute("Your Query");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

// Create a TableEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// Register a Table
tableEnv.registerTable("yourTable", ...)             // or
tableEnv.registerTableSource("yourTableSrc", ...)    // or
tableEnv.registerDataStream("yourTableStream", ...)  // or
tableEnv.registerDataSet("yourTableSet", ...)        // or
tableEnv.registerExternalCatalog("yourCatalog", ...)

// Create a table from a Table API query
val tapiResult = tableEnv.scan("yourTableSrc").select(...)
// Or create a table from a SQL query
val sqlResult  = tableEnv.sql("SELECT ... FROM yourTableSrc ...")

// Emit a Table
resultTable.writeToSink(...)     // or
resultTable.toAppendStream(...)  // or
resultTable.toRetractStream(...) // or
resultTable.toDataSet(...)

// Execute
env.execute("Your Query")

{% endhighlight %}
</div>
</div>

{% top %}

Create a TableEnvironment
-------------------------

A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine Tables of different TableEnvironments.

**TODO: Extend**

{% top %}

Register a Table in the Catalog
-------------------------------

`TableEnvironment`s have an internal table catalog to which tables can be registered with a unique name. After registration, a table can be accessed from the `TableEnvironment` by its name.

*Note: `DataSet`s or `DataStream`s can be directly converted into `Table`s without registering them in the `TableEnvironment`. See [Create a Table from a DataStream or DataSet](#tbd) for details.

### Register a Table

A `Table` that originates from a Table API operation or a SQL query is registered in a `TableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// works for StreamExecutionEnvironment identically
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// convert a DataSet into a Table
Table custT = tableEnv
  .toTable(custDs, "name, zipcode")
  .where("zipcode = '12345'")
  .select("name");

// register the Table custT as table "custNames"
tableEnv.registerTable("custNames", custT);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// works for StreamExecutionEnvironment identically
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// convert a DataSet into a Table
val custT = custDs
  .toTable(tableEnv, 'name, 'zipcode)
  .where('zipcode === "12345")
  .select('name)

// register the Table custT as table "custNames"
tableEnv.registerTable("custNames", custT)
{% endhighlight %}
</div>
</div>

A registered `Table` that originates from a Table API operation or SQL query is treated similarly as a view as known from relational DBMS, i.e., it can be inlined when optimizing the query.

{% top %}

### Register a DataSet

A `DataSet` is registered as a `Table` in a `BatchTableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the DataSet cust as table "Customers" with fields derived from the dataset
tableEnv.registerDataSet("Customers", cust);

// register the DataSet ord as table "Orders" with fields user, product, and amount
tableEnv.registerDataSet("Orders", ord, "user, product, amount");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// register the DataSet cust as table "Customers" with fields derived from the dataset
tableEnv.registerDataSet("Customers", cust)

// register the DataSet ord as table "Orders" with fields user, product, and amount
tableEnv.registerDataSet("Orders", ord, 'user, 'product, 'amount)
{% endhighlight %}
</div>
</div>

*Note: The name of a `DataSet` `Table` must not match the `^_DataSetTable_[0-9]+` pattern which is reserved for internal use only.*

{% top %}

### Register a DataStream

A `DataStream` is registered as a `Table` in a `StreamTableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the DataStream cust as table "Customers" with fields derived from the datastream
tableEnv.registerDataStream("Customers", cust);

// register the DataStream ord as table "Orders" with fields user, product, and amount
tableEnv.registerDataStream("Orders", ord, "user, product, amount");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// register the DataStream cust as table "Customers" with fields derived from the datastream
tableEnv.registerDataStream("Customers", cust)

// register the DataStream ord as table "Orders" with fields user, product, and amount
tableEnv.registerDataStream("Orders", ord, 'user, 'product, 'amount)
{% endhighlight %}
</div>
</div>

*Note: The name of a `DataStream` `Table` must not match the `^_DataStreamTable_[0-9]+` pattern which is reserved for internal use only.*

{% top %}

### Register a TableSource

TableSources provided access to data stored in various storage systems such as databases (MySQL, HBase, ...), file formats (CSV, Apache Parquet, Avro, ORC, ...), or messaging systems (Apache Kafka, RabbitMQ, ...). Flink provides a TableSources for common data formats and storage systems. Please have a look at the [Table Sources and Sinks page]({{ site.baseurl }}/dev/table/sourceSinks.html) for a list of provided TableSources and documentation for how to built your own.

An external table is registered in a `TableEnvironment` using a `TableSource` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// works for StreamExecutionEnvironment identically
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

TableSource custTS = new CsvTableSource("/path/to/file", ...);

// register a `TableSource` as external table "Customers"
tableEnv.registerTableSource("Customers", custTS);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// works for StreamExecutionEnvironment identically
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

val custTS: TableSource = new CsvTableSource("/path/to/file", ...)

// register a `TableSource` as external table "Customers"
tableEnv.registerTableSource("Customers", custTS)

{% endhighlight %}
</div>
</div>

A `TableSource` can provide access to data stored in various storage systems such as databases (MySQL, HBase, ...), file formats (CSV, Apache Parquet, Avro, ORC, ...), or messaging systems (Apache Kafka, RabbitMQ, ...).

{% top %}

Register an External Catalog
----------------------------

An external catalog is defined by the `ExternalCatalog` interface and provides information about databases and tables such as their name, schema, statistics, and access information. An `ExternalCatalog` is registered in a `TableEnvironment` as follows: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// works for StreamExecutionEnvironment identically
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

ExternalCatalog customerCatalog = new InMemoryExternalCatalog();

// register the ExternalCatalog customerCatalog
tableEnv.registerExternalCatalog("Customers", customerCatalog);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// works for StreamExecutionEnvironment identically
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

val customerCatalog: ExternalCatalog = new InMemoryExternalCatalog

// register the ExternalCatalog customerCatalog
tableEnv.registerExternalCatalog("Customers", customerCatalog)

{% endhighlight %}
</div>
</div>

Once registered in a `TableEnvironment`, all tables defined in a `ExternalCatalog` can be accessed from Table API or SQL queries by specifying their full path (`catalog`.`database`.`table`).

Currently, Flink provides an `InMemoryExternalCatalog` for demo and testing purposes. However, the `ExternalCatalog` interface can also be used to connect catalogs like HCatalog or Metastore to the Table API.

{% top %}

Create a Table from a DataStream or DataSet
-------------------------------------------

Besides registering a Table in a catalog, it is also possible to directly create a `Table` from a `DataStream` or `DataSet`. 

### Create a Table from a DataStream

**TODO**

{% top %}

### Create a Table from a DataSet

**TODO**

### Scala Implicit Conversion

If you use the Scala API, A `DataSet` or `DataStream` can be implicitly converted into a `Table`.

{% top %}

Query a Table 
-------------

### Table API

**TODO**

{% top %}

### SQL

**TODO**

{% top %}

### Interoperability

**TODO**

* Mix SQL and Table as you like
* Table API to SQL requires registered tables, register Table
* SQL to Table API just use resulting table

{% top %}

Emit a Table 
------------

### Emit to a TableSink

A `Table` can be written to a `TableSink`, which is a generic interface to support a wide variety of file formats (e.g. CSV, Apache Parquet, Apache Avro), storage systems (e.g., JDBC, Apache HBase, Apache Cassandra, Elasticsearch), or messaging systems (e.g., Apache Kafka, RabbitMQ). A batch `Table` can only be written to a `BatchTableSink`, a streaming table requires a `StreamTableSink`. A `TableSink` can implement both interfaces at the same time.

Currently, Flink only provides a `CsvTableSink` that writes a batch or streaming `Table` to CSV-formatted files. A custom `TableSink` can be defined by implementing the `BatchTableSink` and/or `StreamTableSink` interface.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// compute the result Table using Table API operators and/or SQL queries
Table result = ...

// create a TableSink
TableSink sink = new CsvTableSink("/path/to/file", fieldDelim = "|");
// write the result Table to the TableSink
result.writeToSink(sink);

// execute the program
env.execute();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// compute the result Table using Table API operators and/or SQL queries
val result: Table = ...

// create a TableSink
val sink: TableSink = new CsvTableSink("/path/to/file", fieldDelim = "|")
// write the result Table to the TableSink
result.writeToSink(sink)

// execute the program
env.execute()
{% endhighlight %}
</div>
</div>

{% top %}

### Convert to a DataStream

**TODO**

{% top %}

### Convert to a DataSet

**TODO**

### Scala Implicit Conversion

If you use the Scala API, A `Table` can be implicitly converted into a `DataSet` or `DataStream`.

{% top %}

Execute a Query
---------------

**TODO**

{% top %}

Mappings Types to Table Schema
------------------------------

* Explain how types are mapped to table schema
  * Atomic Types
  * Row
  * Tuples (Java / Scala)
  * Pojos
  * Case Classes

**TODO**

{% top %}

Integration with DataSet and DataStream API
-------------------------------------------

**TODO**

* Create `Table` from `DataSet` and `DataStream` and back
* Easy integration with more expressive APIs and libraries
  * CEP / Gelly / ML
  * Ingestion and projection

{% top %}

Query Optimization
------------------

* No join order yet
* Filter / Projection push down
* Custom rules

### Explaining a Table

The Table API provides a mechanism to explain the logical and optimized query plans to compute a `Table`. 
This is done through the `TableEnvironment#explain(table)` method. It returns a string describing three plans: 

1. the Abstract Syntax Tree of the relational query, i.e., the unoptimized logical query plan,
2. the optimized logical query plan, and
3. the physical execution plan.

The following code shows an example and the corresponding output:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

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
val tEnv = TableEnvironment.getTableEnvironment(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table = table1
      .where('word.like("F%"))
      .unionAll(table2)

val explanation: String = tEnv.explain(table)
println(explanation)
{% endhighlight %}
</div>
</div>

{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, 'F%')])
    LogicalTableScan(table=[[_DataStreamTable_0]])
  LogicalTableScan(table=[[_DataStreamTable_1]])

== Optimized Logical Plan ==
DataStreamUnion(union=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, 'F%')])
    DataStreamScan(table=[[_DataStreamTable_0]])
  DataStreamScan(table=[[_DataStreamTable_1]])

== Physical Execution Plan ==
Stage 1 : Data Source
  content : collect elements with CollectionInputFormat

Stage 2 : Data Source
  content : collect elements with CollectionInputFormat

  Stage 3 : Operator
    content : from: (count, word)
    ship_strategy : REBALANCE

    Stage 4 : Operator
      content : where: (LIKE(word, 'F%')), select: (count, word)
      ship_strategy : FORWARD

      Stage 5 : Operator
        content : from: (count, word)
        ship_strategy : REBALANCE
{% endhighlight %}

{% top %}


