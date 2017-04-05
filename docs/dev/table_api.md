---
title: "Table and SQL"
is_beta: true
nav-parent_id: libs
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

**Table API and SQL are experimental features**

The Table API is a SQL-like expression language for relational stream and batch processing that can be easily embedded in Flink's DataSet and DataStream APIs (Java and Scala).
The Table API and SQL interface operate on a relational `Table` abstraction, which can be created from external data sources, or existing DataSets and DataStreams. With the Table API, you can apply relational operators such as selection, aggregation, and joins on `Table`s.

`Table`s can also be queried with regular SQL, as long as they are registered (see [Registering Tables](#registering-tables)). The Table API and SQL offer equivalent functionality and can be mixed in the same program. When a `Table` is converted back into a `DataSet` or `DataStream`, the logical plan, which was defined by relational operators and SQL queries, is optimized using [Apache Calcite](https://calcite.apache.org/) and transformed into a `DataSet` or `DataStream` program.

* This will be replaced by the TOC
{:toc}

Using the Table API and SQL
----------------------------

The Table API and SQL are part of the *flink-table* Maven project.
The following dependency must be added to your project in order to use the Table API and SQL:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

*Note: The Table API is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/dev/linking.html).*


Registering Tables
--------------------------------

`TableEnvironment`s have an internal table catalog to which tables can be registered with a unique name. After registration, a table can be accessed from the `TableEnvironment` by its name.

*Note: `DataSet`s or `DataStream`s can be directly converted into `Table`s without registering them in the `TableEnvironment`.*

### Register a DataSet

A `DataSet` is registered as a `Table` in a `BatchTableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the DataSet cust as table "Customers" with fields derived from the dataset
tableEnv.registerDataSet("Customers", cust)

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

### Register a DataStream

A `DataStream` is registered as a `Table` in a `StreamTableEnvironment` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the DataStream cust as table "Customers" with fields derived from the datastream
tableEnv.registerDataStream("Customers", cust)

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
  .select("name")

// register the Table custT as table "custNames"
tableEnv.registerTable("custNames", custT)
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

### Register an external Table using a TableSource

An external table is registered in a `TableEnvironment` using a `TableSource` as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// works for StreamExecutionEnvironment identically
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

TableSource custTS = new CsvTableSource("/path/to/file", ...)

// register a `TableSource` as external table "Customers"
tableEnv.registerTableSource("Customers", custTS)
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

Currently, Flink provides the `CsvTableSource` to read CSV files and the `Kafka08JsonTableSource`/`Kafka09JsonTableSource` to read JSON objects from Kafka.
A custom `TableSource` can be defined by implementing the `BatchTableSource` or `StreamTableSource` interface.

### Available Table Sources

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSouce` | `flink-table` | Y | Y | A simple source for CSV files.
| `Kafka08JsonTableSource` | `flink-connector-kafka-0.8` | N | Y | A Kafka 0.8 source for JSON data.
| `Kafka09JsonTableSource` | `flink-connector-kafka-0.9` | N | Y | A Kafka 0.9 source for JSON data.

All sources that come with the `flink-table` dependency can be directly used by your Table programs. For all other table sources, you have to add the respective dependency in addition to the `flink-table` dependency.

#### KafkaJsonTableSource

To use the Kafka JSON source, you have to add the Kafka connector dependency to your project:

  - `flink-connector-kafka-0.8` for Kafka 0.8, and
  - `flink-connector-kafka-0.9` for Kafka 0.9, respectively.

You can then create the source as follows (example for Kafka 0.8):

```java
// The JSON field names and types
String[] fieldNames =  new String[] { "id", "name", "score"};
Class<?>[] fieldTypes = new Class<?>[] { Integer.class, String.class, Double.class };

KafkaJsonTableSource kafkaTableSource = new Kafka08JsonTableSource(
    kafkaTopic,
    kafkaProperties,
    fieldNames,
    fieldTypes);
```

By default, a missing JSON field does not fail the source. You can configure this via:

```java
// Fail on missing JSON field
tableSource.setFailOnMissingField(true);
```

You can work with the Table as explained in the rest of the Table API guide:

```java
tableEnvironment.registerTableSource("kafka-source", kafkaTableSource);
Table result = tableEnvironment.ingest("kafka-source");
```

#### CsvTableSource

The `CsvTableSource` is already included in `flink-table` without additional dependecies.

The easiest way to create a `CsvTableSource` is by using the enclosed builder `CsvTableSource.builder()`, the builder has the following methods to configure properties:

 - `path(String path)` Sets the path to the CSV file, required.
 - `field(String fieldName, TypeInformation<?> fieldType)` Adds a field with the field name and field type information, can be called multiple times, required. The call order of this method defines also the order of the fields in a row.
 - `fieldDelimiter(String delim)` Sets the field delimiter, `","` by default.
 - `lineDelimiter(String delim)` Sets the line delimiter, `"\n"` by default.
 - `quoteCharacter(Character quote)` Sets the quote character for String values, `null` by default.
 - `commentPrefix(String prefix)` Sets a prefix to indicate comments, `null` by default.
 - `ignoreFirstLine()` Ignore the first line. Disabled by default.
 - `ignoreParseErrors()` Skip records with parse error instead to fail. Throwing an exception by default.

You can create the source as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
CsvTableSource csvTableSource = CsvTableSource
    .builder()
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING())
    .field("id", Types.INT())
    .field("score", Types.DOUBLE())
    .field("comments", Types.STRING())
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine()
    .ignoreParseErrors()
    .commentPrefix("%");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val csvTableSource = CsvTableSource
    .builder
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING)
    .field("id", Types.INT)
    .field("score", Types.DOUBLE)
    .field("comments", Types.STRING)
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine
    .ignoreParseErrors
    .commentPrefix("%")
{% endhighlight %}
</div>
</div>

You can work with the Table as explained in the rest of the Table API guide in both stream and batch `TableEnvironment`s:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tableEnvironment.registerTableSource("mycsv", csvTableSource);

Table streamTable = streamTableEnvironment.ingest("mycsv");

Table batchTable = batchTableEnvironment.scan("mycsv");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tableEnvironment.registerTableSource("mycsv", csvTableSource)

val streamTable = streamTableEnvironment.ingest("mycsv")

val batchTable = batchTableEnvironment.scan("mycsv")
{% endhighlight %}
</div>
</div>

Registering external Catalogs
--------------------------------

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

Table API
----------
The Table API provides methods to apply relational operations on DataSets and Datastreams both in Scala and Java.

The central concept of the Table API is a `Table` which represents a table with relational schema (or relation). Tables can be created from a `DataSet` or `DataStream`, converted into a `DataSet` or `DataStream`, or registered in a table catalog using a `TableEnvironment`. A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine Tables of different TableEnvironments.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
When using Flink's Java DataSet API, DataSets are converted to Tables and Tables to DataSets using a `TableEnvironment`.
The following example shows:

- how a `DataSet` is converted to a `Table`,
- how relational queries are specified, and
- how a `Table` is converted back to a `DataSet`.

{% highlight java %}
public class WC {

  public WC(String word, int count) {
    this.word = word; this.count = count;
  }

  public WC() {} // empty constructor to satisfy POJO requirements

  public String word;
  public int count;
}

...

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

DataSet<WC> input = env.fromElements(
        new WC("Hello", 1),
        new WC("Ciao", 1),
        new WC("Hello", 1));

Table table = tEnv.fromDataSet(input);

Table wordCounts = table
        .groupBy("word")
        .select("word, count.sum as count");

DataSet<WC> result = tableEnv.toDataSet(wordCounts, WC.class);
{% endhighlight %}

With Java, expressions must be specified by Strings. The embedded expression DSL is not supported.

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the DataSet cust as table "Customers" with fields derived from the dataset
tableEnv.registerDataSet("Customers", cust)

// register the DataSet ord as table "Orders" with fields user, product, and amount
tableEnv.registerDataSet("Orders", ord, "user, product, amount");
{% endhighlight %}

Please refer to the Javadoc for a full list of supported operations and a description of the expression syntax.
</div>

<div data-lang="scala" markdown="1">
The Table API is enabled by importing `org.apache.flink.table.api.scala._`. This enables
implicit conversions to convert a `DataSet` or `DataStream` to a Table. The following example shows:

- how a `DataSet` is converted to a `Table`,
- how relational queries are specified, and
- how a `Table` is converted back to a `DataSet`.

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

case class WC(word: String, count: Int)

val env = ExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
val expr = input.toTable(tEnv)
val result = expr
               .groupBy('word)
               .select('word, 'count.sum as 'count)
               .toDataSet[WC]
{% endhighlight %}

The expression DSL uses Scala symbols to refer to field names and code generation to
transform expressions to efficient runtime code. Please note that the conversion to and from
Tables only works when using Scala case classes or Java POJOs. Please refer to the [Type Extraction and Serialization]({{ site.baseurl }}/internals/types_serialization.html) section
to learn the characteristics of a valid POJO.

Another example shows how to join two Tables:

{% highlight scala %}
case class MyResult(a: String, d: Int)

val input1 = env.fromElements(...).toTable(tEnv).as('a, 'b)
val input2 = env.fromElements(...).toTable(tEnv, 'c, 'd)

val joined = input1.join(input2)
               .where("a = c && d > 42")
               .select("a, d")
               .toDataSet[MyResult]
{% endhighlight %}

Notice, how the field names of a Table can be changed with `as()` or specified with `toTable()` when converting a DataSet to a Table. In addition, the example shows how to use Strings to specify relational expressions.

Creating a `Table` from a `DataStream` works in a similar way.
The following example shows how to convert a `DataStream` to a `Table` and filter it with the Table API.

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

val inputStream = env.addSource(...)
val result = inputStream
                .toTable(tEnv, 'a, 'b, 'c)
                .filter('a === 3)
val resultStream = result.toDataStream[Row]
{% endhighlight %}

Please refer to the Scaladoc for a full list of supported operations and a description of the expression syntax.
</div>
</div>

{% top %}


### Access a registered Table

A registered table can be accessed from a `TableEnvironment` as follows:

- `tEnv.scan("tName")` scans a `Table` that was registered as `"tName"` in a `BatchTableEnvironment`.
- `tEnv.ingest("tName")` ingests a `Table` that was registered as `"tName"` in a `StreamTableEnvironment`.

{% top %}

### Table API Operators

The Table API features a domain-specific language to execute language-integrated queries on structured data in Scala and Java.
This section gives a brief overview of the available operators. You can find more details of operators in the [Javadoc](http://flink.apache.org/docs/latest/api/java/org/apache/flink/table/api/Table.html).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Select</strong></td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.select("a, c as d");
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight java %}
Table result = in.select("*");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Renames fields.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.as("d, e, f");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Where / Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.where("b = 'red'");
{% endhighlight %}
or
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.filter("a % 2 = 0");
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>GroupBy</strong></td>
      <td>
        <p>Similar to a SQL GROUPBY clause. Groups the rows on the grouping keys, with a following aggregation
        operator to aggregate rows group-wise.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.groupBy("a").select("a, b.sum as d");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined through join operator or using a where or filter operator.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.join(right).where("a = d").select("a, b, e");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>LeftOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL LEFT OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.leftOuterJoin(right, "a = d").select("a, b, e");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>RightOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL RIGHT OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.rightOuterJoin(right, "a = d").select("a, b, e");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FullOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL FULL OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.fullOuterJoin(right, "a = d").select("a, b, e");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.union(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>UnionAll</strong></td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Intersect</strong></td>
      <td>
        <p>Similar to a SQL INTERSECT clause. Intersect returns records that exist in both tables. If a record is present one or both tables more than once, it is returned just once, i.e., the resulting table has no duplicate records. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.intersect(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>IntersectAll</strong></td>
      <td>
        <p>Similar to a SQL INTERSECT ALL clause. IntersectAll returns records that exist in both tables. If a record is present in both tables more than once, it is returned as many times as it is present in both tables, i.e., the resulting table might have duplicate records. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.intersectAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Minus</strong></td>
      <td>
        <p>Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not exist in the right table. Duplicate records in the left table are returned exactly once, i.e., duplicates are removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.minus(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>MinusAll</strong></td>
      <td>
        <p>Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in the right table. A record that is present n times in the left table and m times in the right table is returned (n - m) times, i.e., as many duplicates as are present in the right table are removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.minusAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.distinct();
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Limit</strong></td>
      <td>
        <p>Similar to a SQL LIMIT clause. Limits a sorted result to a specified number of records from an offset position. Limit is technically part of the Order By operator and thus must be preceded by it.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc").limit(3); // returns unlimited number of records beginning with the 4th record
{% endhighlight %}
or
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc").limit(3, 5); // returns 5 records beginning with the 4th record
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operators</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Select</strong></td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.select('a, 'c as 'd);
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.select('*);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Renames fields.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv).as('a, 'b, 'c);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Where / Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.filter('a % 2 === 0)
{% endhighlight %}
or
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.where('b === "red");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>GroupBy</strong></td>
      <td>
        <p>Similar to a SQL GROUPBY clause. Groups rows on the grouping keys, with a following aggregation
        operator to aggregate rows group-wise.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.groupBy('a).select('a, 'b.sum as 'd);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and an equality join predicate must be defined using a where or filter operator.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'd, 'e, 'f);
val result = left.join(right).where('a === 'd).select('a, 'b, 'e);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>LeftOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL LEFT OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight scala %}
val left = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
val right = tableEnv.fromDataSet(ds2, 'd, 'e, 'f)
val result = left.leftOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>RightOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL RIGHT OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight scala %}
val left = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
val right = tableEnv.fromDataSet(ds2, 'd, 'e, 'f)
val result = left.rightOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FullOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL FULL OUTER JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined.</p>
{% highlight scala %}
val left = tableEnv.fromDataSet(ds1, 'a, 'b, 'c)
val right = tableEnv.fromDataSet(ds2, 'd, 'e, 'f)
val result = left.fullOuterJoin(right, 'a === 'd).select('a, 'b, 'e)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed, both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.union(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>UnionAll</strong></td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables, both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Intersect</strong></td>
      <td>
        <p>Similar to a SQL INTERSECT clause. Intersect returns records that exist in both tables. If a record is present in one or both tables more than once, it is returned just once, i.e., the resulting table has no duplicate records. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'e, 'f, 'g);
val result = left.intersect(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>IntersectAll</strong></td>
      <td>
        <p>Similar to a SQL INTERSECT ALL clause. IntersectAll returns records that exist in both tables. If a record is present in both tables more than once, it is returned as many times as it is present in both tables, i.e., the resulting table might have duplicate records. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'e, 'f, 'g);
val result = left.intersectAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Minus</strong></td>
      <td>
        <p>Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not exist in the right table. Duplicate records in the left table are returned exactly once, i.e., duplicates are removed. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.minus(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>MinusAll</strong></td>
      <td>
        <p>Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in the right table. A record that is present n times in the left table and m times in the right table is returned (n - m) times, i.e., as many duplicates as are present in the right table are removed. Both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.minusAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.distinct();
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.orderBy('a.asc);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Limit</strong></td>
      <td>
        <p>Similar to a SQL LIMIT clause. Limits a sorted result to a specified number of records from an offset position. Limit is technically part of the Order By operator and thus must be preceded by it.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.orderBy('a.asc).limit(3); // returns unlimited number of records beginning with the 4th record
{% endhighlight %}
or
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.orderBy('a.asc).limit(3, 5); // returns 5 records beginning with the 4th record
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

{% top %}

### Expression Syntax
Some of the operators in previous sections expect one or more expressions. Expressions can be specified using an embedded Scala DSL or as Strings. Please refer to the examples above to learn how expressions can be specified.

This is the EBNF grammar for expressions:

{% highlight ebnf %}

expressionList = expression , { "," , expression } ;

expression = alias ;

alias = logic | ( logic , "AS" , fieldReference ) ;

logic = comparison , [ ( "&&" | "||" ) , comparison ] ;

comparison = term , [ ( "=" | "==" | "===" | "!=" | "!==" | ">" | ">=" | "<" | "<=" ) , term ] ;

term = product , [ ( "+" | "-" ) , product ] ;

product = unary , [ ( "*" | "/" | "%") , unary ] ;

unary = [ "!" | "-" ] , composite ;

composite = suffixed | atom ;

suffixed = interval | cast | as | aggregation | if | functionCall ;

timeInterval = composite , "." , ("year" | "years" | "month" | "months" | "day" | "days" | "hour" | "hours" | "minute" | "minutes" | "second" | "seconds" | "milli" | "millis") ;

rowInterval = composite , "." , "rows" ;

cast = composite , ".cast(" , dataType , ")" ;

dataType = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOLEAN" | "STRING" | "DECIMAL" | "DATE" | "TIME" | "TIMESTAMP" | "INTERVAL_MONTHS" | "INTERVAL_MILLIS" ;

as = composite , ".as(" , fieldReference , ")" ;

aggregation = composite , ( ".sum" | ".min" | ".max" | ".count" | ".avg" | ".start" | ".end" ) , [ "()" ] ;

if = composite , ".?(" , expression , "," , expression , ")" ;

functionCall = composite , "." , functionIdentifier , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

atom = ( "(" , expression , ")" ) | literal | nullLiteral | fieldReference ;

fieldReference = "*" | identifier ;

nullLiteral = "Null(" , dataType , ")" ;

timeIntervalUnit = "YEAR" | "YEAR_TO_MONTH" | "MONTH" | "DAY" | "DAY_TO_HOUR" | "DAY_TO_MINUTE" | "DAY_TO_SECOND" | "HOUR" | "HOUR_TO_MINUTE" | "HOUR_TO_SECOND" | "MINUTE" | "MINUTE_TO_SECOND" | "SECOND" ;

timePointUnit = "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "WEEK" | "MILLISECOND" | "MICROSECOND" ;

{% endhighlight %}

Here, `literal` is a valid Java literal, `fieldReference` specifies a column in the data (or all columns if `*` is used), and `functionIdentifier` specifies a supported scalar function. The
column names and function names follow Java identifier syntax. The column name `rowtime` is a reserved logical attribute in streaming environments. Expressions specified as Strings can also use prefix notation instead of suffix notation to call operators and functions.

If working with exact numeric values or large decimals is required, the Table API also supports Java's BigDecimal type. In the Scala Table API decimals can be defined by `BigDecimal("123456")` and in Java by appending a "p" for precise e.g. `123456p`.

In order to work with temporal values the Table API supports Java SQL's Date, Time, and Timestamp types. In the Scala Table API literals can be defined by using `java.sql.Date.valueOf("2016-06-27")`, `java.sql.Time.valueOf("10:10:42")`, or `java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")`. The Java and Scala Table API also support calling `"2016-06-27".toDate()`, `"10:10:42".toTime()`, and `"2016-06-27 10:10:42.123".toTimestamp()` for converting Strings into temporal types. *Note:* Since Java's temporal SQL types are time zone dependent, please make sure that the Flink Client and all TaskManagers use the same time zone.

Temporal intervals can be represented as number of months (`Types.INTERVAL_MONTHS`) or number of milliseconds (`Types.INTERVAL_MILLIS`). Intervals of same type can be added or subtracted (e.g. `1.hour + 10.minutes`). Intervals of milliseconds can be added to time points (e.g. `"2016-08-10".toDate + 5.days`).

{% top %}

### Windows

The Table API is a declarative API to define queries on batch and streaming tables. Projection, selection, and union operations can be applied both on streaming and batch tables without additional semantics. Aggregations on (possibly) infinite streaming tables, however, can only be computed on finite groups of records. Window aggregates group rows into finite groups based on time or row-count intervals and evaluate aggregation functions once per group. For batch tables, windows are a convenient shortcut to group records by time intervals.

Windows are defined using the `window(w: Window)` clause and require an alias, which is specified using the `as` clause. In order to group a table by a window, the window alias must be referenced in the `groupBy(...)` clause like a regular grouping attribute. 
The following example shows how to define a window aggregation on a table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([Window w].as("w"))  // define window with alias w
  .groupBy("w")  // group the table by window w
  .select("b.sum")  // aggregate
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: Window] as 'w)  // define window with alias w
  .groupBy('w)   // group the table by window w
  .select('b.sum)  // aggregate
{% endhighlight %}
</div>
</div>

In streaming environments, window aggregates can only be computed in parallel if they group on one or more attributes in addition to the window, i.e., the `groupBy(...)` clause references a window alias and at least one additional attribute. A `groupBy(...)` clause that only references a window alias (such as in the example above) can only be evaluated by a single, non-parallel task. 
The following example shows how to define a window aggregation with additional grouping attributes.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([Window w].as("w"))  // define window with alias w
  .groupBy("w, a")  // group the table by attribute a and window w 
  .select("a, b.sum")  // aggregate
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: Window] as 'w) // define window with alias w
  .groupBy('w, 'a)  // group the table by attribute a and window w 
  .select('a, 'b.sum)  // aggregate
{% endhighlight %}
</div>
</div>

The `Window` parameter defines how rows are mapped to windows. `Window` is not an interface that users can implement. Instead, the Table API provides a set of predefined `Window` classes with specific semantics, which are translated into underlying `DataStream` or `DataSet` operations. The supported window definitions are listed below. Window properties such as the start and end timestamp of a time window can be added in the select statement as a property of the window alias as `w.start` and `w.end`, respectively.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([Window w].as("w"))  // define window with alias w
  .groupBy("w, a")  // group the table by attribute a and window w 
  .select("a, w.start, w.end, b.count") // aggregate and add window start and end timestamps
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val table = input
  .window([w: Window] as 'w)  // define window with alias w
  .groupBy('w, 'a)  // group the table by attribute a and window w 
  .select('a, 'w.start, 'w.end, 'b.count) // aggregate and add window start and end timestamps
{% endhighlight %}
</div>
</div>

#### Tumble (Tumbling Windows)

A tumbling window assigns rows to non-overlapping, continuous windows of fixed length. For example, a tumbling window of 5 minutes groups rows in 5 minutes intervals. Tumbling windows can be defined on event-time, processing-time, or on a row-count.

Tumbling windows are defined by using the `Tumble` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left" style="width: 20%">Required?</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Required.</td>
      <td>Defines the length the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>Required for streaming event-time windows and windows on batch tables.</td>
      <td>Defines the time mode for streaming tables (<code>rowtime</code> is a logical system attribute); for batch tables, the time attribute on which records are grouped.</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Required.</td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start or end time in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Tumbling Event-time Window
.window(Tumble.over("10.minutes").on("rowtime").as("w"))

// Tumbling Processing-time Window
.window(Tumble.over("10.minutes").as("w"))

// Tumbling Row-count Window
.window(Tumble.over("10.rows").as("w"))
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Tumbling Event-time Window
.window(Tumble over 10.minutes on 'rowtime as 'w)

// Tumbling Processing-time Window
.window(Tumble over 10.minutes as 'w)

// Tumbling Row-count Window
.window(Tumble over 10.rows as 'w)
{% endhighlight %}
</div>
</div>

#### Slide (Sliding Windows)

A sliding window has a fixed size and slides by a specified slide interval. If the slide interval is smaller than the window size, sliding windows are overlapping. Thus, rows can be assigned to multiple windows. For example, a sliding window of 15 minutes size and 5 minute slide interval assigns each row to 3 different windows of 15 minute size, which are evaluated in an interval of 5 minutes. Sliding windows can be defined on event-time, processing-time, or on a row-count.

Sliding windows are defined by using the `Slide` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left" style="width: 20%">Required?</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>over</code></td>
      <td>Required.</td>
      <td>Defines the length of the window, either as time or row-count interval.</td>
    </tr>
    <tr>
      <td><code>every</code></td>
      <td>Required.</td>
      <td>Defines the slide interval, either as time or row-count interval. The slide interval must be of the same type as the size interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>Required for event-time windows and windows on batch tables.</td>
      <td>Defines the time mode for streaming tables (<code>rowtime</code> is a logical system attribute); for batch tables, the time attribute on which records are grouped</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Required.</td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start or end time in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Sliding Event-time Window
.window(Slide.over("10.minutes").every("5.minutes").on("rowtime").as("w"))

// Sliding Processing-time window
.window(Slide.over("10.minutes").every("5.minutes").as("w"))

// Sliding Row-count window
.window(Slide.over("10.rows").every("5.rows").as("w"))
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)

// Sliding Processing-time window
.window(Slide over 10.minutes every 5.minutes as 'w)

// Sliding Row-count window
.window(Slide over 10.rows every 5.rows as 'w)
{% endhighlight %}
</div>
</div>

#### Session (Session Windows)

Session windows do not have a fixed size but their bounds are defined by an interval of inactivity, i.e., a session window is closes if no event appears for a defined gap period. For example a session window with a 30 minute gap starts when a row is observed after 30 minutes inactivity (otherwise the row would be added to an existing window) and is closed if no row is added within 30 minutes. Session windows can work on event-time or processing-time.

A session window is defined by using the `Session` class as follows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left" style="width: 20%">Required?</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>withGap</code></td>
      <td>Required.</td>
      <td>Defines the gap between two windows as time interval.</td>
    </tr>
    <tr>
      <td><code>on</code></td>
      <td>Required for event-time windows and windows on batch tables.</td>
      <td>Defines the time mode for streaming tables (<code>rowtime</code> is a logical system attribute); for batch tables, the time attribute on which records are grouped</td>
    </tr>
    <tr>
      <td><code>as</code></td>
      <td>Required.</td>
      <td>Assigns an alias to the window. The alias is used to reference the window in the following <code>groupBy()</code> clause and optionally to select window properties such as window start or end time in the <code>select()</code> clause.</td>
    </tr>
  </tbody>
</table>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// Session Event-time Window
.window(Session.withGap("10.minutes").on("rowtime").as("w"))

// Session Processing-time Window
.window(Session.withGap("10.minutes").as("w"))
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// Session Event-time Window
.window(Session withGap 10.minutes on 'rowtime as 'w)

// Session Processing-time Window
.window(Session withGap 10.minutes as 'w)
{% endhighlight %}
</div>
</div>

#### Limitations

Currently the following features are not supported yet:

- Row-count windows on event-time
- Non-grouped session windows on batch tables
- Sliding windows on batch tables

SQL
----
SQL queries are specified using the `sql()` method of the `TableEnvironment`. The method returns the result of the SQL query as a `Table` which can be converted into a `DataSet` or `DataStream`, used in subsequent Table API queries, or written to a `TableSink` (see [Writing Tables to External Sinks](#writing-tables-to-external-sinks)). SQL and Table API queries can seamlessly mixed and are holistically optimized and translated into a single DataStream or DataSet program.

A `Table`, `DataSet`, `DataStream`, or external `TableSource` must be registered in the `TableEnvironment` in order to be accessible by a SQL query (see [Registering Tables](#registering-tables)). For convenience `Table.toString()` will automatically register an unique table name under the `Table`'s `TableEnvironment` and return the table name. So it allows to call SQL directly on tables in a string concatenation (see examples below).

*Note: Flink's SQL support is not feature complete, yet. Queries that include unsupported SQL features will cause a `TableException`. The limitations of SQL on batch and streaming tables are listed in the following sections.*

### SQL on Batch Tables

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// read a DataSet from an external source
DataSet<Tuple3<Long, String, Integer>> ds = env.readCsvFile(...);

// call SQL on unregistered tables
Table table = tableEnv.toTable(ds, "user, product, amount");
Table result = tableEnv.sql(
  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

// call SQL on registered tables
// register the DataSet as table "Orders"
tableEnv.registerDataSet("Orders", ds, "user, product, amount");
// run a SQL query on the Table and retrieve the result as a new Table
Table result2 = tableEnv.sql(
  "SELECT SUM(amount) FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataSet from an external source
val ds: DataSet[(Long, String, Integer)] = env.readCsvFile(...)

// call SQL on unregistered tables
val table = ds.toTable(tableEnv, 'user, 'product, 'amount)
val result = tableEnv.sql(
  s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

// call SQL on registered tables
// register the DataSet under the name "Orders"
tableEnv.registerDataSet("Orders", ds, 'user, 'product, 'amount)
// run a SQL query on the Table and retrieve the result as a new Table
val result2 = tableEnv.sql(
  "SELECT SUM(amount) FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

#### Limitations

The current version supports selection (filter), projection, inner equi-joins, grouping, aggregates, and sorting on batch tables.

Among others, the following SQL features are not supported, yet:

- Timestamps and intervals are limited to milliseconds precision
- Interval arithmetic is currenly limited
- Non-equi joins and Cartesian products
- Efficient grouping sets

*Note: Tables are joined in the order in which they are specified in the `FROM` clause. In some cases the table order must be manually tweaked to resolve Cartesian products.*

### SQL on Streaming Tables

SQL queries can be executed on streaming Tables (Tables backed by `DataStream` or `StreamTableSource`) like standard SQL.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);

// call SQL on unregistered tables
Table table = tableEnv.toTable(ds, "user, product, amount");
Table result = tableEnv.sql(
  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

// call SQL on registered tables
// register the DataStream as table "Orders"
tableEnv.registerDataStream("Orders", ds, "user, product, amount");
// run a SQL query on the Table and retrieve the result as a new Table
Table result2 = tableEnv.sql(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(Long, String, Integer)] = env.addSource(...)

// call SQL on unregistered tables
val table = ds.toTable(tableEnv, 'user, 'product, 'amount)
val result = tableEnv.sql(
  s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

// call SQL on registered tables
// register the DataStream under the name "Orders"
tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount)
// run a SQL query on the Table and retrieve the result as a new Table
val result2 = tableEnv.sql(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

#### Group windows

Streaming SQL supports aggregation on group windows by specifying the windows in the `GROUP BY` clause. The following table describes the syntax of the group windows:

<table class="table table-bordered">
  <thead>
    <tr>
      <th><code>GROUP BY</code> clause</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>TUMBLE(mode, interval)</code></td>
      <td>A tumbling window over the time period specified by <code>interval</code>.</td>
    </tr>
    <tr>
      <td><code>HOP(mode, slide, size)</code></td>
      <td>A sliding window with the length of <code>size</code> and moves every <code>slide</code>.</td>
    </tr>
    <tr>
      <td><code>SESSION(mode, gap)</code></td>
      <td>A session window that has <code>gap</code> as the gap between two windows.</td>
    </tr>
  </tbody>
</table>

The parameters `interval`, `slide`, `size`, `gap` must be constant time intervals. The `mode` can be either `proctime()` or `rowtime()`, which specifies the window is over the processing time or the event time.

As an example, the following SQL computes the total number of records over a 15 minute tumbling window over processing time:

```
SELECT COUNT(*) FROM $table GROUP BY TUMBLE(proctime(), INTERVAL '15' MINUTE)
```

#### Limitations

The current version of streaming SQL only supports `SELECT`, `FROM`, `WHERE`, and `UNION` clauses. Aggregations or joins are not fully supported yet.

{% top %}

### SQL Syntax

Flink uses [Apache Calcite](https://calcite.apache.org/docs/reference.html) for SQL parsing. Currently, Flink SQL only supports query-related SQL syntax and only a subset of the comprehensive SQL standard. The following BNF-grammar describes the supported SQL features:

```

query:
  values
  | {
      select
      | selectWithoutFrom
      | query UNION [ ALL ] query
      | query EXCEPT query
      | query INTERSECT query
    }
    [ ORDER BY orderItem [, orderItem ]* ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start { ROW | ROWS } ]
    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY]

orderItem:
  expression [ ASC | DESC ]

select:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }
  FROM tableExpression
  [ WHERE booleanExpression ]
  [ GROUP BY { groupItem [, groupItem ]* } ]
  [ HAVING booleanExpression ]

selectWithoutFrom:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }

projectItem:
  expression [ [ AS ] columnAlias ]
  | tableAlias . *

tableExpression:
  tableReference [, tableReference ]*
  | tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]

joinCondition:
  ON booleanExpression
  | USING '(' column [, column ]* ')'

tableReference:
  tablePrimary
  [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
  [ TABLE ] [ [ catalogName . ] schemaName . ] tableName
  | LATERAL TABLE '(' functionName '(' expression [, expression ]* ')' ')'

values:
  VALUES expression [, expression ]*

groupItem:
  expression
  | '(' ')'
  | '(' expression [, expression ]* ')'
  | CUBE '(' expression [, expression ]* ')'
  | ROLLUP '(' expression [, expression ]* ')'
  | GROUPING SETS '(' groupItem [, groupItem ]* ')'
```

For a better definition of SQL queries within a Java String, Flink SQL uses a lexical policy similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. <code>"SELECT a AS `my field` FROM t"</code>).


{% top %}

### Reserved Keywords

Although not every SQL feature is implemented yet, some string combinations are already reserved as keywords for future use. If you want to use one of the following strings as a field name, make sure to surround them with backticks (e.g. `` `value` ``, `` `count` ``).

{% highlight sql %}

A, ABS, ABSOLUTE, ACTION, ADA, ADD, ADMIN, AFTER, ALL, ALLOCATE, ALLOW, ALTER, ALWAYS, AND, ANY, ARE, ARRAY, AS, ASC, ASENSITIVE, ASSERTION, ASSIGNMENT, ASYMMETRIC, AT, ATOMIC, ATTRIBUTE, ATTRIBUTES, AUTHORIZATION, AVG, BEFORE, BEGIN, BERNOULLI, BETWEEN, BIGINT, BINARY, BIT, BLOB, BOOLEAN, BOTH, BREADTH, BY, C, CALL, CALLED, CARDINALITY, CASCADE, CASCADED, CASE, CAST, CATALOG, CATALOG_NAME, CEIL, CEILING, CENTURY, CHAIN, CHAR, CHARACTER, CHARACTERISTICTS, CHARACTERS, CHARACTER_LENGTH, CHARACTER_SET_CATALOG, CHARACTER_SET_NAME, CHARACTER_SET_SCHEMA, CHAR_LENGTH, CHECK, CLASS_ORIGIN, CLOB, CLOSE, COALESCE, COBOL, COLLATE, COLLATION, COLLATION_CATALOG, COLLATION_NAME, COLLATION_SCHEMA, COLLECT, COLUMN, COLUMN_NAME, COMMAND_FUNCTION, COMMAND_FUNCTION_CODE, COMMIT, COMMITTED, CONDITION, CONDITION_NUMBER, CONNECT, CONNECTION, CONNECTION_NAME, CONSTRAINT, CONSTRAINTS, CONSTRAINT_CATALOG, CONSTRAINT_NAME, CONSTRAINT_SCHEMA, CONSTRUCTOR, CONTAINS, CONTINUE, CONVERT, CORR, CORRESPONDING, COUNT, COVAR_POP, COVAR_SAMP, CREATE, CROSS, CUBE, CUME_DIST, CURRENT, CURRENT_CATALOG, CURRENT_DATE, CURRENT_DEFAULT_TRANSFORM_GROUP, CURRENT_PATH, CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TRANSFORM_GROUP_FOR_TYPE, CURRENT_USER, CURSOR, CURSOR_NAME, CYCLE, DATA, DATABASE, DATE, DATETIME_INTERVAL_CODE, DATETIME_INTERVAL_PRECISION, DAY, DEALLOCATE, DEC, DECADE, DECIMAL, DECLARE, DEFAULT, DEFAULTS, DEFERRABLE, DEFERRED, DEFINED, DEFINER, DEGREE, DELETE, DENSE_RANK, DEPTH, DEREF, DERIVED, DESC, DESCRIBE, DESCRIPTION, DESCRIPTOR, DETERMINISTIC, DIAGNOSTICS, DISALLOW, DISCONNECT, DISPATCH, DISTINCT, DOMAIN, DOUBLE, DOW, DOY, DROP, DYNAMIC, DYNAMIC_FUNCTION, DYNAMIC_FUNCTION_CODE, EACH, ELEMENT, ELSE, END, END-EXEC, EPOCH, EQUALS, ESCAPE, EVERY, EXCEPT, EXCEPTION, EXCLUDE, EXCLUDING, EXEC, EXECUTE, EXISTS, EXP, EXPLAIN, EXTEND, EXTERNAL, EXTRACT, FALSE, FETCH, FILTER, FINAL, FIRST, FIRST_VALUE, FLOAT, FLOOR, FOLLOWING, FOR, FOREIGN, FORTRAN, FOUND, FRAC_SECOND, FREE, FROM, FULL, FUNCTION, FUSION, G, GENERAL, GENERATED, GET, GLOBAL, GO, GOTO, GRANT, GRANTED, GROUP, GROUPING, HAVING, HIERARCHY, HOLD, HOUR, IDENTITY, IMMEDIATE, IMPLEMENTATION, IMPORT, IN, INCLUDING, INCREMENT, INDICATOR, INITIALLY, INNER, INOUT, INPUT, INSENSITIVE, INSERT, INSTANCE, INSTANTIABLE, INT, INTEGER, INTERSECT, INTERSECTION, INTERVAL, INTO, INVOKER, IS, ISOLATION, JAVA, JOIN, K, KEY, KEY_MEMBER, KEY_TYPE, LABEL, LANGUAGE, LARGE, LAST, LAST_VALUE, LATERAL, LEADING, LEFT, LENGTH, LEVEL, LIBRARY, LIKE, LIMIT, LN, LOCAL, LOCALTIME, LOCALTIMESTAMP, LOCATOR, LOWER, M, MAP, MATCH, MATCHED, MAX, MAXVALUE, MEMBER, MERGE, MESSAGE_LENGTH, MESSAGE_OCTET_LENGTH, MESSAGE_TEXT, METHOD, MICROSECOND, MILLENNIUM, MIN, MINUTE, MINVALUE, MOD, MODIFIES, MODULE, MONTH, MORE, MULTISET, MUMPS, NAME, NAMES, NATIONAL, NATURAL, NCHAR, NCLOB, NESTING, NEW, NEXT, NO, NONE, NORMALIZE, NORMALIZED, NOT, NULL, NULLABLE, NULLIF, NULLS, NUMBER, NUMERIC, OBJECT, OCTETS, OCTET_LENGTH, OF, OFFSET, OLD, ON, ONLY, OPEN, OPTION, OPTIONS, OR, ORDER, ORDERING, ORDINALITY, OTHERS, OUT, OUTER, OUTPUT, OVER, OVERLAPS, OVERLAY, OVERRIDING, PAD, PARAMETER, PARAMETER_MODE, PARAMETER_NAME, PARAMETER_ORDINAL_POSITION, PARAMETER_SPECIFIC_CATALOG, PARAMETER_SPECIFIC_NAME, PARAMETER_SPECIFIC_SCHEMA, PARTIAL, PARTITION, PASCAL, PASSTHROUGH, PATH, PERCENTILE_CONT, PERCENTILE_DISC, PERCENT_RANK, PLACING, PLAN, PLI, POSITION, POWER, PRECEDING, PRECISION, PREPARE, PRESERVE, PRIMARY, PRIOR, PRIVILEGES, PROCEDURE, PUBLIC, QUARTER, RANGE, RANK, READ, READS, REAL, RECURSIVE, REF, REFERENCES, REFERENCING, REGR_AVGX, REGR_AVGY, REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX, REGR_SXY, REGR_SYY, RELATIVE, RELEASE, REPEATABLE, RESET, RESTART, RESTRICT, RESULT, RETURN, RETURNED_CARDINALITY, RETURNED_LENGTH, RETURNED_OCTET_LENGTH, RETURNED_SQLSTATE, RETURNS, REVOKE, RIGHT, ROLE, ROLLBACK, ROLLUP, ROUTINE, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_SCHEMA, ROW, ROWS, ROW_COUNT, ROW_NUMBER, SAVEPOINT, SCALE, SCHEMA, SCHEMA_NAME, SCOPE, SCOPE_CATALOGS, SCOPE_NAME, SCOPE_SCHEMA, SCROLL, SEARCH, SECOND, SECTION, SECURITY, SELECT, SELF, SENSITIVE, SEQUENCE, SERIALIZABLE, SERVER, SERVER_NAME, SESSION, SESSION_USER, SET, SETS, SIMILAR, SIMPLE, SIZE, SMALLINT, SOME, SOURCE, SPACE, SPECIFIC, SPECIFICTYPE, SPECIFIC_NAME, SQL, SQLEXCEPTION, SQLSTATE, SQLWARNING, SQL_TSI_DAY, SQL_TSI_FRAC_SECOND, SQL_TSI_HOUR, SQL_TSI_MICROSECOND, SQL_TSI_MINUTE, SQL_TSI_MONTH, SQL_TSI_QUARTER, SQL_TSI_SECOND, SQL_TSI_WEEK, SQL_TSI_YEAR, SQRT, START, STATE, STATEMENT, STATIC, STDDEV_POP, STDDEV_SAMP, STREAM, STRUCTURE, STYLE, SUBCLASS_ORIGIN, SUBMULTISET, SUBSTITUTE, SUBSTRING, SUM, SYMMETRIC, SYSTEM, SYSTEM_USER, TABLE, TABLESAMPLE, TABLE_NAME, TEMPORARY, THEN, TIES, TIME, TIMESTAMP, TIMESTAMPADD, TIMESTAMPDIFF, TIMEZONE_HOUR, TIMEZONE_MINUTE, TINYINT, TO, TOP_LEVEL_COUNT, TRAILING, TRANSACTION, TRANSACTIONS_ACTIVE, TRANSACTIONS_COMMITTED, TRANSACTIONS_ROLLED_BACK, TRANSFORM, TRANSFORMS, TRANSLATE, TRANSLATION, TREAT, TRIGGER, TRIGGER_CATALOG, TRIGGER_NAME, TRIGGER_SCHEMA, TRIM, TRUE, TYPE, UESCAPE, UNBOUNDED, UNCOMMITTED, UNDER, UNION, UNIQUE, UNKNOWN, UNNAMED, UNNEST, UPDATE, UPPER, UPSERT, USAGE, USER, USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_CODE, USER_DEFINED_TYPE_NAME, USER_DEFINED_TYPE_SCHEMA, USING, VALUE, VALUES, VARBINARY, VARCHAR, VARYING, VAR_POP, VAR_SAMP, VERSION, VIEW, WEEK, WHEN, WHENEVER, WHERE, WIDTH_BUCKET, WINDOW, WITH, WITHIN, WITHOUT, WORK, WRAPPER, WRITE, XML, YEAR, ZONE

{% endhighlight %}

{% top %}

Data Types
----------

The Table API is built on top of Flink's DataSet and DataStream API. Internally, it also uses Flink's `TypeInformation` to distinguish between types. The Table API does not support all Flink types so far. All supported simple types are listed in `org.apache.flink.table.api.Types`. The following table summarizes the relation between Table API types, SQL types, and the resulting Java class.

| Table API              | SQL                         | Java type              |
| :--------------------- | :-------------------------- | :--------------------- |
| `Types.STRING`         | `VARCHAR`                   | `java.lang.String`     |
| `Types.BOOLEAN`        | `BOOLEAN`                   | `java.lang.Boolean`    |
| `Types.BYTE`           | `TINYINT`                   | `java.lang.Byte`       |
| `Types.SHORT`          | `SMALLINT`                  | `java.lang.Short`      |
| `Types.INT`            | `INTEGER, INT`              | `java.lang.Integer`    |
| `Types.LONG`           | `BIGINT`                    | `java.lang.Long`       |
| `Types.FLOAT`          | `REAL, FLOAT`               | `java.lang.Float`      |
| `Types.DOUBLE`         | `DOUBLE`                    | `java.lang.Double`     |
| `Types.DECIMAL`        | `DECIMAL`                   | `java.math.BigDecimal` |
| `Types.DATE`           | `DATE`                      | `java.sql.Date`        |
| `Types.TIME`           | `TIME`                      | `java.sql.Time`        |
| `Types.TIMESTAMP`      | `TIMESTAMP(3)`              | `java.sql.Timestamp`   |
| `Types.INTERVAL_MONTHS`| `INTERVAL YEAR TO MONTH`    | `java.lang.Integer`    |
| `Types.INTERVAL_MILLIS`| `INTERVAL DAY TO SECOND(3)` | `java.lang.Long`       |


Advanced types such as generic types, composite types (e.g. POJOs or Tuples), and array types (object or primitive arrays) can be fields of a row. 

Generic types are treated as a black box within Table API and SQL yet.

Composite types, however, are fully supported types where fields of a composite type can be accessed using the `.get()` operator in Table API and dot operator (e.g. `MyTable.pojoColumn.myField`) in SQL. Composite types can also be flattened using `.flatten()` in Table API or `MyTable.pojoColumn.*` in SQL.

Array types can be accessed using the `myArray.at(1)` operator in Table API and `myArray[1]` operator in SQL. Array literals can be created using `array(1, 2, 3)` in Table API and `ARRAY[1, 2, 3]` in SQL.

{% top %}

Built-in Functions
----------------

Both the Table API and SQL come with a set of built-in functions for data transformations. This section gives a brief overview of the available functions so far.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Comparison functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
ANY === ANY
{% endhighlight %}
      </td>
      <td>
        <p>Equals.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY !== ANY
{% endhighlight %}
      </td>
      <td>
        <p>Not equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY > ANY
{% endhighlight %}
      </td>
      <td>
        <p>Greater than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY >= ANY
{% endhighlight %}
      </td>
      <td>
        <p>Greater than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY < ANY
{% endhighlight %}
      </td>
      <td>
        <p>Less than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY <= ANY
{% endhighlight %}
      </td>
      <td>
        <p>Less than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.isNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given expression is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.isNotNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given expression is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.like(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified LIKE pattern. E.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.similar(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified SQL regex pattern. E.g. "A+" matches all strings that consist of at least one "A".</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
boolean1 || boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if <i>boolean1</i> is true or <i>boolean2</i> is true. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
boolean1 && boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if <i>boolean1</i> and <i>boolean2</i> are both true. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
!BOOLEAN
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if boolean expression is not true; returns null if boolean is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given boolean expression is true. False otherwise (for null and false).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if given boolean expression is false. False otherwise (for null and true).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isNotTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given boolean expression is not true (for null and false). False otherwise.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.isNotFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if given boolean expression is not false (for null and true). False otherwise.</p>
      </td>
    </tr>

  </tbody>
</table>


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

   <tr>
      <td>
        {% highlight java %}
+ numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
- numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>numeric</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
numeric1 + numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> plus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
numeric1 - numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> minus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
numeric1 * numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> multiplied by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
numeric1 / numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> divided by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
numeric1.power(numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> raised to the power of <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.abs()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the absolute value of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
numeric1 % numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>numeric1</i> divided by <i>numeric2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sqrt()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the square root of a given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.ln()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the natural logarithm of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.log10()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the base 10 logarithm of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.exp()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the Euler's number raised to the given power.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.ceil()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the smallest integer greater than or equal to a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the largest integer less than or equal to a given number.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">String functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
STRING + STRING
{% endhighlight %}
      </td>
      <td>
        <p>Concatenates two character strings.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.charLength()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the length of a String.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.upperCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in upper case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.lowerCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in lower case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.position(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position of string in an other string starting at 1. Returns 0 if string could not be found. E.g. <code>'a'.position('bbbbba')</code> leads to 6.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.trim(LEADING, STRING)
STRING.trim(TRAILING, STRING)
STRING.trim(BOTH, STRING)
STRING.trim(BOTH)
STRING.trim()
{% endhighlight %}
      </td>
      <td>
        <p>Removes leading and/or trailing characters from the given string. By default, whitespaces at both sides are removed.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.overlay(STRING, INT)
STRING.overlay(STRING, INT, INT)
{% endhighlight %}
      </td>
      <td>
        <p>Replaces a substring of string with a string starting at a position (starting at 1). An optional length specifies how many characters should be removed. E.g. <code>'xxxxxtest'.overlay('xxxx', 6)</code> leads to "xxxxxxxxx", <code>'xxxxxtest'.overlay('xxxx', 6, 2)</code> leads to "xxxxxxxxxst".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.substring(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string beginning at the given index to the end. The start index starts at 1 and is inclusive.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.substring(INT, INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string at the given index for the given length. The index starts at 1 and is inclusive, i.e., the character at the index is included in the substring. The substring has the specified length or less.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.initCap()
{% endhighlight %}
      </td>

      <td>
        <p>Converts the initial letter of each word in a string to uppercase. Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.?(value1, value2)
{% endhighlight %}
      </td>
      <td>
        <p>Ternary conditional operator that decides which of two other expressions should be evaluated based on a evaluated boolean condition. E.g. <code>(42 > 5).?("A", "B")</code> leads to "A".</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

    <tr>
      <td>
        {% highlight java %}
ANY.cast(TYPE)
{% endhighlight %}
      </td>
      <td>
        <p>Converts a value to a given type. E.g. <code>"42".cast(INT)</code> leads to 42.</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

    <tr>
      <td>
        {% highlight java %}
ARRAY.at(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at a particular position in an array. The index starts at 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
array(ANY [, ANY ]*)
{% endhighlight %}
      </td>
      <td>
        <p>Creates an array from a list of values. The array will be an array of objects (not primitives).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.rows
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of rows.</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

   <tr>
      <td>
        {% highlight java %}
STRING.toDate()
{% endhighlight %}
      </td>
      <td>
        <p>Parses a date string in the form "yy-mm-dd" to a SQL date.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.toTime()
{% endhighlight %}
      </td>
      <td>
        <p>Parses a time string in the form "hh:mm:ss" to a SQL time.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
STRING.toTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Parses a timestamp string in the form "yy-mm-dd hh:mm:ss.fff" to a SQL timestamp.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.year
NUMERIC.years
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for a given number of years.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.month
NUMERIC.months
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for a given number of months.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.day
NUMERIC.days
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of days.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.hour
NUMERIC.hours
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of hours.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.minute
NUMERIC.minutes
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of minutes.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.second
NUMERIC.seconds
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of seconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.milli
NUMERIC.millis
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentDate()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
currentTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
localTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
localTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TEMPORAL.extract(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Extracts parts of a time point or time interval. Returns the part as a long value. E.g. <code>'2006-06-05'.toDate.extract(DAY)</code> leads to 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TIMEPOINT.floor(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point down to the given unit. E.g. <code>'12:44:31'.toDate.floor(MINUTE)</code> leads to 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
TIMEPOINT.ceil(TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point up to the given unit. E.g. <code>'12:44:31'.toTime.floor(MINUTE)</code> leads to 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
DATE.quarter()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the quarter of a year from a SQL date. E.g. <code>'1994-09-27'.toDate.quarter()</code> leads to 3.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
temporalOverlaps(TIMEPOINT, TEMPORAL, TIMEPOINT, TEMPORAL)
{% endhighlight %}
      </td>
      <td>
        <p>Determines whether two anchored time intervals overlap. Time point and temporal are transformed into a range defined by two time points (start, end). The function evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>. E.g. <code>temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour)</code> leads to true.</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

    <tr>
      <td>
        {% highlight java %}
FIELD.count
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows for which the field is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.avg
{% endhighlight %}
      </td>
      <td>
        <p>Returns the average (arithmetic mean) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.sum
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.max
{% endhighlight %}
      </td>
      <td>
        <p>Returns the maximum value of field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.min
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minimum value of field across all input values.</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
COMPOSITE.get(STRING)
COMPOSITE.get(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index or name and returns it's value. E.g. <code>pojo.get('myField')</code> or <code>tuple.get(0)</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ANY.flatten()
{% endhighlight %}
      </td>
      <td>
        <p>Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes into a flat representation where every subtype is a separate field. In most cases the fields of the flat representation are named similarly to the original fields but with a dollar separator (e.g. <code>mypojo$mytuple$f0</code>).</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Array functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
ARRAY.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements of an array.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
ARRAY.element()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of an array with a single element. Returns <code>null</code> if the array is empty. Throws an exception if the array has more than one element.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight java %}
ANY.as(name [, name ]* )
{% endhighlight %}
      </td>
      <td>
        <p>Specifies a name for an expression i.e. a field. Additional names can be specified if the expression expands to multiple fields.</p>
      </td>
    </tr>

  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Comparison functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

     <tr>
      <td>
        {% highlight scala %}
ANY === ANY
{% endhighlight %}
      </td>
      <td>
        <p>Equals.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY !== ANY
{% endhighlight %}
      </td>
      <td>
        <p>Not equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY > ANY
{% endhighlight %}
      </td>
      <td>
        <p>Greater than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY >= ANY
{% endhighlight %}
      </td>
      <td>
        <p>Greater than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY < ANY
{% endhighlight %}
      </td>
      <td>
        <p>Less than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY <= ANY
{% endhighlight %}
      </td>
      <td>
        <p>Less than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.isNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given expression is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.isNotNull
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given expression is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.like(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified LIKE pattern. E.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.similar(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified SQL regex pattern. E.g. "A+" matches all strings that consist of at least one "A".</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
boolean1 || boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if <i>boolean1</i> is true or <i>boolean2</i> is true. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
boolean1 && boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if <i>boolean1</i> and <i>boolean2</i> are both true. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
!BOOLEAN
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if boolean expression is not true; returns null if boolean is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given boolean expression is true. False otherwise (for null and false).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if given boolean expression is false. False otherwise (for null and true).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isNotTrue
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if the given boolean expression is not true (for null and false). False otherwise.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
BOOLEAN.isNotFalse
{% endhighlight %}
      </td>
      <td>
        <p>Returns true if given boolean expression is not false (for null and true). False otherwise.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

   <tr>
      <td>
        {% highlight scala %}
+ numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
- numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>numeric</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
numeric1 + numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> plus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
numeric1 - numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> minus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
numeric1 * numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> multiplied by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
numeric1 / numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> divided by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
numeric1.power(numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> raised to the power of <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.abs()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the absolute value of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
numeric1 % numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>numeric1</i> divided by <i>numeric2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sqrt()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the square root of a given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.ln()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the natural logarithm of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.log10()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the base 10 logarithm of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.exp()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the Euler's number raised to the given power.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.ceil()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the smallest integer greater than or equal to a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the largest integer less than or equal to a given number.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
STRING + STRING
{% endhighlight %}
      </td>
      <td>
        <p>Concatenates two character strings.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.charLength()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the length of a String.</p>
      </td>
    </tr> 

    <tr>
      <td>
        {% highlight scala %}
STRING.upperCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in upper case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.lowerCase()
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in lower case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.position(STRING)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position of string in an other string starting at 1. Returns 0 if string could not be found. E.g. <code>"a".position("bbbbba")</code> leads to 6.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.trim(
  leading = true,
  trailing = true,
  character = " ")
{% endhighlight %}
      </td>
      <td>
        <p>Removes leading and/or trailing characters from the given string.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.overlay(STRING, INT)
STRING.overlay(STRING, INT, INT)
{% endhighlight %}
      </td>
      <td>
        <p>Replaces a substring of string with a string starting at a position (starting at 1). An optional length specifies how many characters should be removed. E.g. <code>"xxxxxtest".overlay("xxxx", 6)</code> leads to "xxxxxxxxx", <code>"xxxxxtest".overlay('xxxx', 6, 2)</code> leads to "xxxxxxxxxst".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.substring(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string beginning at the given index to the end. The start index starts at 1 and is inclusive.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.substring(INT, INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string at the given index for the given length. The index starts at 1 and is inclusive, i.e., the character at the index is included in the substring. The substring has the specified length or less.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.initCap()
{% endhighlight %}
      </td>

      <td>
        <p>Converts the initial letter of each word in a string to uppercase. Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  
  <tbody>

    <tr>
      <td>
        {% highlight java %}
BOOLEAN.?(value1, value2)
{% endhighlight %}
      </td>
      <td>
        <p>Ternary conditional operator that decides which of two other expressions should be evaluated based on a evaluated boolean condition. E.g. <code>(42 > 5).?("A", "B")</code> leads to "A".</p>
      </td>
    </tr>

    </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
ANY.cast(TYPE)
{% endhighlight %}
      </td>
      <td>
        <p>Converts a value to a given type. E.g. <code>"42".cast(Types.INT)</code> leads to 42.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
ARRAY.at(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at a particular position in an array. The index starts at 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
array(ANY [, ANY ]*)
{% endhighlight %}
      </td>
      <td>
        <p>Creates an array from a list of values. The array will be an array of objects (not primitives).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.rows
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of rows.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
STRING.toDate
{% endhighlight %}
      </td>
      <td>
        <p>Parses a date string in the form "yy-mm-dd" to a SQL date.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.toTime
{% endhighlight %}
      </td>
      <td>
        <p>Parses a time string in the form "hh:mm:ss" to a SQL time.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
STRING.toTimestamp
{% endhighlight %}
      </td>
      <td>
        <p>Parses a timestamp string in the form "yy-mm-dd hh:mm:ss.fff" to a SQL timestamp.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.year
NUMERIC.years
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for a given number of years.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.month
NUMERIC.months
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of months for a given number of months.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.day
NUMERIC.days
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of days.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.hour
NUMERIC.hours
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of hours.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.minute
NUMERIC.minutes
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of minutes.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.second
NUMERIC.seconds
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds for a given number of seconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.milli
NUMERIC.millis
{% endhighlight %}
      </td>
      <td>
        <p>Creates an interval of milliseconds.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentDate()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
currentTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
localTime()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
localTimestamp()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TEMPORAL.extract(TimeIntervalUnit)
{% endhighlight %}
      </td>
      <td>
        <p>Extracts parts of a time point or time interval. Returns the part as a long value. E.g. <code>"2006-06-05".toDate.extract(TimeIntervalUnit.DAY)</code> leads to 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TIMEPOINT.floor(TimeIntervalUnit)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point down to the given unit. E.g. <code>"12:44:31".toTime.floor(TimeIntervalUnit.MINUTE)</code> leads to 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
TIMEPOINT.ceil(TimeIntervalUnit)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point up to the given unit. E.g. <code>"12:44:31".toTime.floor(TimeIntervalUnit.MINUTE)</code> leads to 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
DATE.quarter()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the quarter of a year from a SQL date. E.g. <code>"1994-09-27".toDate.quarter()</code> leads to 3.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
temporalOverlaps(TIMEPOINT, TEMPORAL, TIMEPOINT, TEMPORAL)
{% endhighlight %}
      </td>
      <td>
        <p>Determines whether two anchored time intervals overlap. Time point and temporal are transformed into a range defined by two time points (start, end). The function evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>. E.g. <code>temporalOverlaps('2:55:00'.toTime, 1.hour, '3:30:00'.toTime, 2.hours)</code> leads to true.</p>
      </td>
    </tr>
    
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

   <tr>
      <td>
        {% highlight scala %}
FIELD.count
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows for which the field is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.avg
{% endhighlight %}
      </td>
      <td>
        <p>Returns the average (arithmetic mean) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.sum
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.max
{% endhighlight %}
      </td>
      <td>
        <p>Returns the maximum value of field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.min
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minimum value of field across all input values.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
COMPOSITE.get(STRING)
COMPOSITE.get(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index or name and returns it's value. E.g. <code>'pojo.get("myField")</code> or <code>'tuple.get(0)</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ANY.flatten()
{% endhighlight %}
      </td>
      <td>
        <p>Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes into a flat representation where every subtype is a separate field. In most cases the fields of the flat representation are named similarly to the original fields but with a dollar separator (e.g. <code>mypojo$mytuple$f0</code>).</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Array functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight scala %}
ARRAY.cardinality()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements of an array.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
ARRAY.element()
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of an array with a single element. Returns <code>null</code> if the array is empty. Throws an exception if the array has more than one element.</p>
      </td>
    </tr>

  </tbody>
</table>



<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight scala %}
ANY.as(name [, name ]* )
{% endhighlight %}
      </td>
      <td>
        <p>Specifies a name for an expression i.e. a field. Additional names can be specified if the expression expands to multiple fields.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>

<div data-lang="SQL" markdown="1">


<!--
This list of SQL functions should be kept in sync with SqlExpressionTest to reduce confusion due to the large amount of SQL functions.
The documentation is split up and ordered like the tests in SqlExpressionTest.
-->

The Flink SQL functions (including their syntax) are a subset of Apache Calcite's built-in functions. Most of the documentation has been adopted from the [Calcite SQL reference](https://calcite.apache.org/docs/reference.html).


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Comparison functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
value1 = value2
{% endhighlight %}
      </td>
      <td>
        <p>Equals.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 <> value2
{% endhighlight %}
      </td>
      <td>
        <p>Not equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 > value2
{% endhighlight %}
      </td>
      <td>
        <p>Greater than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 >= value2
{% endhighlight %}
      </td>
      <td>
        <p>Greater than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 < value2
{% endhighlight %}
      </td>
      <td>
        <p>Less than.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 <= value2
{% endhighlight %}
      </td>
      <td>
        <p>Less than or equal.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value IS NULL
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value IS NOT NULL
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 IS DISTINCT FROM value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two values are not equal, treating null values as the same.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 IS NOT DISTINCT FROM value2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if two values are equal, treating null values as the same.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 BETWEEN [ASYMMETRIC | SYMMETRIC] value2 AND value3
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is greater than or equal to <i>value2</i> and less than or equal to <i>value3</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value1 NOT BETWEEN value2 AND value3
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value1</i> is less than <i>value2</i> or greater than <i>value3</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 LIKE string2 [ ESCAPE string3 ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> matches pattern <i>string2</i>. An escape character can be defined if necessary.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 NOT LIKE string2 [ ESCAPE string3 ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> does not match pattern <i>string2</i>. An escape character can be defined if necessary.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
string1 SIMILAR TO string2 [ ESCAPE string3 ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> matches regular expression <i>string2</i>. An escape character can be defined if necessary.</p>
      </td>
    </tr>


    <tr>
      <td>
        {% highlight text %}
string1 NOT SIMILAR TO string2 [ ESCAPE string3 ]
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>string1</i> does not match regular expression <i>string2</i>. An escape character can be defined if necessary.</p>
      </td>
    </tr>


    <tr>
      <td>
        {% highlight text %}
value IN (value [, value]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is equal to a value in a list.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value NOT IN (value [, value]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is not equal to every value in a list.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXISTS (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>sub-query</i> returns at least one row. Only supported if the operation can be rewritten in a join and group operation.</p>
      </td>
    </tr>

<!-- NOT SUPPORTED SO FAR
    <tr>
      <td>
        {% highlight text %}
value IN (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is equal to a row returned by sub-query.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value NOT IN (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is not equal to every row returned by sub-query.</p>
      </td>
    </tr>
    -->

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Logical functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
boolean1 OR boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean1</i> is TRUE or <i>boolean2</i> is TRUE. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean1 AND boolean2
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean1</i> and <i>boolean2</i> are both TRUE. Supports three-valued logic.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
NOT boolean
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is not TRUE; returns UNKNOWN if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS FALSE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is FALSE; returns FALSE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT FALSE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is not FALSE; returns TRUE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS TRUE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is TRUE; returns FALSE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT TRUE
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is not TRUE; returns TRUE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS UNKNOWN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is UNKNOWN.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
boolean IS NOT UNKNOWN
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>boolean</i> is not UNKNOWN.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Arithmetic functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
+ numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
- numeric
{% endhighlight %}
      </td>
      <td>
        <p>Returns negative <i>numeric</i>.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
numeric1 + numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> plus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 - numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> minus <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 * numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> multiplied by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
numeric1 / numeric2
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> divided by <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
POWER(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns <i>numeric1</i> raised to the power of <i>numeric2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ABS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the absolute value of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MOD(numeric1, numeric2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the remainder (modulus) of <i>numeric1</i> divided by <i>numeric2</i>. The result is negative only if <i>numeric1</i> is negative.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SQRT(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the square root of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the natural logarithm (base e) of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOG10(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 10 logarithm of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXP(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns e raised to the power of <i>numeric</i>.</p>
      </td>
    </tr>   

    <tr>
      <td>
        {% highlight text %}
CEIL(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>numeric</i> up, and returns the smallest number that is greater than or equal to <i>numeric</i>.</p>
      </td>
    </tr>  

    <tr>
      <td>
        {% highlight text %}
FLOOR(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds <i>numeric</i> down, and returns the largest number that is less than or equal to <i>numeric</i>.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">String functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
string || string
{% endhighlight %}
      </td>
      <td>
        <p>Concatenates two character strings.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CHAR_LENGTH(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of characters in a character string.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CHARACTER_LENGTH(string)
{% endhighlight %}
      </td>
      <td>
        <p>As CHAR_LENGTH(<i>string</i>).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
UPPER(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a character string converted to upper case.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOWER(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a character string converted to lower case.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
POSITION(string1 IN string2)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the position of the first occurrence of <i>string1</i> in <i>string2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2)
{% endhighlight %}
      </td>
      <td>
        <p>Removes leading and/or trailing characters from <i>string2</i>. By default, whitespaces at both sides are removed.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ])
{% endhighlight %}
      </td>
      <td>
        <p>Replaces a substring of <i>string1</i> with <i>string2</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SUBSTRING(string FROM integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of a character string starting at a given point.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SUBSTRING(string FROM integer FOR integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a substring of a character string starting at a given point with a given length.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
INITCAP(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns string with the first letter of each word converter to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Conditional functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CASE value
WHEN value1 [, value11 ]* THEN result1
[ WHEN valueN [, valueN1 ]* THEN resultN ]*
[ ELSE resultZ ]
END
{% endhighlight %}
      </td>
      <td>
        <p>Simple case.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CASE
WHEN condition1 THEN result1
[ WHEN conditionN THEN resultN ]*
[ ELSE resultZ ]
END
{% endhighlight %}
      </td>
      <td>
        <p>Searched case.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
NULLIF(value, value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns NULL if the values are the same. For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COALESCE(value, value [, value ]* )
{% endhighlight %}
      </td>
      <td>
        <p>Provides a value if the first value is null. For example, <code>COALESCE(NULL, 5)</code> returns 5.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Type conversion functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CAST(value AS type)
{% endhighlight %}
      </td>
      <td>
        <p>Converts a value to a given type.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
  <!-- Disabled temporarily in favor of composite type support
    <tr>
      <td>
        {% highlight text %}
ROW (value [, value]* )
{% endhighlight %}
      </td>
      <td>
        <p>Creates a row from a list of values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
(value [, value]* )
{% endhighlight %}
      </td>
      <td>
        <p>Creates a row from a list of values.</p>
      </td>
    </tr>
-->

    <tr>
      <td>
        {% highlight text %}
array ‘[’ index ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns the element at a particular position in an array. The index starts at 1.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ARRAY ‘[’ value [, value ]* ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Creates an array from a list of values.</p>
      </td>
    </tr>

  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Temporal functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
DATE string
{% endhighlight %}
      </td>
      <td>
        <p>Parses a date string in the form "yy-mm-dd" to a SQL date.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIME string
{% endhighlight %}
      </td>
      <td>
        <p>Parses a time <i>string</i> in the form "hh:mm:ss" to a SQL time.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIMESTAMP string
{% endhighlight %}
      </td>
      <td>
        <p>Parses a timestamp <i>string</i> in the form "yy-mm-dd hh:mm:ss.fff" to a SQL timestamp.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
INTERVAL string range
{% endhighlight %}
      </td>
      <td>
        <p>Parses an interval <i>string</i> in the form "dd hh:mm:ss.fff" for SQL intervals of milliseconds or "yyyy-mm" for SQL intervals of months. An interval range might be e.g. <code>DAY</code>, <code>MINUTE</code>, <code>DAY TO HOUR</code>, or <code>DAY TO SECOND</code> for intervals of milliseconds; <code>YEAR</code> or <code>YEAR TO MONTH</code> for intervals of months. E.g. <code>INTERVAL '10 00:00:00.004' DAY TO SECOND</code>, <code>INTERVAL '10' DAY</code>, or <code>INTERVAL '2-10' YEAR TO MONTH</code> return intervals.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_DATE
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_TIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CURRENT_TIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOCALTIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
LOCALTIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
EXTRACT(timeintervalunit FROM temporal)
{% endhighlight %}
      </td>
      <td>
        <p>Extracts parts of a time point or time interval. Returns the part as a long value. E.g. <code>EXTRACT(DAY FROM DATE '2006-06-05')</code> leads to 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
FLOOR(timepoint TO timeintervalunit)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point down to the given unit. E.g. <code>FLOOR(TIME '12:44:31' TO MINUTE)</code> leads to 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CEIL(timepoint TO timeintervalunit)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point up to the given unit. E.g. <code>CEIL(TIME '12:44:31' TO MINUTE)</code> leads to 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
QUARTER(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the quarter of a year from a SQL date. E.g. <code>QUARTER(DATE '1994-09-27')</code> leads to 3.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
(timepoint, temporal) OVERLAPS (timepoint, temporal)
{% endhighlight %}
      </td>
      <td>
        <p>Determines whether two anchored time intervals overlap. Time point and temporal are transformed into a range defined by two time points (start, end). The function evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>. E.g. <code>(TIME '2:55:00', INTERVAL '1' HOUR) OVERLAPS (TIME '3:30:00', INTERVAL '2' HOUR)</code> leads to true; <code>(TIME '9:00:00', TIME '10:00:00') OVERLAPS (TIME '10:15:00', INTERVAL '3' HOUR)</code> leads to false.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Aggregate functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
COUNT(value [, value]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows for which <i>value</i> is not null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COUNT(*)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of input rows.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
AVG(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the average (arithmetic mean) of <i>numeric</i> across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SUM(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of <i>numeric</i> across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MAX(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the maximum value of <i>value</i> across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MIN(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minimum value of <i>value</i> across all input values.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Grouping functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
GROUP_ID()
{% endhighlight %}
      </td>
      <td>
        <p>Returns an integer that uniquely identifies the combination of grouping keys.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
GROUPING(expression)
{% endhighlight %}
      </td>
      <td>
        <p>Returns 1 if <i>expression</i> is rolled up in the current row’s grouping set, 0 otherwise.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
GROUPING_ID(expression [, expression]* )
{% endhighlight %}
      </td>
      <td>
        <p>Returns a bit vector of the given grouping expressions.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Value access functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
tableName.compositeType.field
{% endhighlight %}
      </td>
      <td>
        <p>Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and returns it's value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
tableName.compositeType.*
{% endhighlight %}
      </td>
      <td>
        <p>Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes into a flat representation where every subtype is a separate field.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Array functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
CARDINALITY(ARRAY)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of elements of an array.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ELEMENT(ARRAY)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sole element of an array with a single element. Returns <code>null</code> if the array is empty. Throws an exception if the array has more than one element.</p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

{% top %}

User-defined Functions
----------------

### User-defined Scalar Functions

If a required scalar function is not contained in the built-in functions, it is possible to define custom, user-defined scalar functions for both the Table API and SQL. A user-defined scalar functions maps zero, one, or multiple scalar values to a new scalar value.

In order to define a scalar function one has to extend the base class `ScalarFunction` in `org.apache.flink.table.functions` and implement (one or more) evaluation methods. The behavior of a scalar function is determined by the evaluation method. An evaluation method must be declared publicly and named `eval`. The parameter types and return type of the evaluation method also determine the parameter and return types of the scalar function. Evaluation methods can also be overloaded by implementing multiple methods named `eval`.

The following example shows how to define your own hash code function, register it in the TableEnvironment, and call it in a query. Note that you can configure your scalar function via a constructor before it is registered:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class HashCode extends ScalarFunction {
  private int factor = 12;
  
  public HashCode(int factor) {
      this.factor = factor;
  }
  
  public int eval(String s) {
      return s.hashCode() * factor;
  }
}

BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the function
tableEnv.registerFunction("hashCode", new HashCode(10))

// use the function in Java Table API
myTable.select("string, string.hashCode(), hashCode(string)");

// use the function in SQL API
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// must be defined in static/object context
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * factor
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)

// use the function in Scala Table API
val hashCode = new HashCode(10)
myTable.select('string, hashCode('string))

// register and use the function in SQL
tableEnv.registerFunction("hashCode", new HashCode(10))
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>
</div>

By default the result type of an evaluation method is determined by Flink's type extraction facilities. This is sufficient for basic types or simple POJOs but might be wrong for more complex, custom, or composite types. In these cases `TypeInformation` of the result type can be manually defined by overriding `ScalarFunction#getResultType()`.

Internally, the Table API and SQL code generation works with primitive values as much as possible. If a user-defined scalar function should not introduce much overhead through object creation/casting during runtime, it is recommended to declare parameters and result types as primitive types instead of their boxed classes. `Types.DATE` and `Types.TIME` can also be represented as `int`. `Types.TIMESTAMP` can be represented as `long`.

The following example shows an advanced example which takes the internal timestamp representation and also returns the internal timestamp representation as a long value. By overriding `ScalarFunction#getResultType()` we define that the returned long value should be interpreted as a `Types.TIMESTAMP` by the code generation.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class TimestampModifier extends ScalarFunction {
  public long eval(long t) {
    return t % 1000;
  }

  public TypeInformation<?> getResultType(signature: Class<?>[]) {
    return Types.TIMESTAMP;
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
object TimestampModifier extends ScalarFunction {
  def eval(t: Long): Long = {
    t % 1000
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.TIMESTAMP
  }
}
{% endhighlight %}
</div>
</div>

### User-defined Table Functions

Similar to a user-defined scalar function, a user-defined table function takes zero, one, or multiple scalar values as input parameters. However in contrast to a scalar function, it can return an arbitrary number of rows as output instead of a single value. The returned rows may consist of one or more columns. 

In order to define a table function one has to extend the base class `TableFunction` in `org.apache.flink.table.functions` and implement (one or more) evaluation methods. The behavior of a table function is determined by its evaluation methods. An evaluation method must be declared `public` and named `eval`. The `TableFunction` can be overloaded by implementing multiple methods named `eval`. The parameter types of the evaluation methods determine all valid parameters of the table function. The type of the returned table is determined by the generic type of `TableFunction`. Evaluation methods emit output rows using the protected `collect(T)` method.

In the Table API, a table function is used with `.join(Expression)` or `.leftOuterJoin(Expression)` for Scala users and `.join(String)` or `.leftOuterJoin(String)` for Java users. The `join` operator (cross) joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator). The `leftOuterJoin` operator joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator) and preserves outer rows for which the table function returns an empty table. In SQL use `LATERAL TABLE(<TableFunction>)` with CROSS JOIN and LEFT JOIN with an ON TRUE join condition (see examples below).

The following example shows how to define table-valued function, register it in the TableEnvironment, and call it in a query. Note that you can configure your table function via a constructor before it is registered: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public Split(String separator) {
        this.separator = separator;
    }
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}

BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
Table myTable = ...         // table schema: [a: String]

// Register the function.
tableEnv.registerFunction("split", new Split("#"));

// Use the table function in the Java Table API. "as" specifies the field names of the table.
myTable.join("split(a) as (word, length)").select("a, word, length");
myTable.leftOuterJoin("split(a) as (word, length)").select("a, word, length");

// Use the table function in SQL with LATERAL and TABLE keywords.
// CROSS JOIN a table function (equivalent to "join" in Table API).
tableEnv.sql("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)");
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API).
tableEnv.sql("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// The generic type "(String, Int)" determines the schema of the returned table as (String, Integer).
class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    // use collect(...) to emit a row.
    str.split(separator).foreach(x -> collect((x, x.length))
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)
val myTable = ...         // table schema: [a: String]

// Use the table function in the Scala Table API (Note: No registration required in Scala Table API).
val split = new Split("#")
// "as" specifies the field names of the generated table.
myTable.join(split('a) as ('word, 'length)).select('a, 'word, 'length);
myTable.leftOuterJoin(split('a) as ('word, 'length)).select('a, 'word, 'length);

// Register the table function to use it in SQL queries.
tableEnv.registerFunction("split", new Split("#"))

// Use the table function in SQL with LATERAL and TABLE keywords.
// CROSS JOIN a table function (equivalent to "join" in Table API)
tableEnv.sql("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)");
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API)
tableEnv.sql("SELECT a, word, length FROM MyTable LEFT JOIN TABLE(split(a)) as T(word, length) ON TRUE");
{% endhighlight %}
**IMPORTANT:** Do not implement TableFunction as a Scala object. Scala object is a singleton and will cause concurrency issues.
</div>
</div>

Please note that POJO types do not have a deterministic field order. Therefore, you cannot rename the fields of POJO returned by a table function using `AS`.

By default the result type of a `TableFunction` is determined by Flink’s automatic type extraction facilities. This works well for basic types and simple POJOs but might be wrong for more complex, custom, or composite types. In such a case, the type of the result can be manually specified by overriding `TableFunction#getResultType()` which returns its `TypeInformation`.

The following example shows an example of a `TableFunction` that returns a `Row` type which requires explicit type information. We define that the returned table type should be `RowTypeInfo(String, Integer)` by overriding `TableFunction#getResultType()`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class CustomTypeSplit extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(" ")) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length);
            collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(new TypeInformation[]{
               			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO});
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class CustomTypeSplit extends TableFunction[Row] {
  def eval(str: String): Unit = {
    str.split(" ").foreach({ s =>
      val row = new Row(2)
      row.setField(0, s)
      row.setField(1, s.length)
      collect(row)
    })
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Seq(BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO))
  }
}
{% endhighlight %}
</div>
</div>

### Advanced Function Features

Sometimes it might be necessary for a user-defined function to get global runtime information or do some setup/clean-up work before the actual work. User-defined functions provide `open()` and `close()` methods that can be overriden and provide similar functionality as the methods in `RichFunction` of DataSet or DataStream API.

The `open()` method is called once before the evaluation method. The `close()` method after the last call to the evaluation method.

The `open()` method provides a `FunctionContext` that contains information about the context in which user-defined functions are executed, such as the metric group, the distributed cache files, or the global job parameters.

The following information can be obtained by calling the corresponding methods of `FunctionContext`:

| Method                                | Description                                            |
| :------------------------------------ | :----------------------------------------------------- |
| `getMetricGroup()`                    | Metric group for this parallel subtask.                |
| `getCachedFile(name)`                 | Local temporary file copy of a distributed cache file. |
| `getJobParameter(name, defaultValue)` | Global job parameter value associated with given key.  |

The following example snippet shows how to use `FunctionContext` in a scalar function for accessing a global job parameter:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class HashCode extends ScalarFunction {

    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // access "hashcode_factor" parameter
        // "12" would be the default value if parameter does not exist
        factor = Integer.valueOf(context.getJobParameter("hashcode_factor", "12")); 
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// set job parameter
Configuration conf = new Configuration();
conf.setString("hashcode_factor", "31");
env.getConfig().setGlobalJobParameters(conf);

// register the function
tableEnv.registerFunction("hashCode", new HashCode())

// use the function in Java Table API
myTable.select("string, string.hashCode(), hashCode(string)");

// use the function in SQL
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
object hashCode extends ScalarFunction {

  var hashcode_factor = 12;

  override def open(context: FunctionContext): Unit = {
    // access "hashcode_factor" parameter
    // "12" would be the default value if parameter does not exist
    hashcode_factor = context.getJobParameter("hashcode_factor", "12").toInt
  }

  def eval(s: String): Int = {
    s.hashCode() * hashcode_factor
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)

// use the function in Scala Table API
myTable.select('string, hashCode('string))

// register and use the function in SQL
tableEnv.registerFunction("hashCode", hashCode)
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}

</div>
</div>


### Limitations

The following operations are not supported yet:

- Binary string operators and functions
- System functions
- Collection functions
- Aggregate functions like STDDEV_xxx, VAR_xxx, and REGR_xxx
- Distinct aggregate functions like COUNT DISTINCT
- Row windows



{% top %}

Writing Tables to External Sinks
--------------------------------

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

Runtime Configuration
----
The Table API provides a configuration (the so-called `TableConfig`) to modify runtime behavior. It can be accessed through the `TableEnvironment`.

### Null Handling
By default, the Table API supports `null` values. Null handling can be disabled to improve preformance by setting the `nullCheck` property in the `TableConfig` to `false`.

{% top %}

Explaining a Table
----
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



