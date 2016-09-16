---
title: "Table and SQL"
is_beta: true
nav-parent_id: apis
nav-pos: 3
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

*Note: The Table API is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/dev/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).*


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

It can be configured with the following properties:

 - `path` The path to the CSV file, required.
 - `fieldNames` The names of the table fields, required.
 - `fieldTypes` The types of the table fields, required.
 - `fieldDelim` The field delimiter, `","` by default.
 - `rowDelim` The row delimiter, `"\n"` by default.
 - `quoteCharacter` An optional quote character for String values, `null` by default.
 - `ignoreFirstLine` Flag to ignore the first line, `false` by default.
 - `ignoreComments` An optional prefix to indicate comments, `null` by default.
 - `lenient` Flag to skip records with parse error instead to fail, `false` by default.

You can create the source as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
CsvTableSource csvTableSource = new CsvTableSource(
    "/path/to/your/file.csv",
    new String[] { "name", "id", "score", "comments" },
    new TypeInformation<?>[] {
      Types.STRING(),
      Types.INT(),
      Types.DOUBLE(),
      Types.STRING()
    },
    "#",    // fieldDelim
    "$",    // rowDelim
    null,   // quoteCharacter
    true,   // ignoreFirstLine
    "%",    // ignoreComments
    false); // lenient
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val csvTableSource = new CsvTableSource(
    "/path/to/your/file.csv",
    Array("name", "id", "score", "comments"),
    Array(
      Types.STRING,
      Types.INT,
      Types.DOUBLE,
      Types.STRING
    ),
    fieldDelim = "#",
    rowDelim = "$",
    ignoreFirstLine = true,
    ignoreComments = "%")
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


Table API
----------
The Table API provides methods to apply relational operations on DataSets and Datastreams both in Scala and Java.

The central concept of the Table API is a `Table` which represents a table with relational schema (or relation). Tables can be created from a `DataSet` or `DataStream`, converted into a `DataSet` or `DataStream`, or registered in a table catalog using a `TableEnvironment`. A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine Tables of different TableEnvironments.

*Note: The only operations currently supported on streaming Tables are selection, projection, and union.*

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
The Table API is enabled by importing `org.apache.flink.api.scala.table._`. This enables
implicit conversions to convert a `DataSet` or `DataStream` to a Table. The following example shows:

- how a `DataSet` is converted to a `Table`,
- how relational queries are specified, and
- how a `Table` is converted back to a `DataSet`.

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

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
import org.apache.flink.api.scala.table._

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
This section gives a brief overview of the available operators. You can find more details of operators in the [Javadoc]({{site.baseurl}}/api/java/org/apache/flink/api/table/Table.html).

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

comparison = term , [ ( "=" | "===" | "!=" | "!==" | ">" | ">=" | "<" | "<=" ) , term ] ;

term = product , [ ( "+" | "-" ) , product ] ;

product = unary , [ ( "*" | "/" | "%") , unary ] ;

unary = [ "!" | "-" ] , composite ;

composite = suffixed | atom ;

suffixed = interval | cast | as | aggregation | if | functionCall ;

interval = composite , "." , ("year" | "month" | "day" | "hour" | "minute" | "second" | "milli") ;

cast = composite , ".cast(" , dataType , ")" ;

dataType = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOLEAN" | "STRING" | "DECIMAL" | "DATE" | "TIME" | "TIMESTAMP" | "INTERVAL_MONTHS" | "INTERVAL_MILLIS" ;

as = composite , ".as(" , fieldReference , ")" ;

aggregation = composite , ( ".sum" | ".min" | ".max" | ".count" | ".avg" ) , [ "()" ] ;

if = composite , ".?(" , expression , "," , expression , ")" ;

functionCall = composite , "." , functionIdentifier , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

atom = ( "(" , expression , ")" ) | literal | nullLiteral | fieldReference ;

fieldReference = "*" | identifier ;

nullLiteral = "Null(" , dataType , ")" ;

timeIntervalUnit = "YEAR" | "YEAR_TO_MONTH" | "MONTH" | "DAY" | "DAY_TO_HOUR" | "DAY_TO_MINUTE" | "DAY_TO_SECOND" | "HOUR" | "HOUR_TO_MINUTE" | "HOUR_TO_SECOND" | "MINUTE" | "MINUTE_TO_SECOND" | "SECOND" ;

timePointUnit = "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "WEEK" | "MILLISECOND" | "MICROSECOND" ;

{% endhighlight %}

Here, `literal` is a valid Java literal, `fieldReference` specifies a column in the data (or all columns if `*` is used), and `functionIdentifier` specifies a supported scalar function. The
column names and function names follow Java identifier syntax. Expressions specified as Strings can also use prefix notation instead of suffix notation to call operators and functions.

If working with exact numeric values or large decimals is required, the Table API also supports Java's BigDecimal type. In the Scala Table API decimals can be defined by `BigDecimal("123456")` and in Java by appending a "p" for precise e.g. `123456p`.

In order to work with temporal values the Table API supports Java SQL's Date, Time, and Timestamp types. In the Scala Table API literals can be defined by using `java.sql.Date.valueOf("2016-06-27")`, `java.sql.Time.valueOf("10:10:42")`, or `java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")`. The Java and Scala Table API also support calling `"2016-06-27".toDate()`, `"10:10:42".toTime()`, and `"2016-06-27 10:10:42.123".toTimestamp()` for converting Strings into temporal types. *Note:* Since Java's temporal SQL types are time zone dependent, please make sure that the Flink Client and all TaskManagers use the same time zone.

Temporal intervals can be represented as number of months (`Types.INTERVAL_MONTHS`) or number of milliseconds (`Types.INTERVAL_MILLIS`). Intervals of same type can be added or subtracted (e.g. `2.hour + 10.minutes`). Intervals of milliseconds can be added to time points (e.g. `"2016-08-10".toDate + 5.day`).

{% top %}


SQL
----
SQL queries are specified using the `sql()` method of the `TableEnvironment`. The method returns the result of the SQL query as a `Table` which can be converted into a `DataSet` or `DataStream`, used in subsequent Table API queries, or written to a `TableSink` (see [Writing Tables to External Sinks](#writing-tables-to-external-sinks)). SQL and Table API queries can seamlessly mixed and are holistically optimized and translated into a single DataStream or DataSet program.

A `Table`, `DataSet`, `DataStream`, or external `TableSource` must be registered in the `TableEnvironment` in order to be accessible by a SQL query (see [Registering Tables](#registering-tables)).

*Note: Flink's SQL support is not feature complete, yet. Queries that include unsupported SQL features will cause a `TableException`. The limitations of SQL on batch and streaming tables are listed in the following sections.*

### SQL on Batch Tables

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// read a DataSet from an external source
DataSet<Tuple3<Long, String, Integer>> ds = env.readCsvFile(...);
// register the DataSet as table "Orders"
tableEnv.registerDataSet("Orders", ds, "user, product, amount");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sql(
  "SELECT SUM(amount) FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataSet from an external source
val ds: DataSet[(Long, String, Integer)] = env.readCsvFile(...)
// register the DataSet under the name "Orders"
tableEnv.registerDataSet("Orders", ds, 'user, 'product, 'amount)
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sql(
  "SELECT SUM(amount) FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

#### Limitations

The current version supports selection (filter), projection, inner equi-joins, grouping, non-distinct aggregates, and sorting on batch tables.

Among others, the following SQL features are not supported, yet:

- Timestamps and intervals are limited to milliseconds precision
- Interval arithmetic is currenly limited
- Distinct aggregates (e.g., `COUNT(DISTINCT name)`)
- Non-equi joins and Cartesian products
- Grouping sets

*Note: Tables are joined in the order in which they are specified in the `FROM` clause. In some cases the table order must be manually tweaked to resolve Cartesian products.*

### SQL on Streaming Tables

SQL queries can be executed on streaming Tables (Tables backed by `DataStream` or `StreamTableSource`) by using the `SELECT STREAM` keywords instead of `SELECT`. Please refer to the [Apache Calcite's Streaming SQL documentation](https://calcite.apache.org/docs/stream.html) for more information on the Streaming SQL syntax.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);
// register the DataStream as table "Orders"
tableEnv.registerDataStream("Orders", ds, "user, product, amount");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sql(
  "SELECT STREAM product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(Long, String, Integer)] = env.addSource(...)
// register the DataStream under the name "Orders"
tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount)
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sql(
  "SELECT STREAM product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

#### Limitations

The current version of streaming SQL only supports `SELECT`, `FROM`, `WHERE`, and `UNION` clauses. Aggregations or joins are not supported yet.

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
  SELECT [ STREAM ] [ ALL | DISTINCT ]
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

values:
  VALUES expression [, expression ]*

groupItem:
  expression
  | '(' ')'
  | '(' expression [, expression ]* ')'

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

The Table API is built on top of Flink's DataSet and DataStream API. Internally, it also uses Flink's `TypeInformation` to distinguish between types. The Table API does not support all Flink types so far. All supported simple types are listed in `org.apache.flink.api.table.Types`. The following table summarizes the relation between Table API types, SQL types, and the resulting Java class.

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

Advanced types such as generic types, composite types (e.g. POJOs or Tuples), and arrays can be fields of a row but can not be accessed yet. They are treated like a black box within Table API and SQL.

{% top %}

Scalar Functions
----------------

Both the Table API and SQL come with a set of built-in scalar functions for data transformations. This section gives a brief overview of the available scalar function so far.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br/>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Function</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
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
NUMERIC.power(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the given number raised to the power of the other value.</p>
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
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the largest integer less than or equal to a given number.</p>
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
STRING.initCap()
{% endhighlight %}
      </td>

      <td>
        <p>Converts the initial letter of each word in a string to uppercase. Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.</p>
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

  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Function</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
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
NUMERIC.power(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the given number raised to the power of the other value.</p>
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
NUMERIC.floor()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the largest integer less than or equal to a given number.</p>
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
STRING.initCap()
{% endhighlight %}
      </td>

      <td>
        <p>Converts the initial letter of each word in a string to uppercase. Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.</p>
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

  </tbody>
</table>
</div>

<div data-lang="SQL" markdown="1">
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Function</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight sql %}
EXP(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the Euler's number raised to the given power.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
LOG10(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the base 10 logarithm of given value.</p>
      </td>
    </tr>


    <tr>
      <td>
        {% highlight sql %}
LN(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the natural logarithm of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
POWER(NUMERIC, NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the given number raised to the power of the other value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
ABS(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the absolute value of given value.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
FLOOR(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the largest integer less than or equal to a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CEIL(NUMERIC)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the smallest integer greater than or equal to a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
SUBSTRING(VARCHAR, INT, INT)
SUBSTRING(VARCHAR FROM INT FOR INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string at the given index for the given length. The index starts at 1 and is inclusive, i.e., the character at the index is included in the substring. The substring has the specified length or less.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
SUBSTRING(VARCHAR, INT)
SUBSTRING(VARCHAR FROM INT)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a substring of the given string beginning at the given index to the end. The start index starts at 1 and is inclusive.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
TRIM(LEADING VARCHAR FROM VARCHAR)
TRIM(TRAILING VARCHAR FROM VARCHAR)
TRIM(BOTH VARCHAR FROM VARCHAR)
TRIM(VARCHAR)
{% endhighlight %}
      </td>
      <td>
        <p>Removes leading and/or trailing characters from the given string. By default, whitespaces at both sides are removed.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CHAR_LENGTH(VARCHAR)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the length of a String.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
UPPER(VARCHAR)
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in upper case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
LOWER(VARCHAR)
{% endhighlight %}
      </td>
      <td>
        <p>Returns all of the characters in a string in lower case using the rules of the default locale.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
INITCAP(VARCHAR)
{% endhighlight %}
      </td>

      <td>
        <p>Converts the initial letter of each word in a string to uppercase. Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
VARCHAR LIKE VARCHAR
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified LIKE pattern. E.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
VARCHAR SIMILAR TO VARCHAR
{% endhighlight %}
      </td>
      <td>
        <p>Returns true, if a string matches the specified SQL regex pattern. E.g. "A+" matches all strings that consist of at least one "A".</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
DATE VARCHAR
{% endhighlight %}
      </td>
      <td>
        <p>Parses a date string in the form "yy-mm-dd" to a SQL date.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
TIME VARCHAR
{% endhighlight %}
      </td>
      <td>
        <p>Parses a time string in the form "hh:mm:ss" to a SQL time.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
TIMESTAMP VARCHAR
{% endhighlight %}
      </td>
      <td>
        <p>Parses a timestamp string in the form "yy-mm-dd hh:mm:ss.fff" to a SQL timestamp.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
EXTRACT(TIMEINTERVALUNIT FROM TEMPORAL)
{% endhighlight %}
      </td>
      <td>
        <p>Extracts parts of a time point or time interval. Returns the part as a long value. E.g. <code>EXTRACT(DAY FROM DATE '2006-06-05')</code> leads to 5.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
FLOOR(TIMEPOINT TO TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point down to the given unit. E.g. <code>FLOOR(TIME '12:44:31' TO MINUTE)</code> leads to 12:44:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CEIL(TIMEPOINT TO TIMEINTERVALUNIT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds a time point up to the given unit. E.g. <code>CEIL(TIME '12:44:31' TO MINUTE)</code> leads to 12:45:00.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CURRENT_DATE
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL date in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CURRENT_TIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
CURRENT_TIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in UTC time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
LOCALTIME
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL time in local time zone.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight sql %}
LOCALTIMESTAMP
{% endhighlight %}
      </td>
      <td>
        <p>Returns the current SQL timestamp in local time zone.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

### User-defined Scalar Functions

If a required scalar function is not contained in the built-in functions, it is possible to define custom, user-defined scalar functions for both the Table API and SQL. A user-defined scalar functions maps zero, one, or multiple scalar values to a new scalar value.

In order to define a scalar function one has to extend the base class `ScalarFunction` in `org.apache.flink.api.table.functions` and implement (one or more) evaluation methods. The behavior of a scalar function is determined by the evaluation method. An evaluation method must be declared publicly and named `eval`. The parameter types and return type of the evaluation method also determine the parameter and return types of the scalar function. Evaluation methods can also be overloaded by implementing multiple methods named `eval`.

The following example snippet shows how to define your own hash code function:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class HashCode extends ScalarFunction {
  public int eval(String s) {
    return s.hashCode() * 12;
  }
}

BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the function
tableEnv.registerFunction("hashCode", new HashCode())

// use the function in Java Table API
myTable.select("string, string.hashCode(), hashCode(string)");

// use the function in SQL API
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// must be defined in static/object context
object hashCode extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * 12
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
The Table API provides a mechanism to describe the graph of operations that leads to the resulting output. This is done through the `TableEnvironment#explain(table)` method. It returns a string describing two graphs: the Abstract Syntax Tree of the relational algebra query and Flink's Execution Plan of the Job. 

Table `explain` is supported for both `BatchTableEnvironment` and `StreamTableEnvironment`. Currently `StreamTableEnvironment` doesn't support the explanation of the Execution Plan.

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
Table table = table1.unionAll(table2);

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
val table = table1.unionAll(table2)

val explanation: String = tEnv.explain(table)
println(explanation)
{% endhighlight %}
</div>
</div>

{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalTableScan(table=[[_DataStreamTable_0]])
  LogicalTableScan(table=[[_DataStreamTable_1]])
{% endhighlight %}

{% top %}



