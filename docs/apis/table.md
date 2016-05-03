---
title: "Table API and SQL"
is_beta: true
# Top-level navigation
top-nav-group: apis
top-nav-pos: 4
top-nav-title: "Table API and SQL"
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
The Table API and SQL insterface operate on a relational `Table` abstraction, which can be created from external data sources, or existing DataSets and DataStreams. With the Table API, you can apply relational operators such as selection, aggregation, and joins on `Table`s.

`Table`s can also be queried with regular SQL, as long as they are registered (see [Registering and Accessing Tables](#registering-and-accessing-tables)). The Table API and SQL offer equivalent functionality and can be mixed in the same program. When a `Table` is converted back into a `DataSet` or `DataStream`, the logical plan, which was defined by relational operators and SQL queries, is optimized using [Apache Calcite](https://calcite.apache.org/) and transformed into a `DataSet` or `DataStream` execution plan.

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

Note that the Table API is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).


Registering Tables
--------------------------------

`TableEnvironment`s have an internal table catalog to which tables can be registered with a unique name. After registration, a table can be accessed from the `TableEnvironment` by its name. Tables can be registered in different ways.

*Note that it is not required to register a `DataSet` or `DataStream` as a table in a `TableEnvironment` in order to process it with the Table API.* 

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

*Note: DataSet table names are not allowed to follow the `^_DataSetTable_[0-9]+` pattern, as these are reserved for internal use only.*

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

*Note: DataStream table names are not allowed to follow the `^_DataStreamTable_[0-9]+` pattern, as these are reserved for internal use only.*

### Register a Table

A `Table` that originates from a Table API operation or a SQL query is registered in a `TableEnvironemnt` as follows:

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

Currently, Flink only provides a `CsvTableSource` to read CSV files. A custom `TableSource` can be defined by implementing the `BatchTableSource` or `StreamTableSource` interface. 


Table API
----------
The Table API provides methods to apply relational operations on DataSets and Datastreams both in Scala and Java.

The central concept of the Table API is a `Table` which represents a table with relational schema (or relation). Tables can be created from a `DataSet` or `DataStream`, converted into a `DataSet` or `DataStream`, or registered in a table catalog using a `TableEnvironment`. A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine Tables of different TableEnvironments. 

*Note that the only operations currently supported on streaming Tables are selection, filtering, and union.*

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

<br/>

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
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and an equality join predicate must be defined using a where or filter operator.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.join(right).where("a = d").select("a, b, e");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables. Both tables must have identical schema, i.e., field names and types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns rows with distinct value combinations.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.distinct();
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns rows globally sorted across all parallel partitions.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc");
{% endhighlight %}
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
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Unions two tables, both tables must have identical schema(field names and types).</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns rows with distinct value combinations.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.distinct();
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns rows globally sorted across all parallel partitions.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.orderBy('a.asc);
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

suffixed = cast | as | aggregation | nullCheck | evaluate | functionCall ;

cast = composite , ".cast(" , dataType , ")" ;

dataType = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOL" | "BOOLEAN" | "STRING" | "DATE" ;

as = composite , ".as(" , fieldReference , ")" ;

aggregation = composite , ( ".sum" | ".min" | ".max" | ".count" | ".avg" ) , [ "()" ] ;

nullCheck = composite , ( ".isNull" | ".isNotNull" ) , [ "()" ] ;

evaluate = composite , ".eval(" , expression , "," , expression , ")" ;

functionCall = composite , "." , functionIdentifier , "(" , [ expression , { "," , expression } ] , ")"

atom = ( "(" , expression , ")" ) | literal | nullLiteral | fieldReference ;

nullLiteral = "Null(" , dataType , ")" ;

{% endhighlight %}

Here, `literal` is a valid Java literal, `fieldReference` specifies a column in the data, and `functionIdentifier` specifies a supported scalar function. The
column names and function names follow Java identifier syntax. Expressions specified as Strings can also use prefix notation instead of suffix notation to call operators and functions. 

{% top %}


SQL
----
Registered `Table`s can be directly queried with SQL and SQL queries can also be mixed with Table API expressions. Table API and SQL statements will be translated into a single optimized DataStream or DataSet program.

*Note: The current SQL implementation is not feature complete. Outer joins, distinct aggregates, date and decimal data types are currently not supported. However, all operations supported by the Table API are also supported by SQL.*

In order to use a `Table`, `DataSet`, `DataStream`, or external `TableSource` in a SQL query, it has to be registered in the `TableEnvironment`, using a unique name. 
A registered table can be accessed from a `TableEnvironment`  using the `sql()` method of the `TableEnvironment`:

- `tEnv.sql(SELECT * FROM tName)` executes the SQL query on the corresponding tables which were registered in a `TableEnvironment`.

This method returns a new `Table` which can be converted back to a `DataSet`, or `DataStream`, or used in subsequent Table API queries.


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// read a DataSet from an external source
DataSet<Tuple2<Integer, Long>> ds = env.readCsvFile(...);
// register the DataSet as table "Orders"
tableEnv.registerDataSet("Orders", ds, "user, product, amount");
// run a SQL query and retrieve the result in a new Table
Table result = tableEnv.sql("SELECT SUM(amount) FROM Orders WHERE product = 10");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataSet from an external source
val ds = env.readCsvFile(...)
// register the DataSet under the name "Orders"
tableEnv.registerDataSet("Orders", ds, 'user, 'product, 'amount)
// run a SQL query and retrieve the result in a new Table
val result = tableEnv.sql("SELECT SUM(amount) FROM Orders WHERE product = 10")
{% endhighlight %}
</div>
</div>

SQL queries can be executed on DataStream Tables by adding the `STREAM` SQL keyword before the table name. Please refer to the [Apache Calcite SQL Streaming documentation](https://calcite.apache.org/docs/stream.html) for more information on the Streaming SQL syntax.

{% top %}

Runtime Configuration
----
The Table API provides a configuration (the so-called `TableConfig`) to modify runtime behavior. It can be accessed either through `TableEnvironment` or passed to the `toDataSet`/`toDataStream` method when using Scala implicit conversion.

### Null Handling
By default, the Table API does not support `null` values at runtime for efficiency purposes. Null handling can be enabled by setting the `nullCheck` property in the `TableConfig` to `true`.

