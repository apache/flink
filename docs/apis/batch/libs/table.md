---
title: "Table API and SQL"
is_beta: true
# Top navigation
top-nav-group: libs
top-nav-pos: 3
top-nav-title: "Table API and SQL"
# Sub navigation
sub-nav-group: batch
sub-nav-parent: libs
sub-nav-pos: 3
sub-nav-title: Table API and SQL
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

The Table API is a SQL-like expression language that can be embedded in Flink's DataSet and DataStream APIs (Java and Scala).
A `DataSet` or `DataStream` can be converted into a relational `Table` abstraction. You can apply relational operators such as selection, aggregation, and joins on `Table`s or query them with regular SQL queries.
When a `Table` is converted back into a `DataSet` or `DataStream`, the logical plan, which was defined by relational operators and SQL queries, is optimized using [Apache Calcite](https://calcite.apache.org/)
and transformed into a `DataSet` or `DataStream` execution plan.

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

Table API
----------
The Table API provides methods to apply relational operations on DataSets and Datastreams both in Scala and Java.

The central concept of the Table API is a `Table` which represents a table with relational schema (or relation). Tables can be created from a `DataSet` or `DataStream`, converted into a `DataSet` or `DataStream`, or registered in a table catalog using a `TableEnvironment`. A `Table` is always bound to a specific `TableEnvironment`. It is not possible to combine Tables of different TableEnvironments. 

*Note that the only operations currently supported on streaming Tables are selection, filtering, and union.*

The following sections show by example how to use the Table API embedded in the Scala and Java DataSet APIs.

### Scala Table API

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

Please refer to the Scaladoc (and Javadoc) for a full list of supported operations and a description of the expression syntax.

{% top %}

### Java Table API

When using Flink's Java DataSet API, DataSets are converted to Tables and Tables to DataSets using a `TableEnvironment`.
The following example shows:

- how a `DataSet` is converted to a `Table`,
- how relational queries are specified, and 
- how a `Table` is converted back to a `DataSet`.

It is equivalent to the Scala example in the previous section.

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

  </tbody>
</table>
</div>
</div>

{% top %}

### Expression Syntax
Some of the operators in previous sections expect one or more expressions. Expressions can be specified using an embedded Scala DSL or as Strings. Please refer to the examples above to learn how expressions can be specified.

This is the complete EBNF grammar for expressions:

{% highlight ebnf %}

expression = single expression , { "," , single expression } ;

single expression = alias | logic ;

alias = logic | logic , "AS" , field reference ;

logic = comparison , [ ( "&&" | "||" ) , comparison ] ;

comparison = term , [ ( "=" | "!=" | ">" | ">=" | "<" | "<=" ) , term ] ;

term = product , [ ( "+" | "-" ) , product ] ;

unary = [ "!" | "-" ] , suffix ;

suffix = atom | aggregation | cast | as ;

aggregation = atom , [ ".sum" | ".min" | ".max" | ".count" | ".avg" ] ;

cast = atom , ".cast(" , data type , ")" ;

data type = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOL" | "BOOLEAN" | "STRING" | "DATE" ;

as = atom , ".as(" , field reference , ")" ;

atom = ( "(" , single expression , ")" ) | literal | field reference ;

{% endhighlight %}

Here, `literal` is a valid Java literal and `field reference` specifies a column in the data. The
column names follow Java identifier syntax.

Only the types `LONG` and `STRING` can be casted to `DATE` and vice versa. A `LONG` casted to `DATE` must be a milliseconds timestamp. A `STRING` casted to `DATE` must have the format "`yyyy-MM-dd HH:mm:ss.SSS`", "`yyyy-MM-dd`", "`HH:mm:ss`", or a milliseconds timestamp. By default, all timestamps refer to the UTC timezone beginning from January 1, 1970, 00:00:00 in milliseconds.

{% top %}

SQL
----
The Table API also supports embedded SQL queries.
In order to use a `Table`, `DataSet`, or `DataStream` in a SQL query, it has to be registered in the `TableEnvironment`, using a unique name.
A registered Dataset `Table` can be retrieved back from the `TableEnvironment` using the `scan()` method and a registered DataStream `Table` can be retrieved using the `ingest()` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// create a Table environment
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

DataSet<Tuple2<Integer, Long>> ds = env.readCsvFile(...);
// register the DataSet as table "MyTable"
tableEnv.registerDataSet("MyTable", ds);
// retrieve "MyTable" into a new Table
Table t = tableEnv.scan("MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
// create a Table environment
val tableEnv = TableEnvironment.getTableEnvironment(env)

val ds = env.readCsvFile(...)
// register the DataSet as table "MyTable"
tableEnv.registerDataSet("MyTable", ds)
// retrieve "MyTable" into a new Table
val t = tableEnv.scan("MyTable")
{% endhighlight %}
</div>
</div>

A DataStream `Table` can be registered in the `StreamTableEnvironment` using the correponding `registerDataStream` method.

*Note: DataSet Table names are not allowed to follow the `^_DataSetTable_[0-9]+` pattern and DataStream Tables are not allowed to follow the `^_DataStreamTable_[0-9]+` pattern, as these are reserved for internal use only.*

When registering a `DataSet` or `DataStream`, one can also specify the field names of the table:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// register the DataSet as table "Orders" with fields user, product, and amount
tableEnv.registerDataSet("Orders", ds, "user, product, amount");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// register the DataSet as table "Orders" with fields user, product, and amount
tableEnv.registerDataSet("Orders", ds, 'user, 'product, 'amount)
{% endhighlight %}
</div>
</div>

A `Table` can be registered in a similar way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table t = tableEnv.fromDataSet(ds).as("user, product, amount");
// register the Table as table "Orders"
tableEnv.registerTable("Orders", t);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val t = ds.toTable(tableEnv, 'user, 'product, 'amount)
// register the Table as table "Orders"
tableEnv.registerTable("Orders", t)
{% endhighlight %}
</div>
</div>

Registered tables can be used in SQL queries. A SQL query is defined using the `sql()` method of the `TableEnvironment`. It returns a new `Table` which can be converted back to a `DataSet`, or `DataStream`, or used in subsequent Table API queries.

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

