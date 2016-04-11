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


**The Table API: an experimental feature**

Flink's Table API is a SQL-like expression language embedded in Java and Scala.
Instead of manipulating a `DataSet` or `DataStream`, you can create and work with a relational `Table` abstraction.
Tables have a schema and allow running relational operations on them, including selection, aggregation, and joins.
A `Table` can be created from a `DataSet` or a `DataStream` and then queried either using the Table API operators or using SQL queries.
Once a `Table` is converted back to a `DataSet` or `DataStream`, the defined relational plan is optimized using [Apache Calcite](https://calcite.apache.org/)
and transformed into a `DataSet` or `DataStream` execution plan.

* This will be replaced by the TOC
{:toc}

Using the Table API and SQL
----------------------------

The Table API and SQL are part of the *flink-libraries* Maven project.
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
The Table API provides methods for running relational operations on Tables, both in Scala and Java.
In the following sections you can find examples that show how to create Tables, how to define and execute relational queries on them,
and how to retrieve the result of a query as a `DataSet`.

### Scala Table API

The Table API can be enabled by importing `org.apache.flink.api.scala.table._`. This enables
implicit conversions that allow
converting a DataSet to a Table. This example shows how a DataSet can
be converted, how relational queries can be specified and how a Table can be
converted back to a DataSet:

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

case class WC(word: String, count: Int)
val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
val expr = input.toTable
val result = expr.groupBy('word).select('word, 'count.sum as 'count).toDataSet[WC]
{% endhighlight %}

The expression DSL uses Scala symbols to refer to field names and code generation to
transform expressions to efficient runtime code. Please note that the conversion to and from
Tables only works when using Scala case classes or Flink POJOs. Please check out
the [Type Extraction and Serialization]({{ site.baseurl }}/internals/types_serialization.html) section
to learn the requirements for a class to be considered a POJO.

This is another example that shows how you
can join two Tables:

{% highlight scala %}
case class MyResult(a: String, d: Int)

val input1 = env.fromElements(...).toTable.as('a, 'b)
val input2 = env.fromElements(...).toTable.as('c, 'd)
val joined = input1.join(input2).where("a = c && d > 42").select("a, d").toDataSet[MyResult]
{% endhighlight %}

Notice, how a DataSet can be converted to a Table by using `as` and specifying new
names for the fields. This can also be used to disambiguate fields before a join operation. Also,
in this example we see that you can also use Strings to specify relational expressions.

Please refer to the Scaladoc (and Javadoc) for a full list of supported operations and a
description of the expression syntax.

{% top %}

### Java Table API

When using Java, Tables can be converted to and from DataSet using `TableEnvironment`.
This example is equivalent to the above Scala Example:

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

ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
TableEnvironment tableEnv = new TableEnvironment();

DataSet<WC> input = env.fromElements(
        new WC("Hello", 1),
        new WC("Ciao", 1),
        new WC("Hello", 1));

Table table = tableEnv.fromDataSet(input);

Table wordCounts = table
        .groupBy("word")
        .select("word, count.sum as count");

DataSet<WC> result = tableEnv.toDataSet(wordCounts, WC.class);
{% endhighlight %}

When using Java, the embedded DSL for specifying expressions cannot be used. Only String expressions
are supported. They support exactly the same feature set as the expression DSL.

{% top %}

### Table API Operators
The Table API provides a domain-spcific language to execute language-integrated queries on structured data in Scala and Java.
This section gives a brief overview of all available operators. You can find more details of operators in the [Javadoc]({{site.baseurl}}/api/java/org/apache/flink/api/table/Table.html).

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
        <p>Similar to a SQL SELECT statement. Perform a select operation.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.select("a, c as d");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Rename fields.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.as("d, e, f");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filter out elements that do not pass the filter predicate.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.filter("a % 2 = 0");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Where</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filter out elements that do not pass the filter predicate.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.where("b = 'red'");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>GroupBy</strong></td>
      <td>
        <p>Similar to a SQL GROUPBY clause. Group the elements on the grouping keys, with a following aggregation
        operator to aggregate on per-group basis.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.groupBy("a").select("a, b.sum as d");
{% endhighlight %}
        <p><i>Note:</i> Flink can refer to nonaggregated columns in the select list that are not named in
        the groupBy clause, it could be used to get better performance by avoiding unnecessary column sorting and
        grouping while nonaggregated column is cogrouped with columns in groupBy clause. For example:</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.groupBy("a").select("a, b, c.sum as d");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Join two tables, both tables must have distinct field name, and the where
        clause is mandatory for join condition.</p>
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
        <p>Similar to a SQL UNION ALL clause. Union two tables, both tables must have identical schema(field names and types).</p>
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
        <p>Similar to a SQL SELECT statement. Perform a select operation.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.select('a, 'c as 'd);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Rename fields.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filter out elements that do not pass the filter predicate.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.filter('a % 2 === 0)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Where</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filter out elements that do not pass the filter predicate.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.where('b === "red");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>GroupBy</strong></td>
      <td>
        <p>Similar to a SQL GROUPBY clause. Group the elements on the grouping keys, with a following aggregation
        operator to aggregate on per-group basis.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.groupBy('a).select('a, 'b.sum as 'd);
{% endhighlight %}
        <p><i>Note:</i> Flink can refer to nonaggregated columns in the select list that are not named in
        the groupBy clause, it could be used to get better performance by avoiding unnecessary column sorting and
        grouping while nonaggregated column is cogrouped with columns in groupBy clause. For example:</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.groupBy('a).select('a, 'b, 'c.sum as 'd);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Join two tables, both tables must have distinct field name, and the where
        clause is mandatory for join condition.</p>
{% highlight scala %}
val left = ds1.as('a, 'b, 'c);
val right = ds2.as('d, 'e, 'f);
val result = left.join(right).where('a === 'd).select('a, 'b, 'e);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION ALL clause. Union two tables, both tables must have identical schema(field names and types).</p>
{% highlight scala %}
val left = ds1.as('a, 'b, 'c);
val right = ds2.as('a, 'b, 'c);
val result = left.unionAll(right);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns rows with distinct value combinations.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
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
Some of operators in previous section expect an expression. These can either be specified using an embedded Scala DSL or
a String expression. Please refer to the examples above to learn how expressions can be
formulated.

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
In order to use a `Table` or `DataSet` in a SQL query, it has to be registered in the `TableEnvironment`, using a unique name.
A registered `Table` can be retrieved back from the `TableEnvironment` using the `scan` method:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// create a Table environment
TableEnvironment tableEnv = new TableEnvironment();
// reset the translation context: this will erase existing registered Tables
TranslationContext.reset();
// read a DataSet from an external source
DataSet<Tuple2<Integer, Long>> ds = env.readCsvFile(...);
// register the DataSet under the name "MyTable"
tableEnv.registerDataSet("MyTable", ds);
// retrieve "MyTable" into a new Table
Table t = tableEnv.scan("MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
// create a Table environment
val tableEnv = new TableEnvironment
// reset the translation context: this will erase existing registered Tables
TranslationContext.reset()
// read a DataSet from an external source
val ds = env.readCsvFile(...)
// register the DataSet under the name "MyTable"
tableEnv.registerDataSet("MyTable", ds)
// retrieve "MyTable" into a new Table
val t = tableEnv.scan("MyTable")
{% endhighlight %}
</div>
</div>

*Note: Table names are not allowed to follow the `^_DataSetTable_[0-9]+` pattern, as this is reserved for internal use only.*

When registering a `DataSet`, one can also give names to the `Table` columns. For example, if "MyTable" has three columns, `user`, `product`, and `order`, we can give them names upon registering the `DataSet` as shown below:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// register the DataSet under the name "MyTable" with columns user, product, and order
tableEnv.registerDataSet("MyTable", ds, "user, product, order");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// register the DataSet under the name "MyTable" with columns user, product, and order
tableEnv.registerDataSet("MyTable", ds, 'user, 'product, 'order)
{% endhighlight %}
</div>
</div>

A `Table` can be registered in a similar way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// read a DataSet from an external source
DataSet<Tuple2<Integer, Long>> ds = env.readCsvFile(...);
// create a Table from the DataSet with columns user, product, and order
Table t = tableEnv.fromDataSet(ds).as("user, product, order");
// register the Table under the name "MyTable"
tableEnv.registerTable("MyTable", t);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// read a DataSet from an external source and
// create a Table from the DataSet with columns user, product, and order
val t = env.readCsvFile(...).as('user, 'product, 'order)
// register the Table under the name "MyTable"
tableEnv.registerTable("MyTable", t)
{% endhighlight %}
</div>
</div>

After registering a `Table` or `DataSet`, one can use them in SQL queries. A SQL query is defined using the `sql` method of the `TableEnvironment`.
The result of the method is a new `Table` which can either be converted back to a `DataSet` or used in subsequent Table API queries.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// create a Table environment
TableEnvironment tableEnv = new TableEnvironment();
// reset the translation context: this will erase existing registered Tables
TranslationContext.reset();
// read a DataSet from an external source
DataSet<Tuple2<Integer, Long>> ds = env.readCsvFile(...);
// create a Table from the DataSet
Table t = tableEnv.fromDataSet(ds);
// register the Table under the name "MyTable"
tableEnv.registerTable("MyTable", t);
// run a sql query and retrieve the result in a new Table
Table result = tableEnv.sql("SELECT * FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
// create a Table environment
val tableEnv = new TableEnvironment
// reset the translation context: this will erase existing registered Tables
TranslationContext.reset()
// create a Table
val t = env.readCsvFile(...).as('a, 'b, 'c)
// register the Table under the name "MyTable"
tableEnv.registerTable("MyTable", t)
// run a sql query and retrieve the result in a new Table
val result = tableEnv.sql("SELECT * FROM MyTable")
{% endhighlight %}
</div>
</div>

{% top %}

