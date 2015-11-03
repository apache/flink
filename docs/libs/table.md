---
title: "Table API - Relational Queries"
is_beta: true
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

**The Table API an experimental feature**

Flink provides an API that allows specifying operations using SQL-like expressions. Instead of
manipulating `DataSet` or `DataStream` you work with `Table` on which relational operations can
be performed.

The following dependency must be added to your project when using the Table API:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the Table API is currently not part of the binary distribution. See linking with it for cluster execution [here]({{ site.baseurl }}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

## Scala Table API

The Table API can be enabled by importing `org.apache.flink.api.scala.table._`.  This enables
implicit conversions that allow
converting a DataSet or DataStream to a Table. This example shows how a DataSet can
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

The expression DSL uses Scala symbols to refer to field names and we use code generation to
transform expressions to efficient runtime code. Please note that the conversion to and from
Tables only works when using Scala case classes or Flink POJOs. Please check out
the [programming guide]({{ site.baseurl }}/apis/programming_guide.html) to learn the requirements for a class to be
considered a POJO.

This is another example that shows how you
can join to Tables:

{% highlight scala %}
case class MyResult(a: String, d: Int)

val input1 = env.fromElements(...).toTable('a, 'b)
val input2 = env.fromElements(...).toTable('c, 'd)
val joined = input1.join(input2).where("b = a && d > 42").select("a, d").toDataSet[MyResult]
{% endhighlight %}

Notice, how a DataSet can be converted to a Table by using `as` and specifying new
names for the fields. This can also be used to disambiguate fields before a join operation. Also,
in this example we see that you can also use Strings to specify relational expressions.

Please refer to the Scaladoc (and Javadoc) for a full list of supported operations and a
description of the expression syntax.

## Java Table API

When using Java, Tables can be converted to and from DataSet and DataStream using `TableEnvironment`.
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

Table filtered = table
        .groupBy("word")
        .select("word.count as count, word")
        .filter("count = 2");

DataSet<WC> result = tableEnv.toDataSet(filtered, WC.class);
{% endhighlight %}

When using Java, the embedded DSL for specifying expressions cannot be used. Only String expressions
are supported. They support exactly the same feature set as the expression DSL.

## Table API Operators
Table API provide a domain-spcific language to execute language-integrated queries on structured data in Scala and Java.
This section gives a brief overview of all available operators. You can find more details of operators in the [Javadoc]({{site.baseurl}}/api/java/org/apache/flink/api/table/Table.html).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

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
        <p>Similar to a SQL GROUPBY clause. Group the elements on the grouping keys, with a following aggregation</p>
        <p>operator to aggregate on per-group basis.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.groupBy("a").select("a, b.sum as d");
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Join two tables, both tables must have distinct field name, and the where</p>
        <p>clause is mandatory for join condition.</p>
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
Table result = left.union(right);
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
        <p>Similar to a SQL GROUPBY clause. Group the elements on the grouping keys, with a following aggregation</p>
        <p>operator to aggregate on per-group basis.</p>
{% highlight scala %}
val in = ds.as('a, 'b, 'c);
val result = in.groupBy('a).select('a, 'b.sum as 'd);
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Join two tables, both tables must have distinct field name, and the where</p>
        <p>clause is mandatory for join condition.</p>
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
val result = left.union(right);
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

## Expression Syntax
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

product = binary bitwise , [ ( "*" | "/" | "%" ) , binary bitwise ] ;

binary bitwise = unary , [ ( "&" | "!" | "^" ) , unary ] ;

unary = [ "!" | "-" | "~" ] , suffix ;

suffix = atom | aggregation | cast | as | substring ;

aggregation = atom , [ ".sum" | ".min" | ".max" | ".count" | "avg" ] ;

cast = atom , ".cast(" , data type , ")" ;

data type = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOL" | "BOOLEAN" | "STRING" ;

as = atom , ".as(" , field reference , ")" ;

substring = atom , ".substring(" , substring start , ["," substring end] , ")" ;

substring start = single expression ;

substring end = single expression ;

atom = ( "(" , single expression , ")" ) | literal | field reference ;

{% endhighlight %}

Here, `literal` is a valid Java literal and `field reference` specifies a column in the data. The
column names follow Java identifier syntax.

