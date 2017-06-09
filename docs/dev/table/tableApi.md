---
title: "Table API"
nav-parent_id: tableapi
nav-pos: 20
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

The Table API is a language-integrated relational API for Scala and Java. It is a unified API for both stream and batch processing. The Table API is a super set of the SQL language and is specially designed for working with Apache Flink. Instead of defining table programs as a large SQL string, the Table API allows for specifying programs in Java or Scala with support by the IDE (e.g. by autocompletion of available operations and functions, and by syntax validation). 

SQL and the Table API have much in common. Please have a look at the [Common Concepts & API]({{ site.baseurl }}/dev/table/common.html) to learn how to create a `Table` object from DataSet API, DataStream API, or a table source. Have a look at the [Streaming Concepts]({{ site.baseurl }}/dev/table/streaming.html) to learn about *Dynamic Tables* and *Time Attributes* if you work with streaming data.

The following examples assume a registered table called `Orders` with attributes `a, b, c, rowtime`. The `rowtime` field is either a logical time attribute in streaming or a regular timestamp field in batch.

* This will be replaced by the TOC
{:toc}

Overview & Examples
-----------------------------

The Table API is available for Scala and Java. The Scala Table API uses Scala expressions that are converted implicitly, the Java Table API uses strings which are parsed and converted into expressions.

The following example shows the differences between the Scala and Java Table API. The table program is executed in a batch environment. It scans the `Orders` table, groups by field `a`, and counts the resulting rows per group. For debugging reasons, we convert the result of the table program to a DataSet program of `Row` objects and print it.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

The Java Table API is enabled by importing `org.apache.flink.table.api.java.*`. The following example shows how a Java Table API program is constructed. With Java, expressions must be specified by strings. The embedded expression DSL is not supported.

{% highlight java %}
// environment configuration
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

// register Orders table in table environment
// ...

// specify table program
Table orders = tEnv.scan("Orders"); // schema (a, b, c, rowtime)

Table counts = orders
        .groupBy("a")
        .select("a, b.count as cnt");

// conversion to underlying API
DataSet<Row> result = tableEnv.toDataSet(wordCounts, Row.class);
result.print();
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

The Scala Table API is enabled by importing `org.apache.flink.table.api.scala._`. If you also use non-relational APIs make sure to also import `org.apache.flink.api.scala._`. 

The following example shows how a Scala Table API program is constructed. Fields in the Scala Table API are referenced using Scala Symbols (start with `'`).

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

// environment configuration
val env = ExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getTableEnvironment(env)

// register Orders table in table environment
// ...

// specify table program
val orders = tEnv.scan("Orders") // schema (a, b, c, rowtime)

val result = orders
               .groupBy('a)
               .select('a, 'b.count as 'cnt)
               .toDataSet[Row] // conversion to underlying API
               .print()
{% endhighlight %}

</div>
</div>

The following code shows a more complex Table API program. The program scans again the `Orders` table. It filters null values, normalizes string field `a`, and calculates the average billing amount `b` that was made per hour for a certain product `a`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// environment configuration
// ...

// specify table program
Table orders = tEnv.scan("Orders"); // schema (a, b, c, rowtime)

Table result = orders
        .filter("a.isNotNull && b.isNotNull && c.isNotNull")
        .select("a.lowerCase(), b, rowtime")
        .window(Tumble.over("1.hour").on("rowtime").as("hourlyWindow"))
        .groupBy("hourlyWindow, a")
        .select("a, b.avg as averageBillingAmount");
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

{% highlight scala %}
// environment configuration
// ...

// specify table program
val orders: Table = tEnv.scan("Orders") // schema (a, b, c, rowtime)

val result: Table = orders
        .filter('a.isNotNull && 'b.isNotNull && 'c.isNotNull)
        .select('a.lowerCase(), 'b, rowtime)
        .window(Tumble over 1.hour on 'rowtime as 'hourlyWindow)
        .groupBy('hourlyWindow, 'a)
        .select('a, 'b.avg as 'averageBillingAmount);
{% endhighlight %}

</div>
</div>

Since the Table API aims to unify batch and streaming, the program above is runnable in both a batch and streaming environment without any modification of the table program itself. This is particularly interesting if we want to compute exact results from time-to-time, so that late events that are heavily out-of-order can be included in the computation.

{% top %}

Operations
----------

The Table API supports the following operations. Please note that not all operations are possible in both batch and streaming yet; they are tagged accordingly.

### Scan, Projection, and Filter

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
  		<td><strong>Scan</strong></td>
  		<td>
        <p>Similar to the FROM clause in a SQL query. Performs a scan of a registered table.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
  	</tr>
    <tr>
      <td><strong>Select</strong></td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.select("a, c as d");
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight java %}
Table result = orders.select("*");
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Renames fields.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.as("x, y, z, t");
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>

    <tr>
      <td><strong>Where / Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.where("b === 'red'");
{% endhighlight %}
or
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.filter("a % 2 === 0");
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
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
  		<td><strong>Scan</strong></td>
  		<td>
        <p>Similar to the FROM clause in a SQL query. Performs a scan of a registered table.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
  	</tr>
  	<tr>
      <td><strong>Select</strong></td>
      <td>
        <p>Similar to a SQL SELECT statement. Performs a select operation.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.select('a, 'c as 'd)
{% endhighlight %}
        <p>You can use star (<code>*</code>) to act as a wild card, selecting all of the columns in the table.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.select('*)
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>

    <tr>
      <td><strong>As</strong></td>
      <td>
        <p>Renames fields.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders").as('x, 'y, 'z, 't')
{% endhighlight %}
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>

    <tr>
      <td><strong>Where / Filter</strong></td>
      <td>
        <p>Similar to a SQL WHERE clause. Filters out rows that do not pass the filter predicate.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.filter('a % 2 === 0)
{% endhighlight %}
or
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.where('b === "red")
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### Aggregations

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
      <td><strong>GroupBy Aggregation</strong></td>
      <td>
        <p>Similar to a SQL GROUP BY clause. Groups the rows on the grouping keys with a following running aggregation operator to aggregate rows group-wise.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.groupBy("a").select("a, b.sum as d");
{% endhighlight %}
        <p>For streaming: Depending on the type of aggregation, the accumulated state might grow infinitely within this operation. Please provide a query configuration with valid retention interval to prevent excessive state size.</p>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <span class="label label-info">Result Updating</span>
      </td>
    </tr>
    <tr>
    	<td><strong>GroupBy Window Aggregation</strong></td>
    	<td>
        <p>Groups the rows on the grouping keys and a window with a following aggregation to compute a single result row per group. A group window splits a possibly infinite number of rows into pieces of finite size, over which computations can be applied. See the <a href="#group-windows">group windows section</a> for more details.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders
    .window(Tumble.over("5.minutes").on("rowtime").as("w")) // define window
    .groupBy("a, w") // group by key and window
    .select("w.start, w.end, b.sum as d"); // access window properties and aggregate
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> 
      </td>
    </tr>
    <tr>
    	<td><strong>Over Window Aggregation</strong></td>
      <td>
       <p>Similar to a SQL OVER clause. Over window aggregates are computed for each row, based on a window (range) of preceding and succeeding rows. See the <a href="#over-windows">over windows section</a> for more details.</p>
       {% highlight scala %}
Table orders = tableEnv.scan("Orders");
Table result = orders
    // define window
    .window(Over  
      .partitionBy("a")
      .orderBy("rowtime)
      .preceding("UNBOUNDED_RANGE")
      .following("CURRENT_RANGE")
      .as("w")
    .select("a, b.avg over w, b.max over w, b.min over w") // sliding aggregate
{% endhighlight %}
       <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming.html#time-attributes">time attribute</a></p>
       <span class="label label-primary">Streaming</span> 
      </td>
    </tr>
    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight java %}
Table orders = tableEnv.scan("Orders");
Table result = orders.distinct();
{% endhighlight %}
        <span class="label label-primary">Batch</span>
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
      <td><strong>GroupBy Window Aggregation</strong></td>
      <td>
        <p>Similar to a SQL GROUP BY clause. Groups the rows on the grouping keys with a following running aggregation operator to aggregate rows group-wise.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.groupBy('a).select('a, 'b.sum as 'd)
{% endhighlight %}
        <p>For streaming: Depending on the type of aggregation, the accumulated state might grow infinitely within this operation. Please provide a query configuration with valid retention interval to prevent excessive state size.</p>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <span class="label label-info">Result Updating</span>
      </td>
    </tr>
    <tr>
    	<td><strong>GroupBy Window Aggregation</strong></td>
    	<td>
        <p>Groups the rows on the grouping keys and a window with a following aggregation to compute a single result row per group. A group window splits a possibly infinite number of rows into pieces of finite size, over which computations can be applied. See the <a href="#group-windows">group windows section</a> for more details.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result: Table = orders
    .window(Tumble over 5.minutes on 'rowtime as 'w) // define window
    .groupBy('a, 'w) // group by key and window
    .select('w.start, 'w.end, 'b.sum as 'd) // access window properties and aggregate
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>
    <tr>
    	<td><strong>Over Window Aggregation</strong></td>
    	<td>
       <p>Similar to a SQL OVER clause. Over window aggregates are computed for each row, based on a window (range) of preceding and succeeding rows. See the <a href="#over-windows">over windows section</a> for more details.</p>
       {% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result: Table = orders
    // define window
    .window(Over  
      partitionBy 'a
      orderBy 'rowtime
      preceding UNBOUNDED_RANGE
      following CURRENT_RANGE
      as 'w)
    .select('a, 'b.avg over 'w, 'b.max over 'w, 'b.min over 'w,) // sliding aggregate
{% endhighlight %}
       <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming.html#time-attributes">time attribute</a></p>
       <span class="label label-primary">Streaming</span> 
      </td>
    </tr>
    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Similar to a SQL DISTINCT clause. Returns records with distinct value combinations.</p>
{% highlight scala %}
val orders: Table = tableEnv.scan("Orders")
val result = orders.distinct()
{% endhighlight %}
        <span class="label label-primary">Batch</span>
      </td>
    </tr>
  </tbody>
</table>
</div>
</div>

{% top %}

### Joins

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
      <td><strong>Inner Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and at least one equality join predicate must be defined through join operator or using a where or filter operator.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "d, e, f");
Table result = left.join(right).where("a = d").select("a, b, e");
{% endhighlight %}
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
      </td>
    </tr>
    <tr>
    	<td><strong>TableFunction Join</strong></td>
    	<td>
        <p>Similar to a SQL JOIN. Joins a table with a table function. Each row of the left table is joined with all rows produced by the table function. If the table function does not produce any row, the join does not produce a result row.
        </p>
{% highlight java %}
// register function
TableFunction<String> split = new MySplitUDTF();
tEnv.registerFunction("split", split);

// join
Table orders = tableEnv.scan("Orders");
Table result = orders
    .join(new Table(tEnv, "split(c)").as("s", "t", "v")))
    .select("a, b, s, t, v");
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>
    <tr>
    	<td><strong>TableFunction LeftOuterJoin</strong></td>
      <td>
        <p>Similar to a SQL LEFT OUTER JOIN with ON TRUE predicate. Joins a table with a table function. Each row of the outer table is joined with all rows produced by the table function. If the table function does not produce any row, the outer row is padded with nulls.</p>
{% highlight java %}
// register function
TableFunction<String> split = new MySplitUDTF();
tEnv.registerFunction("split", split);

// join
Table orders = tableEnv.scan("Orders");
Table result = orders
    .leftOuterJoin(new Table(tEnv, "split(c)").as("s", "t", "v")))
    .select("a, b, s, t, v");
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
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
      <td><strong>Join</strong></td>
      <td>
        <p>Similar to a SQL JOIN clause. Joins two tables. Both tables must have distinct field names and an equality join predicate must be defined using a where or filter operator.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'd, 'e, 'f);
val result = left.join(right).where('a === 'd).select('a, 'b, 'e);
{% endhighlight %}
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
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
        <span class="label label-primary">Batch</span>
      </td>
    </tr>
    <tr>
    	<td><strong>TableFunction Join</strong></td>
    	<td>
        <p>Similar to a SQL JOIN. Joins a table with a table function. Each row of the left table is joined with all rows produced by the table function. If the table function does not produce any row, the join does not produce a result row.</p>
        {% highlight scala %}
// instantiate function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .join(split('c) as ('s, 't, 'v))
    .select('a, 'b, 's, 't, 'v)
{% endhighlight %}
          <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
        </td>
    </tr>
    <tr>
    	<td><strong>TableFunction LeftOuterJoin</strong></td>
    	<td>
        <p>Similar to a SQL LEFT OUTER JOIN with ON TRUE predicate. Joins a table with a table function. Each row of the outer table is joined with all rows produced by the table function. If the table function does not produce any row, the outer row is padded with nulls.</p>
{% highlight scala %}
// instantiate function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .leftOuterJoin(split('c) as ('s, 't, 'v))
    .select('a, 'b, 's, 't, 'v)
{% endhighlight %}
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

{% top %}

### Set Operations

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
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed. Both tables must have identical field types.</p>
{% highlight java %}
Table left = tableEnv.fromDataSet(ds1, "a, b, c");
Table right = tableEnv.fromDataSet(ds2, "a, b, c");
Table result = left.union(right);
{% endhighlight %}
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
      <td><strong>Union</strong></td>
      <td>
        <p>Similar to a SQL UNION clause. Unions two tables with duplicate records removed, both tables must have identical field types.</p>
{% highlight scala %}
val left = ds1.toTable(tableEnv, 'a, 'b, 'c);
val right = ds2.toTable(tableEnv, 'a, 'b, 'c);
val result = left.union(right);
{% endhighlight %}
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

{% top %}

### OrderBy & Limit

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
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight scala %}
val in = ds.toTable(tableEnv, 'a, 'b, 'c);
val result = in.orderBy('a.asc);
{% endhighlight %}
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
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
      <td><strong>Order By</strong></td>
      <td>
        <p>Similar to a SQL ORDER BY clause. Returns records globally sorted across all parallel partitions.</p>
{% highlight java %}
Table in = tableEnv.fromDataSet(ds, "a, b, c");
Table result = in.orderBy("a.asc");
{% endhighlight %}
<span class="label label-primary">Batch</span>
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
<span class="label label-primary">Batch</span>
      </td>
    </tr>

  </tbody>
</table>
</div>
</div>

### Group Windows

The Table API is a declarative API to define queries on batch and streaming tables. Projection, selection, and union operations can be applied both on streaming and batch tables without additional semantics. Aggregations on (possibly) infinite streaming tables, however, can only be computed on finite groups of records. Window aggregates group rows into finite groups based on time or row-count intervals and evaluate aggregation functions once per group. For batch tables, windows are a convenient shortcut to group records by time intervals.

Windows are defined using the `window(w: Window)` clause and require an alias, which is specified using the `as` clause. In order to group a table by a window, the window alias must be referenced in the `groupBy(...)` clause like a regular grouping attribute. 
The following example shows how to define a window aggregation on a table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Table table = input
  .window([Window w].as("w"))  // define window with alias w
  .groupBy("w")  // group the table by window w
  .select("b.sum");  // aggregate
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
  .select("a, b.sum");  // aggregate
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
  .select("a, w.start, w.end, b.count"); // aggregate and add window start and end timestamps
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
.window(Tumble.over("10.minutes").on("rowtime").as("w"));

// Tumbling Processing-time Window
.window(Tumble.over("10.minutes").as("w"));

// Tumbling Row-count Window
.window(Tumble.over("10.rows").as("w"));
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
.window(Slide.over("10.minutes").every("5.minutes").on("rowtime").as("w"));

// Sliding Processing-time window
.window(Slide.over("10.minutes").every("5.minutes").as("w"));

// Sliding Row-count window
.window(Slide.over("10.rows").every("5.rows").as("w"));
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
.window(Session.withGap("10.minutes").on("rowtime").as("w"));

// Session Processing-time Window
.window(Session.withGap("10.minutes").as("w"));
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

{% top %}

### Over Windows

**TODO**

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
| `Types.PRIMITIVE_ARRAY`| `ARRAY`                     | e.g. `int[]`           |
| `Types.OBJECT_ARRAY`   | `ARRAY`                     | e.g. `java.lang.Byte[]`|
| `Types.MAP`            | `MAP`                       | `java.util.HashMap`    |


Advanced types such as generic types, composite types (e.g. POJOs or Tuples), and array types (object or primitive arrays) can be fields of a row. 

Generic types are treated as a black box within Table API and SQL yet.

Composite types, however, are fully supported types where fields of a composite type can be accessed using the `.get()` operator in Table API and dot operator (e.g. `MyTable.pojoColumn.myField`) in SQL. Composite types can also be flattened using `.flatten()` in Table API or `MyTable.pojoColumn.*` in SQL.

Array types can be accessed using the `myArray.at(1)` operator in Table API and `myArray[1]` operator in SQL. Array literals can be created using `array(1, 2, 3)` in Table API and `ARRAY[1, 2, 3]` in SQL.

**TODO: Clean-up and move relevant parts to the "Mappings Types to Table Schema" section of the Common Concepts & API page.**

{% top %}

Expression Syntax
-----------------

Some of the operators in previous sections expect one or more expressions. Expressions can be specified using an embedded Scala DSL or as Strings. Please refer to the examples above to learn how expressions can be specified.

This is the EBNF grammar for expressions:

{% highlight ebnf %}

expressionList = expression , { "," , expression } ;

expression = timeIndicator | overConstant | alias ;

alias = logic | ( logic , "as" , fieldReference ) | ( logic , "as" , "(" , fieldReference , { "," , fieldReference } , ")" ) ;

logic = comparison , [ ( "&&" | "||" ) , comparison ] ;

comparison = term , [ ( "=" | "==" | "===" | "!=" | "!==" | ">" | ">=" | "<" | "<=" ) , term ] ;

term = product , [ ( "+" | "-" ) , product ] ;

product = unary , [ ( "*" | "/" | "%") , unary ] ;

unary = [ "!" | "-" ] , composite ;

composite = over | nullLiteral | suffixed | atom ;

suffixed = interval | cast | as | if | functionCall ;

interval = timeInterval | rowInterval ;

timeInterval = composite , "." , ("year" | "years" | "month" | "months" | "day" | "days" | "hour" | "hours" | "minute" | "minutes" | "second" | "seconds" | "milli" | "millis") ;

rowInterval = composite , "." , "rows" ;

cast = composite , ".cast(" , dataType , ")" ;

dataType = "BYTE" | "SHORT" | "INT" | "LONG" | "FLOAT" | "DOUBLE" | "BOOLEAN" | "STRING" | "DECIMAL" | "SQL_DATE" | "SQL_TIME" | "SQL_TIMESTAMP" | "INTERVAL_MONTHS" | "INTERVAL_MILLIS" | ( "PRIMITIVE_ARRAY" , "(" , dataType , ")" ) | ( "OBJECT_ARRAY" , "(" , dataType , ")" ) ;

as = composite , ".as(" , fieldReference , ")" ;

if = composite , ".?(" , expression , "," , expression , ")" ;

functionCall = composite , "." , functionIdentifier , [ "(" , [ expression , { "," , expression } ] , ")" ] ;

atom = ( "(" , expression , ")" ) | literal | fieldReference ;

fieldReference = "*" | identifier ;

nullLiteral = "Null(" , dataType , ")" ;

timeIntervalUnit = "YEAR" | "YEAR_TO_MONTH" | "MONTH" | "DAY" | "DAY_TO_HOUR" | "DAY_TO_MINUTE" | "DAY_TO_SECOND" | "HOUR" | "HOUR_TO_MINUTE" | "HOUR_TO_SECOND" | "MINUTE" | "MINUTE_TO_SECOND" | "SECOND" ;

timePointUnit = "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "QUARTER" | "WEEK" | "MILLISECOND" | "MICROSECOND" ;

over = composite , "over" , fieldReference ;

overConstant = "current_row" | "current_range" | "unbounded_row" | "unbounded_row" ;

timeIndicator = fieldReference , "." , ( "proctime" | "rowtime" ) ;

{% endhighlight %}

Here, `literal` is a valid Java literal, `fieldReference` specifies a column in the data (or all columns if `*` is used), and `functionIdentifier` specifies a supported scalar function. The
column names and function names follow Java identifier syntax. Expressions specified as Strings can also use prefix notation instead of suffix notation to call operators and functions.

If working with exact numeric values or large decimals is required, the Table API also supports Java's BigDecimal type. In the Scala Table API decimals can be defined by `BigDecimal("123456")` and in Java by appending a "p" for precise e.g. `123456p`.

In order to work with temporal values the Table API supports Java SQL's Date, Time, and Timestamp types. In the Scala Table API literals can be defined by using `java.sql.Date.valueOf("2016-06-27")`, `java.sql.Time.valueOf("10:10:42")`, or `java.sql.Timestamp.valueOf("2016-06-27 10:10:42.123")`. The Java and Scala Table API also support calling `"2016-06-27".toDate()`, `"10:10:42".toTime()`, and `"2016-06-27 10:10:42.123".toTimestamp()` for converting Strings into temporal types. *Note:* Since Java's temporal SQL types are time zone dependent, please make sure that the Flink Client and all TaskManagers use the same time zone.

Temporal intervals can be represented as number of months (`Types.INTERVAL_MONTHS`) or number of milliseconds (`Types.INTERVAL_MILLIS`). Intervals of same type can be added or subtracted (e.g. `1.hour + 10.minutes`). Intervals of milliseconds can be added to time points (e.g. `"2016-08-10".toDate + 5.days`).

**TODO: needs to be reworked, IMO. Grammar might be complete but is hard to understand without concrete examples**

{% top %}

Built-In Functions
------------------

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

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sin()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.cos()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.tan()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.cot()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cotangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.asin()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.acos()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.atan()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.degrees()
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from radians to degrees.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.radians()
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from degrees to radians.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.sign()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the signum of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
NUMERIC.round(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds the given number to <i>integer</i> places right to the decimal point.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
pi()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other value to pi.</p>
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
        <p>Returns the sum of the numeric field across all input values. If all values are null, null is returned.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.sum0
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of the numeric field across all input values. If all values are null, 0 is returned.</p>
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


    <tr>
      <td>
        {% highlight java %}
FIELD.stddevPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight java %}
FIELD.stddevSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.varPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population variance (square of the population standard deviation) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight java %}
FIELD.varSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample variance (square of the sample standard deviation) of the numeric field across all input values.</p>
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
    
    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sin()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.cos()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.tan()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cotangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.cot()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.asin()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.acos()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.atan()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.degrees()
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from radians to degrees.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.radians()
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from degrees to radians.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.sign()
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the signum of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
NUMERIC.round(INT)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds the given number to <i>integer</i> places right to the decimal point.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
pi()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other value to pi.</p>
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
        <p>Returns the sum of the numeric field across all input values. If all values are null, null is returned.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.sum0
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sum of the numeric field across all input values. If all values are null, 0 is returned.</p>
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

    <tr>
      <td>
        {% highlight scala %}
FIELD.stddevPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight scala %}
FIELD.stddevSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.varPop
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population variance (square of the population standard deviation) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight scala %}
FIELD.varSamp
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample variance (square of the sample standard deviation) of the numeric field across all input values.</p>
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

</div>

### Limitations

The following operations are not supported yet:

- Binary string operators and functions
- System functions
- Collection functions
- Aggregate functions like REGR_xxx
- Distinct aggregate functions like COUNT DISTINCT

{% top %}
