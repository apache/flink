---
title: "SQL"
nav-parent_id: tableapi
nav-pos: 30
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

SQL queries are specified with the `sqlQuery()` method of the `TableEnvironment`. The method returns the result of the SQL query as a `Table`. A `Table` can be used in [subsequent SQL and Table API queries](common.html#mixing-table-api-and-sql), be [converted into a DataSet or DataStream](common.html#integration-with-datastream-and-dataset-api), or [written to a TableSink](common.html#emit-a-table)). SQL and Table API queries can seamlessly mixed and are holistically optimized and translated into a single program.

In order to access a table in a SQL query, it must be [registered in the TableEnvironment](common.html#register-tables-in-the-catalog). A table can be registered from a [TableSource](common.html#register-a-tablesource), [Table](common.html#register-a-table), [DataStream, or DataSet](common.html#register-a-datastream-or-dataset-as-table). Alternatively, users can also [register external catalogs in a TableEnvironment](common.html#register-an-external-catalog) to specify the location of the data sources.

For convenience `Table.toString()` automatically registers the table under a unique name in its `TableEnvironment` and returns the name. Hence, `Table` objects can be directly inlined into SQL queries (by string concatenation) as shown in the examples below.

**Note:** Flink's SQL support is not yet feature complete. Queries that include unsupported SQL features cause a `TableException`. The supported features of SQL on batch and streaming tables are listed in the following sections.

* This will be replaced by the TOC
{:toc}

Specifying a Query
------------------

The following examples show how to specify a SQL queries on registered and inlined tables.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);

// SQL query with an inlined (unregistered) table
Table table = tableEnv.toTable(ds, "user, product, amount");
Table result = tableEnv.sqlQuery(
  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

// SQL query with a registered table
// register the DataStream as table "Orders"
tableEnv.registerDataStream("Orders", ds, "user, product, amount");
// run a SQL query on the Table and retrieve the result as a new Table
Table result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// create and register a TableSink
TableSink csvSink = new CsvTableSink("/path/to/file", ...);
String[] fieldNames = {"product", "amount"};
TypeInformation[] fieldTypes = {Types.STRING, Types.INT};
tableEnv.registerTableSink("RubberOrders", fieldNames, fieldTypes, csvSink);
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(Long, String, Integer)] = env.addSource(...)

// SQL query with an inlined (unregistered) table
val table = ds.toTable(tableEnv, 'user, 'product, 'amount)
val result = tableEnv.sqlQuery(
  s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

// SQL query with a registered table
// register the DataStream under the name "Orders"
tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount)
// run a SQL query on the Table and retrieve the result as a new Table
val result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")

// SQL update with a registered table
// create and register a TableSink
TableSink csvSink = new CsvTableSink("/path/to/file", ...)
val fieldNames: Array[String] = Array("product", "amount")
val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT)
tableEnv.registerTableSink("RubberOrders", fieldNames, fieldTypes, csvSink)
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

Supported Syntax
----------------

Flink parses SQL using [Apache Calcite](https://calcite.apache.org/docs/reference.html), which supports standard ANSI SQL. DDL statements are not supported by Flink.

The following BNF-grammar describes the superset of supported SQL features in batch and streaming queries. The [Operations](#operations) section shows examples for the supported features and indicates which features are only supported for batch or streaming queries.

{% highlight sql %}

insert:
  INSERT INTO tableReference
  query
  
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
  [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]
  
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
  | UNNEST '(' expression ')'

values:
  VALUES expression [, expression ]*

groupItem:
  expression
  | '(' ')'
  | '(' expression [, expression ]* ')'
  | CUBE '(' expression [, expression ]* ')'
  | ROLLUP '(' expression [, expression ]* ')'
  | GROUPING SETS '(' groupItem [, groupItem ]* ')'

windowRef:
    windowName
  | windowSpec

windowSpec:
    [ windowName ]
    '('
    [ ORDER BY orderItem [, orderItem ]* ]
    [ PARTITION BY expression [, expression ]* ]
    [
        RANGE numericOrIntervalExpression {PRECEDING}
      | ROWS numericExpression {PRECEDING}
    ]
    ')'

{% endhighlight %}

Flink SQL uses a lexical policy for identifier (table, attribute, function names) similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. <code>"SELECT a AS `my field` FROM t"</code>).

{% top %}

Operations
--------------------

### Scan, Projection, and Filter

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
  		<td>
        <strong>Scan / Select / As</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
  		<td>
{% highlight sql %}
SELECT * FROM Orders

SELECT a, c AS d FROM Orders
{% endhighlight %}
      </td>
  	</tr>
    <tr>
      <td>
        <strong>Where / Filter</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
{% highlight sql %}
SELECT * FROM Orders WHERE b = 'red'

SELECT * FROM Orders WHERE a % 2 = 0
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>User-defined Scalar Functions (Scalar UDF)</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
      <p>UDFs must be registered in the TableEnvironment. See the <a href="udfs.html">UDF documentation</a> for details on how to specify and register scalar UDFs.</p>
{% highlight sql %}
SELECT PRETTY_PRINT(user) FROM Orders
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### Aggregations

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>GroupBy Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span><br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p><b>Note:</b> GroupBy on a streaming table produces an updating result. See the <a href="streaming.html">Streaming Concepts</a> page for details.
        </p>
{% highlight sql %}
SELECT a, SUM(b) as d
FROM Orders
GROUP BY a
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>GroupBy Window Aggregation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Use a group window to compute a single result row per group. See <a href="#group-windows">Group Windows</a> section for more details.</p>
{% highlight sql %}
SELECT user, SUM(amount)
FROM Orders
GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Over Window aggregation</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming.html#time-attributes">time attribute</a></p>
{% highlight sql %}
SELECT COUNT(amount) OVER (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM Orders

SELECT COUNT(amount) OVER w, SUM(amount) OVER w
FROM Orders 
WINDOW w AS (
  PARTITION BY user
  ORDER BY proctime
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)  
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Distinct</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span> <br>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
{% highlight sql %}
SELECT DISTINCT users FROM Orders
{% endhighlight %}
       <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Grouping sets, Rollup, Cube</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
{% highlight sql %}
SELECT SUM(amount)
FROM Orders
GROUP BY GROUPING SETS ((user), (product))
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Having</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
{% highlight sql %}
SELECT SUM(amount)
FROM Orders
GROUP BY users
HAVING SUM(amount) > 50
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>User-defined Aggregate Functions (UDAGG)</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>UDAGGs must be registered in the TableEnvironment. See the <a href="udfs.html">UDF documentation</a> for details on how to specify and register UDAGGs.</p>
{% highlight sql %}
SELECT MyAggregate(amount)
FROM Orders
GROUP BY users
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### Joins

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Inner Equi-join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Currently, only equi-joins are supported, i.e., joins that have at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.</p>
        <p><b>Note:</b> The order of joins is not optimized. Tables are joined in the order in which they are specified in the FROM clause. Make sure to specify tables in an order that does not yield a cross join (Cartesian product) which are not supported and would cause a query to fail.</p>
{% highlight sql %}
SELECT *
FROM Orders INNER JOIN Product ON Orders.productId = Product.id
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Outer Equi-join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
        <span class="label label-info">Result Updating</span>
      </td>
      <td>
        <p>Currently, only equi-joins are supported, i.e., joins that have at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.</p>
        <p><b>Note:</b> The order of joins is not optimized. Tables are joined in the order in which they are specified in the FROM clause. Make sure to specify tables in an order that does not yield a cross join (Cartesian product) which are not supported and would cause a query to fail.</p>
{% highlight sql %}
SELECT *
FROM Orders LEFT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders RIGHT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders FULL OUTER JOIN Product ON Orders.productId = Product.id
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Time-windowed Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><b>Note:</b> Time-windowed joins are a subset of regular joins that can be processed in a streaming fashion.</p>

        <p>A time-windowed join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (<code>&lt;, &lt;=, &gt;=, &gt;</code>), a <code>BETWEEN</code> predicate, or a single equality predicate that compares <a href="streaming.html#time-attributes">time attributes</a> of the same type (i.e., processing time or event time) of both input tables.</p> 
        <p>For example, the following predicates are valid window join conditions:</p>
          
        <ul>
          <li><code>ltime = rtime</code></li>
          <li><code>ltime &gt;= rtime AND ltime &lt; rtime + INTERVAL '10' MINUTE</code></li>
          <li><code>ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND</code></li>
        </ul>

{% highlight sql %}
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

The example above will join all orders with their corresponding shipments if the order was shipped four hours after the order was received.
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Expanding arrays into a relation</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Unnesting WITH ORDINALITY is not supported yet.</p>
{% highlight sql %}
SELECT users, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
{% endhighlight %}
      </td>
    </tr>
    <tr>
    	<td>
        <strong>Join with User Defined Table Functions (UDTF)</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>UDTFs must be registered in the TableEnvironment. See the <a href="udfs.html">UDF documentation</a> for details on how to specify and register UDTFs. </p>
        <p>Inner Join</p>
{% highlight sql %}
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags)) t AS tag
{% endhighlight %}
        <p>Left Outer Join</p>
{% highlight sql %}
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) t AS tag ON TRUE
{% endhighlight %}

        <p><b>Note:</b> Currently, only literal <code>TRUE</code> is supported as predicate for a left outer join against a lateral table.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### Set Operations

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Union</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>UnionAll</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION ALL
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>Intersect / Except</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  INTERSECT
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
{% highlight sql %}
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  EXCEPT
    (SELECT user FROM Orders WHERE b = 0)
)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td>
        <strong>In</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Returns true if an expression exists in a given table sub-query. The sub-query table must consist of one column. This column must have the same data type as the expression.</p>
{% highlight sql %}
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        <strong>Exists</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Returns true if the sub-query returns at least one row. Only supported if the operation can be rewritten in a join and group operation.</p>
{% highlight sql %}
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
{% endhighlight %}
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

### OrderBy & Limit

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  	<tr>
      <td>
        <strong>Order By</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
<b>Note:</b> The result of streaming queries must be primarily sorted on an ascending <a href="streaming.html#time-attributes">time attribute</a>. Additional sorting attributes are supported.

{% highlight sql %}
SELECT *
FROM Orders
ORDER BY orderTime
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Limit</strong><br>
        <span class="label label-primary">Batch</span>
      </td>
      <td>
{% highlight sql %}
SELECT *
FROM Orders
LIMIT 3
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

### Insert

<div markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>Insert Into</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Output tables must be registered in the TableEnvironment (see <a href="common.html#register-a-tablesink">Register a TableSink</a>). Moreover, the schema of the registered table must match the schema of the query.</p>

{% highlight sql %}
INSERT INTO OutputTable
SELECT users, tag
FROM Orders
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

### Group Windows

Group windows are defined in the `GROUP BY` clause of a SQL query. Just like queries with regular `GROUP BY` clauses, queries with a `GROUP BY` clause that includes a group window function compute a single result row per group. The following group windows functions are supported for SQL on batch and streaming tables.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Group Window Function</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><code>TUMBLE(time_attr, interval)</code></td>
      <td>Defines a tumbling time window. A tumbling time window assigns rows to non-overlapping, continuous windows with a fixed duration (<code>interval</code>). For example, a tumbling window of 5 minutes groups rows in 5 minutes intervals. Tumbling windows can be defined on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
    <tr>
      <td><code>HOP(time_attr, interval, interval)</code></td>
      <td>Defines a hopping time window (called sliding window in the Table API). A hopping time window has a fixed duration (second <code>interval</code> parameter) and hops by a specified hop interval (first <code>interval</code> parameter). If the hop interval is smaller than the window size, hopping windows are overlapping. Thus, rows can be assigned to multiple windows. For example, a hopping window of 15 minutes size and 5 minute hop interval assigns each row to 3 different windows of 15 minute size, which are evaluated in an interval of 5 minutes. Hopping windows can be defined on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
    <tr>
      <td><code>SESSION(time_attr, interval)</code></td>
      <td>Defines a session time window. Session time windows do not have a fixed duration but their bounds are defined by a time <code>interval</code> of inactivity, i.e., a session window is closed if no event appears for a defined gap period. For example a session window with a 30 minute gap starts when a row is observed after 30 minutes inactivity (otherwise the row would be added to an existing window) and is closed if no row is added within 30 minutes. Session windows can work on event-time (stream + batch) or processing-time (stream).</td>
    </tr>
  </tbody>
</table>


#### Time Attributes

For SQL queries on streaming tables, the `time_attr` argument of the group window function must refer to a valid time attribute that specifies the processing time or event time of rows. See the [documentation of time attributes](streaming.html#time-attributes) to learn how to define time attributes.

For SQL on batch tables, the `time_attr` argument of the group window function must be an attribute of type `TIMESTAMP`.

#### Selecting Group Window Start and End Timestamps

The start and end timestamps of group windows as well as time attributes can be selected with the following auxiliary functions:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Auxiliary Function</th>
      <th class="text-left">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <code>TUMBLE_START(time_attr, interval)</code><br/>
        <code>HOP_START(time_attr, interval, interval)</code><br/>
        <code>SESSION_START(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the inclusive lower bound of the corresponding tumbling, hopping, or session window.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_END(time_attr, interval)</code><br/>
        <code>HOP_END(time_attr, interval, interval)</code><br/>
        <code>SESSION_END(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>exclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
        <p><b>Note:</b> The exclusive upper bound timestamp <i>cannot</i> be used as a <a href="streaming.html#time-attributes">rowtime attribute</a> in subsequent time-based operations, such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>inclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
      <p>The resulting attribute is a <a href="streaming.html#time-attributes">rowtime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns a <a href="streaming.html#time-attributes">proctime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
  </tbody>
</table>

*Note:* Auxiliary functions must be called with exactly same arguments as the group window function in the `GROUP BY` clause.

The following examples show how to specify SQL queries with group windows on streaming tables.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);
// register the DataStream as table "Orders"
tableEnv.registerDataStream("Orders", ds, "user, product, amount, proctime.proctime, rowtime.rowtime");

// compute SUM(amount) per day (in event-time)
Table result1 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,  " +
  "  SUM(amount) FROM Orders " +
  "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user");

// compute SUM(amount) per day (in processing-time)
Table result2 = tableEnv.sqlQuery(
  "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");

// compute every hour the SUM(amount) of the last 24 hours in event-time
Table result3 = tableEnv.sqlQuery(
  "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");

// compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
Table result4 = tableEnv.sqlQuery(
  "SELECT user, " +
  "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
  "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
  "  SUM(amount) " +
  "FROM Orders " +
  "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(Long, String, Int)] = env.addSource(...)
// register the DataStream under the name "Orders"
tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount, 'proctime.proctime, 'rowtime.rowtime)

// compute SUM(amount) per day (in event-time)
val result1 = tableEnv.sqlQuery(
    """
      |SELECT
      |  user,
      |  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,
      |  SUM(amount)
      | FROM Orders
      | GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user
    """.stripMargin)

// compute SUM(amount) per day (in processing-time)
val result2 = tableEnv.sqlQuery(
  "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user")

// compute every hour the SUM(amount) of the last 24 hours in event-time
val result3 = tableEnv.sqlQuery(
  "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product")

// compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
val result4 = tableEnv.sqlQuery(
    """
      |SELECT
      |  user,
      |  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart,
      |  SESSION_END(rowtime, INTERVAL '12' HOUR) AS sEnd,
      |  SUM(amount)
      | FROM Orders
      | GROUP BY SESSION(rowtime(), INTERVAL '12' HOUR), user
    """.stripMargin)

{% endhighlight %}
</div>
</div>

{% top %}

Data Types
----------

The SQL runtime is built on top of Flink's DataSet and DataStream APIs. Internally, it also uses Flink's `TypeInformation` to define data types. Fully supported types are listed in `org.apache.flink.table.api.Types`. The following table summarizes the relation between SQL Types, Table API types, and the resulting Java class.

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
| `Types.SQL_DATE`       | `DATE`                      | `java.sql.Date`        |
| `Types.SQL_TIME`       | `TIME`                      | `java.sql.Time`        |
| `Types.SQL_TIMESTAMP`  | `TIMESTAMP(3)`              | `java.sql.Timestamp`   |
| `Types.INTERVAL_MONTHS`| `INTERVAL YEAR TO MONTH`    | `java.lang.Integer`    |
| `Types.INTERVAL_MILLIS`| `INTERVAL DAY TO SECOND(3)` | `java.lang.Long`       |
| `Types.PRIMITIVE_ARRAY`| `ARRAY`                     | e.g. `int[]`           |
| `Types.OBJECT_ARRAY`   | `ARRAY`                     | e.g. `java.lang.Byte[]`|
| `Types.MAP`            | `MAP`                       | `java.util.HashMap`    |
| `Types.MULTISET`       | `MULTISET`                  | e.g. `java.util.HashMap<String, Integer>` for a multiset of `String` |

Generic types and composite types (e.g., POJOs or Tuples) can be fields of a row as well. Generic types are treated as a black box and can be passed on or processed by [user-defined functions](udfs.html). Composite types can be accessed with [built-in functions](#built-in-functions) (see *Value access functions* section).

{% top %}

Built-In Functions
------------------

Flink's SQL support comes with a set of built-in functions for data transformations. This section gives a brief overview of the available functions.

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
        <p> Returns TRUE if an expression exists in a given list of expressions. This is a shorthand for multiple OR conditions. If the testing set contains NULL, the result will be NULL if the element can not be found and TRUE if it can be found. If the element is NULL, the result is always NULL. E.g. "42 IN (1, 2, 3)" leads to FALSE.</p>
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
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
value IN (sub-query)
{% endhighlight %}
      </td>
      <td>
        <p>Returns TRUE if <i>value</i> is equal to a row returned by sub-query.</p>
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
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
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming.html">Streaming Concepts</a> for details.</p>
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
LOG2(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base 2 logarithm of <i>numeric</i>.</p>
      </td>
    </tr>

    <tr>
      <td>
       {% highlight text %}
LOG(x numeric)
LOG(b numeric, x numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the logarithm of a <i>numeric</i>.</p>
        <p>If called with one parameter, this function returns the natural logarithm of <code>x</code>. If called with two parameters, this function returns the logarithm of <code>x</code> to the base <code>b</code>. <code>x</code> must be greater than 0. <code>b</code> must be greater than 1.</p>
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
CEILING(numeric)
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

    <tr>
      <td>
        {% highlight text %}
SIN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TAN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
COT(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the cotangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ATAN2(numeric, numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc tangent of a given coordinate.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ASIN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc sine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ACOS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc cosine of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ATAN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the arc tangent of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DEGREES(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from radians to degrees.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RADIANS(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Converts <i>numeric</i> from degrees to radians.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SIGN(numeric)
{% endhighlight %}
      </td>
      <td>
        <p>Calculates the signum of a given number.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ROUND(numeric, int)
{% endhighlight %}
      </td>
      <td>
        <p>Rounds the given number to <i>integer</i> places right to the decimal point.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
PI()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other value to pi.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
E()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a value that is closer than any other value to e.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RAND()
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
RAND(seed integer)
{% endhighlight %}
      </td>
      <td>
        <p>Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a initial seed. Two RAND functions will return identical sequences of numbers if they have same initial seed.</p>
      </td>
    </tr>

    <tr>
     <td>
       {% highlight text %}
RAND_INTEGER(bound integer)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value (exclusive).</p>
    </td>
   </tr>

    <tr>
     <td>
       {% highlight text %}
RAND_INTEGER(seed integer, bound integer)
{% endhighlight %}
     </td>
    <td>
      <p>Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value (exclusive) with a initial seed. Two RAND_INTEGER functions will return identical sequences of numbers if they have same initial seed and same bound.</p>
    </td>
   </tr>

    <tr>
      <td>
{% highlight text %}
BIN(numeric)
      {% endhighlight %}
      </td>
      <td>
        <p>Returns a string representation of an integer numeric value in binary format. Returns null if numeric is null. E.g. "4" leads to "100", "12" leads to "1100".</p>
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

    <tr>
      <td>
        {% highlight text %}
CONCAT(string1, string2,...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the string that results from concatenating the arguments. Returns NULL if any argument is NULL. E.g. <code>CONCAT("AA", "BB", "CC")</code> returns <code>AABBCC</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
CONCAT_WS(separator, string1, string2,...)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the string that results from concatenating the arguments using a separator. The separator is added between the strings to be concatenated. Returns NULL If the separator is NULL. CONCAT_WS() does not skip empty strings. However, it does skip any NULL argument. E.g. <code>CONCAT_WS("~", "AA", "BB", "", "CC")</code> returns <code>AA~BB~~CC</code></p>
  </td>
    </tr>

        <tr>
      <td>
        {% highlight text %}
LPAD(text string, len integer, pad string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the string text left-padded with the string pad to a length of len characters. If text is longer than len, the return value is shortened to len characters. E.g. <code>LPAD('hi',4,'??')</code> returns <code>??hi</code>, <code>LPAD('hi',1,'??')</code> returns <code>h</code>.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
RPAD(text string, len integer, pad string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the string text right-padded with the string pad to a length of len characters. If text is longer than len, the return value is shortened to len characters. E.g. <code>RPAD('hi',4,'??')</code> returns <code>hi??</code>, <code>RPAD('hi',1,'??')</code> returns <code>h</code>.</p>
      </td>
    </tr>
    <tr>
      <td>
        {% highlight text %}
FROM_BASE64(text string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base string decoded with base64, if text is NULL, returns NULL. E.g. <code>FROM_BASE64('aGVsbG8gd29ybGQ=')</code> returns <code>hello world</code>.</p>
      </td>
    </tr>  
        
    <tr>
      <td>
        {% highlight text %}
TO_BASE64(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the base64-encoded result of <i>string</i>; returns NULL if <i>string</i> is NULL.</p> 
        <p>E.g., <code>TO_BASE64("hello world")</code> returns "aGVsbG8gd29ybGQ=".</p>
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
YEAR(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the year from a SQL date. Equivalent to <code>EXTRACT(YEAR FROM date)</code>. E.g. <code>YEAR(DATE '1994-09-27')</code> leads to 1994.</p>
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
        <p>Returns the quarter of a year from a SQL date (an integer between 1 and 4). Equivalent to <code>EXTRACT(QUARTER FROM date)</code>. E.g. <code>QUARTER(DATE '1994-09-27')</code> leads to 3. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MONTH(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the month of a year from a SQL date (an integer between 1 and 12). Equivalent to <code>EXTRACT(MONTH FROM date)</code>. E.g. <code>MONTH(DATE '1994-09-27')</code> leads to 9. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
WEEK(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the week of a year from a SQL date (an integer between 1 and 53). Equivalent to <code>EXTRACT(WEEK FROM date)</code>. E.g. <code>WEEK(DATE '1994-09-27')</code> leads to 39. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFYEAR(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a year from a SQL date (an integer between 1 and 366). Equivalent to <code>EXTRACT(DOY FROM date)</code>. E.g. <code>DAYOFYEAR(DATE '1994-09-27')</code> leads to 270. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFMONTH(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a month from a SQL date (an integer between 1 and 31). Equivalent to <code>EXTRACT(DAY FROM date)</code>. E.g. <code>DAYOFMONTH(DATE '1994-09-27')</code> leads to 27. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
DAYOFWEEK(date)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the day of a week from a SQL date (an integer between 1 and 7; Sunday = 1). Equivalent to <code>EXTRACT(DOW FROM date)</code>. E.g. <code>DAYOFWEEK(DATE '1994-09-27')</code> leads to 3. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
HOUR(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hour of a day from a SQL timestamp (an integer between 0 and 23). Equivalent to <code>EXTRACT(HOUR FROM timestamp)</code>. E.g. <code>HOUR(TIMESTAMP '1994-09-27 13:14:15')</code> leads to 13. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
MINUTE(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the minute of an hour from a SQL timestamp (an integer between 0 and 59). Equivalent to <code>EXTRACT(MINUTE FROM timestamp)</code>. E.g. <code>MINUTE(TIMESTAMP '1994-09-27 13:14:15')</code> leads to 14. </p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SECOND(timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the second of a minute from a SQL timestamp (an integer between 0 and 59). Equivalent to <code>EXTRACT(SECOND FROM timestamp)</code>. E.g. <code>SECOND(TIMESTAMP '1994-09-27 13:14:15')</code> leads to 15. </p>
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

    <tr>
      <td>
        {% highlight text %}
DATE_FORMAT(timestamp, format)
{% endhighlight %}
      </td>
      <td>
        <p>Formats <code>timestamp</code> as a string using a specified <code>format</code> string. The format must be compatible with MySQL's date formatting syntax as used by the <code>date_parse</code> function. The format specification is given in the <a href="#date-format-specifier">Date Format Specifier table</a> below.</p>
        <p>For example <code>DATE_FORMAT(ts, '%Y, %d %M')</code> results in strings formatted as <code>"2017, 05 May"</code>.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
TIMESTAMPADD(unit, interval, timestamp)
{% endhighlight %}
      </td>
      <td>
        <p>Adds a (signed) integer interval to a timestamp. The unit for the interval is given by the unit argument, which should be one of the following values: <code>SECOND</code>, <code>MINUTE</code>, <code>HOUR</code>, <code>DAY</code>, <code>WEEK</code>, <code>MONTH</code>, <code>QUARTER</code>, or <code>YEAR</code>. E.g. <code>TIMESTAMPADD(WEEK, 1, '2003-01-02')</code> leads to <code>2003-01-09</code>.</p>
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
        <p>Returns the number of input rows for which <i>value</i> is not null. Use <code>COUNT(DISTINCT value)</code> for the number of unique values in the column or expression.</p>
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
        <p>Returns the sum of <i>numeric</i> across all input values. Use <code>SUM(DISTINCT value)</code> for the sum of unique values in the column or expression.</p>
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
    <tr>
      <td>
        {% highlight text %}
STDDEV_POP(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>

<tr>
      <td>
        {% highlight text %}
STDDEV_SAMP(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample standard deviation of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
VAR_POP(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the population variance (square of the population standard deviation) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
VAR_SAMP(value)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the sample variance (square of the sample standard deviation) of the numeric field across all input values.</p>
      </td>
    </tr>

    <tr>
      <td>
          {% highlight text %}
COLLECT(value)
{% endhighlight %}
      </td>
      <td>
          <p>Returns a multiset of the <i>value</i>s. null input <i>value</i> will be ignored. Return an empty multiset if only null values are added. </p>
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
      <th class="text-left" style="width: 40%">Value constructor functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight text %}
(value, [, value]*)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a row from a list of values.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
ROW(value, [, value]*)
{% endhighlight %}
      </td>
      <td>
        <p>Creates a row from a list of values.</p>
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

    <tr>
      <td>
        {% highlight text %}
MAP ‘[’ key, value [, key, value ]* ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Creates a map from a list of key-value pairs.</p>
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
ELEMENT(ARRAY)
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
      <th class="text-left" style="width: 40%">Map functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>

    <tr>
      <td>
        {% highlight text %}
CARDINALITY(MAP)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the number of entries of a map.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
map ‘[’ key ‘]’
{% endhighlight %}
      </td>
      <td>
        <p>Returns the value specified by a particular key in a map.</p>
      </td>
    </tr>
  </tbody>
</table>

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Hash functions</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        {% highlight text %}
MD5(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the MD5 hash of the <i>string</i> argument as a string of 32 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SHA1(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-1 hash of the <i>string</i> argument as a string of 40 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
SHA224(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-224 hash of the <i>string</i> argument as a string of 56 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>    
    
    <tr>
      <td>
        {% highlight text %}
SHA256(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-256 hash of the <i>string</i> argument as a string of 64 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>
    
    <tr>
      <td>
        {% highlight text %}
SHA384(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-384 hash of the <i>string</i> argument as a string of 96 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>  

    <tr>
      <td>
        {% highlight text %}
SHA512(string)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the SHA-512 hash of the <i>string</i> argument as a string of 128 hexadecimal digits; null if <i>string</i> is null.</p>
      </td>
    </tr>

    <tr>
      <td>
        {% highlight text %}
SHA2(string, hashLength)
{% endhighlight %}
      </td>
      <td>
        <p>Returns the hash using the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, or SHA-512). The first argument <i>string</i> is the string to be hashed. <i>hashLength</i> is the bit length of the result (either 224, 256, 384, or 512). Returns <i>null</i> if <i>string</i> or <i>hashLength</i> is <i>null</i>.
        </p>
      </td>
    </tr>
  </tbody>
</table>

### Unsupported Functions

The following functions are not supported yet:

- Binary string operators and functions
- System functions

{% top %}

Reserved Keywords
-----------------

Although not every SQL feature is implemented yet, some string combinations are already reserved as keywords for future use. If you want to use one of the following strings as a field name, make sure to surround them with backticks (e.g. `` `value` ``, `` `count` ``).

{% highlight sql %}

A, ABS, ABSOLUTE, ACTION, ADA, ADD, ADMIN, AFTER, ALL, ALLOCATE, ALLOW, ALTER, ALWAYS, AND, ANY, ARE, ARRAY, AS, ASC, ASENSITIVE, ASSERTION, ASSIGNMENT, ASYMMETRIC, AT, ATOMIC, ATTRIBUTE, ATTRIBUTES, AUTHORIZATION, AVG, BEFORE, BEGIN, BERNOULLI, BETWEEN, BIGINT, BINARY, BIT, BLOB, BOOLEAN, BOTH, BREADTH, BY, C, CALL, CALLED, CARDINALITY, CASCADE, CASCADED, CASE, CAST, CATALOG, CATALOG_NAME, CEIL, CEILING, CENTURY, CHAIN, CHAR, CHARACTER, CHARACTERISTICS, CHARACTERS, CHARACTER_LENGTH, CHARACTER_SET_CATALOG, CHARACTER_SET_NAME, CHARACTER_SET_SCHEMA, CHAR_LENGTH, CHECK, CLASS_ORIGIN, CLOB, CLOSE, COALESCE, COBOL, COLLATE, COLLATION, COLLATION_CATALOG, COLLATION_NAME, COLLATION_SCHEMA, COLLECT, COLUMN, COLUMN_NAME, COMMAND_FUNCTION, COMMAND_FUNCTION_CODE, COMMIT, COMMITTED, CONDITION, CONDITION_NUMBER, CONNECT, CONNECTION, CONNECTION_NAME, CONSTRAINT, CONSTRAINTS, CONSTRAINT_CATALOG, CONSTRAINT_NAME, CONSTRAINT_SCHEMA, CONSTRUCTOR, CONTAINS, CONTINUE, CONVERT, CORR, CORRESPONDING, COUNT, COVAR_POP, COVAR_SAMP, CREATE, CROSS, CUBE, CUME_DIST, CURRENT, CURRENT_CATALOG, CURRENT_DATE, CURRENT_DEFAULT_TRANSFORM_GROUP, CURRENT_PATH, CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TRANSFORM_GROUP_FOR_TYPE, CURRENT_USER, CURSOR, CURSOR_NAME, CYCLE, DATA, DATABASE, DATE, DATETIME_INTERVAL_CODE, DATETIME_INTERVAL_PRECISION, DAY, DEALLOCATE, DEC, DECADE, DECIMAL, DECLARE, DEFAULT, DEFAULTS, DEFERRABLE, DEFERRED, DEFINED, DEFINER, DEGREE, DELETE, DENSE_RANK, DEPTH, DEREF, DERIVED, DESC, DESCRIBE, DESCRIPTION, DESCRIPTOR, DETERMINISTIC, DIAGNOSTICS, DISALLOW, DISCONNECT, DISPATCH, DISTINCT, DOMAIN, DOUBLE, DOW, DOY, DROP, DYNAMIC, DYNAMIC_FUNCTION, DYNAMIC_FUNCTION_CODE, EACH, ELEMENT, ELSE, END, END-EXEC, EPOCH, EQUALS, ESCAPE, EVERY, EXCEPT, EXCEPTION, EXCLUDE, EXCLUDING, EXEC, EXECUTE, EXISTS, EXP, EXPLAIN, EXTEND, EXTERNAL, EXTRACT, FALSE, FETCH, FILTER, FINAL, FIRST, FIRST_VALUE, FLOAT, FLOOR, FOLLOWING, FOR, FOREIGN, FORTRAN, FOUND, FRAC_SECOND, FREE, FROM, FULL, FUNCTION, FUSION, G, GENERAL, GENERATED, GET, GLOBAL, GO, GOTO, GRANT, GRANTED, GROUP, GROUPING, HAVING, HIERARCHY, HOLD, HOUR, IDENTITY, IMMEDIATE, IMPLEMENTATION, IMPORT, IN, INCLUDING, INCREMENT, INDICATOR, INITIALLY, INNER, INOUT, INPUT, INSENSITIVE, INSERT, INSTANCE, INSTANTIABLE, INT, INTEGER, INTERSECT, INTERSECTION, INTERVAL, INTO, INVOKER, IS, ISOLATION, JAVA, JOIN, K, KEY, KEY_MEMBER, KEY_TYPE, LABEL, LANGUAGE, LARGE, LAST, LAST_VALUE, LATERAL, LEADING, LEFT, LENGTH, LEVEL, LIBRARY, LIKE, LIMIT, LN, LOCAL, LOCALTIME, LOCALTIMESTAMP, LOCATOR, LOWER, M, MAP, MATCH, MATCHED, MAX, MAXVALUE, MEMBER, MERGE, MESSAGE_LENGTH, MESSAGE_OCTET_LENGTH, MESSAGE_TEXT, METHOD, MICROSECOND, MILLENNIUM, MIN, MINUTE, MINVALUE, MOD, MODIFIES, MODULE, MONTH, MORE, MULTISET, MUMPS, NAME, NAMES, NATIONAL, NATURAL, NCHAR, NCLOB, NESTING, NEW, NEXT, NO, NONE, NORMALIZE, NORMALIZED, NOT, NULL, NULLABLE, NULLIF, NULLS, NUMBER, NUMERIC, OBJECT, OCTETS, OCTET_LENGTH, OF, OFFSET, OLD, ON, ONLY, OPEN, OPTION, OPTIONS, OR, ORDER, ORDERING, ORDINALITY, OTHERS, OUT, OUTER, OUTPUT, OVER, OVERLAPS, OVERLAY, OVERRIDING, PAD, PARAMETER, PARAMETER_MODE, PARAMETER_NAME, PARAMETER_ORDINAL_POSITION, PARAMETER_SPECIFIC_CATALOG, PARAMETER_SPECIFIC_NAME, PARAMETER_SPECIFIC_SCHEMA, PARTIAL, PARTITION, PASCAL, PASSTHROUGH, PATH, PERCENTILE_CONT, PERCENTILE_DISC, PERCENT_RANK, PLACING, PLAN, PLI, POSITION, POWER, PRECEDING, PRECISION, PREPARE, PRESERVE, PRIMARY, PRIOR, PRIVILEGES, PROCEDURE, PUBLIC, QUARTER, RANGE, RANK, READ, READS, REAL, RECURSIVE, REF, REFERENCES, REFERENCING, REGR_AVGX, REGR_AVGY, REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX, REGR_SXY, REGR_SYY, RELATIVE, RELEASE, REPEATABLE, RESET, RESTART, RESTRICT, RESULT, RETURN, RETURNED_CARDINALITY, RETURNED_LENGTH, RETURNED_OCTET_LENGTH, RETURNED_SQLSTATE, RETURNS, REVOKE, RIGHT, ROLE, ROLLBACK, ROLLUP, ROUTINE, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_SCHEMA, ROW, ROWS, ROW_COUNT, ROW_NUMBER, SAVEPOINT, SCALE, SCHEMA, SCHEMA_NAME, SCOPE, SCOPE_CATALOGS, SCOPE_NAME, SCOPE_SCHEMA, SCROLL, SEARCH, SECOND, SECTION, SECURITY, SELECT, SELF, SENSITIVE, SEQUENCE, SERIALIZABLE, SERVER, SERVER_NAME, SESSION, SESSION_USER, SET, SETS, SIMILAR, SIMPLE, SIZE, SMALLINT, SOME, SOURCE, SPACE, SPECIFIC, SPECIFICTYPE, SPECIFIC_NAME, SQL, SQLEXCEPTION, SQLSTATE, SQLWARNING, SQL_TSI_DAY, SQL_TSI_FRAC_SECOND, SQL_TSI_HOUR, SQL_TSI_MICROSECOND, SQL_TSI_MINUTE, SQL_TSI_MONTH, SQL_TSI_QUARTER, SQL_TSI_SECOND, SQL_TSI_WEEK, SQL_TSI_YEAR, SQRT, START, STATE, STATEMENT, STATIC, STDDEV_POP, STDDEV_SAMP, STREAM, STRUCTURE, STYLE, SUBCLASS_ORIGIN, SUBMULTISET, SUBSTITUTE, SUBSTRING, SUM, SYMMETRIC, SYSTEM, SYSTEM_USER, TABLE, TABLESAMPLE, TABLE_NAME, TEMPORARY, THEN, TIES, TIME, TIMESTAMP, TIMESTAMPADD, TIMESTAMPDIFF, TIMEZONE_HOUR, TIMEZONE_MINUTE, TINYINT, TO, TOP_LEVEL_COUNT, TRAILING, TRANSACTION, TRANSACTIONS_ACTIVE, TRANSACTIONS_COMMITTED, TRANSACTIONS_ROLLED_BACK, TRANSFORM, TRANSFORMS, TRANSLATE, TRANSLATION, TREAT, TRIGGER, TRIGGER_CATALOG, TRIGGER_NAME, TRIGGER_SCHEMA, TRIM, TRUE, TYPE, UESCAPE, UNBOUNDED, UNCOMMITTED, UNDER, UNION, UNIQUE, UNKNOWN, UNNAMED, UNNEST, UPDATE, UPPER, UPSERT, USAGE, USER, USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_CODE, USER_DEFINED_TYPE_NAME, USER_DEFINED_TYPE_SCHEMA, USING, VALUE, VALUES, VARBINARY, VARCHAR, VARYING, VAR_POP, VAR_SAMP, VERSION, VIEW, WEEK, WHEN, WHENEVER, WHERE, WIDTH_BUCKET, WINDOW, WITH, WITHIN, WITHOUT, WORK, WRAPPER, WRITE, XML, YEAR, ZONE

{% endhighlight %}

#### Date Format Specifier

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Specifier</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr><td>{% highlight text %}%a{% endhighlight %}</td>
  <td>Abbreviated weekday name (<code>Sun</code> .. <code>Sat</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%b{% endhighlight %}</td>
  <td>Abbreviated month name (<code>Jan</code> .. <code>Dec</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%c{% endhighlight %}</td>
  <td>Month, numeric (<code>1</code> .. <code>12</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%D{% endhighlight %}</td>
  <td>Day of the month with English suffix (<code>0th</code>, <code>1st</code>, <code>2nd</code>, <code>3rd</code>, ...)</td>
  </tr>
  <tr><td>{% highlight text %}%d{% endhighlight %}</td>
  <td>Day of the month, numeric (<code>01</code> .. <code>31</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%e{% endhighlight %}</td>
  <td>Day of the month, numeric (<code>1</code> .. <code>31</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%f{% endhighlight %}</td>
  <td>Fraction of second (6 digits for printing: <code>000000</code> .. <code>999000</code>; 1 - 9 digits for parsing: <code>0</code> .. <code>999999999</code>) (Timestamp is truncated to milliseconds.) </td>
  </tr>
  <tr><td>{% highlight text %}%H{% endhighlight %}</td>
  <td>Hour (<code>00</code> .. <code>23</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%h{% endhighlight %}</td>
  <td>Hour (<code>01</code> .. <code>12</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%I{% endhighlight %}</td>
  <td>Hour (<code>01</code> .. <code>12</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%i{% endhighlight %}</td>
  <td>Minutes, numeric (<code>00</code> .. <code>59</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%j{% endhighlight %}</td>
  <td>Day of year (<code>001</code> .. <code>366</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%k{% endhighlight %}</td>
  <td>Hour (<code>0</code> .. <code>23</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%l{% endhighlight %}</td>
  <td>Hour (<code>1</code> .. <code>12</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%M{% endhighlight %}</td>
  <td>Month name (<code>January</code> .. <code>December</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%m{% endhighlight %}</td>
  <td>Month, numeric (<code>01</code> .. <code>12</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%p{% endhighlight %}</td>
  <td><code>AM</code> or <code>PM</code></td>
  </tr>
  <tr><td>{% highlight text %}%r{% endhighlight %}</td>
  <td>Time, 12-hour (<code>hh:mm:ss</code> followed by <code>AM</code> or <code>PM</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%S{% endhighlight %}</td>
  <td>Seconds (<code>00</code> .. <code>59</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%s{% endhighlight %}</td>
  <td>Seconds (<code>00</code> .. <code>59</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%T{% endhighlight %}</td>
  <td>Time, 24-hour (<code>hh:mm:ss</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%U{% endhighlight %}</td>
  <td>Week (<code>00</code> .. <code>53</code>), where Sunday is the first day of the week</td>
  </tr>
  <tr><td>{% highlight text %}%u{% endhighlight %}</td>
  <td>Week (<code>00</code> .. <code>53</code>), where Monday is the first day of the week</td>
  </tr>
  <tr><td>{% highlight text %}%V{% endhighlight %}</td>
  <td>Week (<code>01</code> .. <code>53</code>), where Sunday is the first day of the week; used with <code>%X</code></td>
  </tr>
  <tr><td>{% highlight text %}%v{% endhighlight %}</td>
  <td>Week (<code>01</code> .. <code>53</code>), where Monday is the first day of the week; used with <code>%x</code></td>
  </tr>
  <tr><td>{% highlight text %}%W{% endhighlight %}</td>
  <td>Weekday name (<code>Sunday</code> .. <code>Saturday</code>)</td>
  </tr>
  <tr><td>{% highlight text %}%w{% endhighlight %}</td>
  <td>Day of the week (<code>0</code> .. <code>6</code>), where Sunday is the first day of the week</td>
  </tr>
  <tr><td>{% highlight text %}%X{% endhighlight %}</td>
  <td>Year for the week where Sunday is the first day of the week, numeric, four digits; used with <code>%V</code></td>
  </tr>
  <tr><td>{% highlight text %}%x{% endhighlight %}</td>
  <td>Year for the week, where Monday is the first day of the week, numeric, four digits; used with <code>%v</code></td>
  </tr>
  <tr><td>{% highlight text %}%Y{% endhighlight %}</td>
  <td>Year, numeric, four digits</td>
  </tr>
  <tr><td>{% highlight text %}%y{% endhighlight %}</td>
  <td>Year, numeric (two digits) </td>
  </tr>
  <tr><td>{% highlight text %}%%{% endhighlight %}</td>
  <td>A literal <code>%</code> character</td>
  </tr>
  <tr><td>{% highlight text %}%x{% endhighlight %}</td>
  <td><code>x</code>, for any <code>x</code> not listed above</td>
  </tr>
  </tbody>
</table>

{% top %}


