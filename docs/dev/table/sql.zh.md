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

This is a complete list of Data Definition Language (DDL) and Data Manipulation Language (DML) constructs supported in Flink.
* This will be replaced by the TOC
{:toc}

## Query
SQL queries are specified with the `sqlQuery()` method of the `TableEnvironment`. The method returns the result of the SQL query as a `Table`. A `Table` can be used in [subsequent SQL and Table API queries](common.html#mixing-table-api-and-sql), be [converted into a DataSet or DataStream](common.html#integration-with-datastream-and-dataset-api), or [written to a TableSink](common.html#emit-a-table)). SQL and Table API queries can be seamlessly mixed and are holistically optimized and translated into a single program.

In order to access a table in a SQL query, it must be [registered in the TableEnvironment](common.html#register-tables-in-the-catalog). A table can be registered from a [TableSource](common.html#register-a-tablesource), [Table](common.html#register-a-table), [CREATE TABLE statement](#create-table), [DataStream, or DataSet](common.html#register-a-datastream-or-dataset-as-table). Alternatively, users can also [register external catalogs in a TableEnvironment](common.html#register-an-external-catalog) to specify the location of the data sources.

For convenience `Table.toString()` automatically registers the table under a unique name in its `TableEnvironment` and returns the name. Hence, `Table` objects can be directly inlined into SQL queries (by string concatenation) as shown in the examples below.

**Note:** Flink's SQL support is not yet feature complete. Queries that include unsupported SQL features cause a `TableException`. The supported features of SQL on batch and streaming tables are listed in the following sections.

### Specifying a Query

The following examples show how to specify a SQL queries on registered and inlined tables.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);

// SQL query with an inlined (unregistered) table
Table table = tableEnv.fromDataStream(ds, "user, product, amount");
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
val tableEnv = StreamTableEnvironment.create(env)

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
val csvSink: CsvTableSink = new CsvTableSink("/path/to/file", ...)
val fieldNames: Array[String] = Array("product", "amount")
val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT)
tableEnv.registerTableSink("RubberOrders", fieldNames, fieldTypes, csvSink)
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>


<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# SQL query with an inlined (unregistered) table
# elements data type: BIGINT, STRING, BIGINT
table = table_env.from_elements(..., ['user', 'product', 'amount'])
result = table_env \
    .sql_query("SELECT SUM(amount) FROM %s WHERE product LIKE '%%Rubber%%'" % table)

# SQL update with a registered table
# create and register a TableSink
table_env.register_table("Orders", table)
field_names = ["product", "amount"]
field_types = [DataTypes.STRING(), DataTypes.BIGINT()]
csv_sink = CsvTableSink(field_names, field_types, "/path/to/file", ...)
table_env.register_table_sink("RubberOrders", csv_sink)
# run a SQL update query on the Table and emit the result to the TableSink
table_env \
    .sql_update("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

### Supported Syntax

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
  [ matchRecognize ]
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

matchRecognize:
      MATCH_RECOGNIZE '('
      [ PARTITION BY expression [, expression ]* ]
      [ ORDER BY orderItem [, orderItem ]* ]
      [ MEASURES measureColumn [, measureColumn ]* ]
      [ ONE ROW PER MATCH ]
      [ AFTER MATCH
            ( SKIP TO NEXT ROW
            | SKIP PAST LAST ROW
            | SKIP TO FIRST variable
            | SKIP TO LAST variable
            | SKIP TO variable )
      ]
      PATTERN '(' pattern ')'
      [ WITHIN intervalLiteral ]
      DEFINE variable AS condition [, variable AS condition ]*
      ')'

measureColumn:
      expression AS alias

pattern:
      patternTerm [ '|' patternTerm ]*

patternTerm:
      patternFactor [ patternFactor ]*

patternFactor:
      variable [ patternQuantifier ]

patternQuantifier:
      '*'
  |   '*?'
  |   '+'
  |   '+?'
  |   '?'
  |   '??'
  |   '{' { [ minRepeat ], [ maxRepeat ] } '}' ['?']
  |   '{' repeat '}'

{% endhighlight %}

Flink SQL uses a lexical policy for identifier (table, attribute, function names) similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. <code>"SELECT a AS `my field` FROM t"</code>).

String literals must be enclosed in single quotes (e.g., `SELECT 'Hello World'`). Duplicate a single quote for escaping (e.g., `SELECT 'It''s me.'`). Unicode characters are supported in string literals. If explicit unicode code points are required, use the following syntax:

- Use the backslash (`\`) as escaping character (default): `SELECT U&'\263A'`
- Use a custom escaping character: `SELECT U&'#263A' UESCAPE '#'`

{% top %}

### Operations

#### Show and Use

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
        <strong>Show</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Show all catalogs</p>
{% highlight sql %}
SHOW CATALOGS;
{% endhighlight %}
		<p>Show all databases in the current catalog</p>
{% highlight sql %}
SHOW DATABASES;
{% endhighlight %}
		<p>Show all tables in the current database in the current catalog</p>
{% highlight sql %}
SHOW TABLES;
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td>
        <strong>Use</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
			<p>Set current catalog for the session </p>
{% highlight sql %}
USE CATALOG mycatalog;
{% endhighlight %}
            <p>Set current database of the current catalog for the session</p>
{% highlight sql %}
USE mydatabase;
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>
</div>

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

#### Aggregations

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
        <p><b>Note:</b> GroupBy on a streaming table produces an updating result. See the <a href="streaming/dynamic_tables.html">Dynamic Tables Streaming Concepts</a> page for details.
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
        <p><b>Note:</b> All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single <a href="streaming/time_attributes.html">time attribute</a></p>
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
       <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct fields. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
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

#### Joins

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
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
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
        <p><b>Note:</b> For streaming queries the required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Time-windowed Join</strong><br>
        <span class="label label-primary">Batch</span>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><b>Note:</b> Time-windowed joins are a subset of regular joins that can be processed in a streaming fashion.</p>

        <p>A time-windowed join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (<code>&lt;, &lt;=, &gt;=, &gt;</code>), a <code>BETWEEN</code> predicate, or a single equality predicate that compares <a href="streaming/time_attributes.html">time attributes</a> of the same type (i.e., processing time or event time) of both input tables.</p>
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
        <strong>Join with Table Function</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
    	<td>
        <p>Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function.</p>
        <p>User-defined table functions (UDTFs) must be registered before. See the <a href="udfs.html">UDF documentation</a> for details on how to specify and register UDTFs. </p>

        <p><b>Inner Join</b></p>
        <p>A row of the left (outer) table is dropped, if its table function call returns an empty result.</p>
{% highlight sql %}
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags)) t AS tag
{% endhighlight %}

        <p><b>Left Outer Join</b></p>
        <p>If a table function call returns an empty result, the corresponding outer row is preserved and the result padded with null values.</p>
{% highlight sql %}
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) t AS tag ON TRUE
{% endhighlight %}

        <p><b>Note:</b> Currently, only literal <code>TRUE</code> is supported as predicate for a left outer join against a lateral table.</p>
      </td>
    </tr>
   <tr>
      <td>
        <strong>Join with Temporal Table Function</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><a href="streaming/temporal_tables.html">Temporal tables</a> are tables that track changes over time.</p>
        <p>A <a href="streaming/temporal_tables.html#temporal-table-functions">Temporal table function</a> provides access to the state of a temporal table at a specific point in time.
        The syntax to join a table with a temporal table function is the same as in <i>Join with Table Function</i>.</p>

        <p><b>Note:</b> Currently only inner joins with temporal tables are supported.</p>

        <p>Assuming <i>Rates</i> is a <a href="streaming/temporal_tables.html#temporal-table-functions">temporal table function</a>, the join can be expressed in SQL as follows:</p>
{% highlight sql %}
SELECT
  o_amount, r_rate
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
{% endhighlight %}
        <p>For more information please check the more detailed <a href="streaming/temporal_tables.html">temporal tables concept description</a>.</p>
      </td>
    </tr>
    <tr>
      <td>
        <strong>Join with Temporal Table</strong><br>
        <span class="label label-primary">Batch</span> <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p><a href="streaming/temporal_tables.html">Temporal Tables</a> are tables that track changes over time.
        A <a href="streaming/temporal_tables.html#temporal-table">Temporal Table</a> provides access to the versions of a temporal table at a specific point in time.</p>

        <p>Only inner and left joins with processing-time temporal tables are supported.</p>
        <p>The following example assumes that <strong>LatestRates</strong> is a <a href="streaming/temporal_tables.html#temporal-table">Temporal Table</a> which is materialized with the latest rate.</p>
    {% highlight sql %}
    SELECT
    o.amout, o.currency, r.rate, o.amount * r.rate
    FROM
    Orders AS o
    JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
    ON r.currency = o.currency
    {% endhighlight %}
        <p>For more information please check the more detailed <a href="streaming/temporal_tables.html">Temporal Tables</a> concept description.</p>
        <p>Only supported in Blink planner.</p>
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

#### Set Operations

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
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
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
        <p><b>Note:</b> For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. See <a href="streaming/query_configuration.html">Query Configuration</a> for details.</p>
      </td>
    </tr>
  </tbody>
</table>
</div>

{% top %}

#### OrderBy & Limit

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
<b>Note:</b> The result of streaming queries must be primarily sorted on an ascending <a href="streaming/time_attributes.html">time attribute</a>. Additional sorting attributes are supported.

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
<b>Note:</b> The LIMIT clause requires an ORDER BY clause.
{% highlight sql %}
SELECT *
FROM Orders
ORDER BY orderTime
LIMIT 3
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

#### Top-N

<span class="label label-danger">Attention</span> Top-N is only supported in Blink planner.

Top-N queries ask for the N smallest or largest values ordered by columns. Both smallest and largest values sets are considered Top-N queries. Top-N queries are useful in cases where the need is to display only the N bottom-most or the N top-
most records from batch/streaming table on a condition. This result set can be used for further analysis.

Flink uses the combination of a OVER window clause and a filter condition to express a Top-N query. With the power of OVER window `PARTITION BY` clause, Flink also supports per group Top-N. For example, the top five products per category that have the maximum sales in realtime. Top-N queries are supported for SQL on batch and streaming tables.

The following shows the syntax of the TOP-N statement:

{% highlight sql %}
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]
{% endhighlight %}

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one, according to the ordering of rows within the partition. Currently, we only support `ROW_NUMBER` as the over window function. In the future, we will support `RANK()` and `DENSE_RANK()`.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns. Each partition will have a Top-N result.
- `ORDER BY col1 [asc|desc][, col2 [asc|desc]...]`: Specifies the ordering columns. The ordering directions can be different on different columns.
- `WHERE rownum <= N`: The `rownum <= N` is required for Flink to recognize this query is a Top-N query. The N represents the N smallest or largest records will be retained.
- `[AND conditions]`: It is free to add other conditions in the where clause, but the other conditions can only be combined with `rownum <= N` using `AND` conjunction.

<span class="label label-danger">Attention in Streaming Mode</span> The TopN query is <span class="label label-info">Result Updating</span>. Flink SQL will sort the input data stream according to the order key, so if the top N records have been changed, the changed ones will be sent as retraction/update records to downstream.
It is recommended to use a storage which supports updating as the sink of Top-N query. In addition, if the top N records need to be stored in external storage, the result table should have the same unique key with the Top-N query.

The unique keys of Top-N query is the combination of partition columns and rownum column. Top-N query can also derive the unique key of upstream. Take following job as an example, say `product_id` is the unique key of the `ShopSales`, then the unique keys of the Top-N query are [`category`, `rownum`] and [`product_id`].

The following examples show how to specify SQL queries with Top-N on streaming tables. This is an example to get "the top five products per category that have the maximum sales in realtime" we mentioned above.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<String, String, String, Long>> ds = env.addSource(...);
// register the DataStream as table "ShopSales"
tableEnv.registerDataStream("ShopSales", ds, "product_id, category, product_name, sales");

// select top-5 products per category which have the maximum sales.
Table result1 = tableEnv.sqlQuery(
  "SELECT * " +
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
  "   FROM ShopSales)" +
  "WHERE row_num <= 5");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(String, String, String, Long)] = env.addSource(...)
// register the DataStream under the name "ShopSales"
tableEnv.registerDataStream("ShopSales", ds, 'product_id, 'category, 'product_name, 'sales)


// select top-5 products per category which have the maximum sales.
val result1 = tableEnv.sqlQuery(
    """
      |SELECT *
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num
      |   FROM ShopSales)
      |WHERE row_num <= 5
    """.stripMargin)
{% endhighlight %}
</div>
</div>

##### No Ranking Output Optimization

As described above, the `rownum` field will be written into the result table as one field of the unique key, which may lead to a lot of records being written to the result table. For example, when the record (say `product-1001`) of ranking 9 is updated and its rank is upgraded to 1, all the records from ranking 1 ~ 9 will be output to the result table as update messages. If the result table receives too many data, it will become the bottleneck of the SQL job.

The optimization way is omitting rownum field in the outer SELECT clause of the Top-N query. This is reasonable because the number of the top N records is usually not large, thus the consumers can sort the records themselves quickly. Without rownum field, in the example above, only the changed record (`product-1001`) needs to be sent to downstream, which can reduce much IO to the result table.

The following example shows how to optimize the above Top-N example in this way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<String, String, String, Long>> ds = env.addSource(...);
// register the DataStream as table "ShopSales"
tableEnv.registerDataStream("ShopSales", ds, "product_id, category, product_name, sales");

// select top-5 products per category which have the maximum sales.
Table result1 = tableEnv.sqlQuery(
  "SELECT product_id, category, product_name, sales " + // omit row_num field in the output
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
  "   FROM ShopSales)" +
  "WHERE row_num <= 5");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(String, String, String, Long)] = env.addSource(...)
// register the DataStream under the name "ShopSales"
tableEnv.registerDataStream("ShopSales", ds, 'product_id, 'category, 'product_name, 'sales)


// select top-5 products per category which have the maximum sales.
val result1 = tableEnv.sqlQuery(
    """
      |SELECT product_id, category, product_name, sales  -- omit row_num field in the output
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num
      |   FROM ShopSales)
      |WHERE row_num <= 5
    """.stripMargin)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention in Streaming Mode</span> In order to output the above query to an external storage and have a correct result, the external storage must have the same unique key with the Top-N query. In the above example query, if the `product_id` is the unique key of the query, then the external table should also has `product_id` as the unique key.

{% top %}

#### Deduplication

<span class="label label-danger">Attention</span> Deduplication is only supported in Blink planner.

Deduplication is removing rows that duplicate over a set of columns, keeping only the first one or the last one. In some cases, the upstream ETL jobs are not end-to-end exactly-once, this may result in there are duplicate records in the sink in case of failover. However, the duplicate records will affect the correctness of downstream analytical jobs (e.g. `SUM`, `COUNT`). So a deduplication is needed before further analysis.

Flink uses `ROW_NUMBER()` to remove duplicates just like the way of Top-N query. In theory, deduplication is a special case of Top-N which the N is one and order by the processing time or event time.

The following shows the syntax of the Deduplication statement:

{% highlight sql %}
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1
{% endhighlight %}

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns, i.e. the deduplicate key.
- `ORDER BY time_attr [asc|desc]`: Specifies the ordering column, it must be a [time attribute](streaming/time_attributes.html). Currently only support [proctime attribute](streaming/time_attributes.html#processing-time). [Rowtime atttribute](streaming/time_attributes.html#event-time) will be supported in the future. Ordering by ASC means keeping the first row, ordering by DESC means keeping the last row.
- `WHERE rownum = 1`: The `rownum = 1` is required for Flink to recognize this query is deduplication.

The following examples show how to specify SQL queries with Deduplication on streaming tables.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// ingest a DataStream from an external source
DataStream<Tuple3<String, String, String, Integer>> ds = env.addSource(...);
// register the DataStream as table "Orders"
tableEnv.registerDataStream("Orders", ds, "order_id, user, product, number, proctime.proctime");

// remove duplicate rows on order_id and keep the first occurrence row,
// because there shouldn't be two orders with the same order_id.
Table result1 = tableEnv.sqlQuery(
  "SELECT order_id, user, product, number " +
  "FROM (" +
  "   SELECT *," +
  "       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) as row_num" +
  "   FROM Orders)" +
  "WHERE row_num = 1");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = TableEnvironment.getTableEnvironment(env)

// read a DataStream from an external source
val ds: DataStream[(String, String, String, Int)] = env.addSource(...)
// register the DataStream under the name "Orders"
tableEnv.registerDataStream("Orders", ds, 'order_id, 'user, 'product, 'number, 'proctime.proctime)

// remove duplicate rows on order_id and keep the first occurrence row,
// because there shouldn't be two orders with the same order_id.
val result1 = tableEnv.sqlQuery(
    """
      |SELECT order_id, user, product, number
      |FROM (
      |   SELECT *,
      |       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime DESC) as row_num
      |   FROM Orders)
      |WHERE row_num = 1
    """.stripMargin)
{% endhighlight %}
</div>
</div>

{% top %}

#### Insert

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

#### Group Windows

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

##### Time Attributes

For SQL queries on streaming tables, the `time_attr` argument of the group window function must refer to a valid time attribute that specifies the processing time or event time of rows. See the [documentation of time attributes](streaming/time_attributes.html) to learn how to define time attributes.

For SQL on batch tables, the `time_attr` argument of the group window function must be an attribute of type `TIMESTAMP`.

##### Selecting Group Window Start and End Timestamps

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
        <p><b>Note:</b> The exclusive upper bound timestamp <i>cannot</i> be used as a <a href="streaming/time_attributes.html">rowtime attribute</a> in subsequent time-based operations, such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>inclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
      <p>The resulting attribute is a <a href="streaming/time_attributes.html">rowtime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns a <a href="streaming/time_attributes.html#processing-time">proctime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">time-windowed joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
  </tbody>
</table>

*Note:* Auxiliary functions must be called with exactly same arguments as the group window function in the `GROUP BY` clause.

The following examples show how to specify SQL queries with group windows on streaming tables.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
val tableEnv = StreamTableEnvironment.create(env)

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

#### Pattern Recognition

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
        <strong>MATCH_RECOGNIZE</strong><br>
        <span class="label label-primary">Streaming</span>
      </td>
      <td>
        <p>Searches for a given pattern in a streaming table according to the <code>MATCH_RECOGNIZE</code> <a href="https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip">ISO standard</a>. This makes it possible to express complex event processing (CEP) logic in SQL queries.</p>
        <p>For a more detailed description, see the dedicated page for <a href="streaming/match_recognize.html">detecting patterns in tables</a>.</p>

{% highlight sql %}
SELECT T.aid, T.bid, T.cid
FROM MyTable
MATCH_RECOGNIZE (
  PARTITION BY userid
  ORDER BY proctime
  MEASURES
    A.id AS aid,
    B.id AS bid,
    C.id AS cid
  PATTERN (A B C)
  DEFINE
    A AS name = 'a',
    B AS name = 'b',
    C AS name = 'c'
) AS T
{% endhighlight %}
      </td>
    </tr>

  </tbody>
</table>
</div>

{% top %}

### Drop Table

{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

{% top %}

## DDL

DDLs are specified with the `sqlUpdate()` method of the `TableEnvironment`. The method returns nothing for a success table creation. A `Table` can be register into the [Catalog](catalogs.html) with a `CREATE TABLE` statement, then be referenced in the SQL queries in method `sqlQuery()` of `TableEnvironment`.

**Note:** Flink's DDL support is not yet feature complete. Queries that include unsupported SQL features cause a `TableException`. The supported features of SQL DDL on batch and streaming tables are listed in the following sections.

### Specifying a DDL

The following examples show how to specify a SQL DDL.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// SQL query with a registered table
// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// SQL query with a registered table
// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
// run a SQL query on the Table and retrieve the result as a new Table
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// SQL update with a registered table
// register a TableSink
tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ('connector.path'='/path/to/file' ...)");
// run a SQL update query on the Table and emit the result to the TableSink
tableEnv.sqlUpdate(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# SQL update with a registered table
# register a TableSink
table_env.sql_update("CREATE TABLE RubberOrders(product VARCHAR, amount INT) with (...)")
# run a SQL update query on the Table and emit the result to the TableSink
table_env \
    .sql_update("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
{% endhighlight %}
</div>
</div>

{% top %}

### Create Table

{% highlight sql %}
CREATE TABLE [catalog_name.][db_name.]table_name
  [(col_name1 col_type1 [COMMENT col_comment1], ...)]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name1, col_name2, ...)]
  WITH (key1=val1, key2=val2, ...)
{% endhighlight %}

Create a table with the given table properties. If a table with the same name already exists in the database, an exception is thrown.

**PARTITIONED BY**

Partition the created table by the specified columns. A directory is created for each partition if this table is used as a filesystem sink.

**WITH OPTIONS**

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector.

The key and value of expression `key1=val1` should both be string literal. See details in [Connect to External Systems](connect.html) for all the supported table properties of different connectors.

**Notes:** The table name can be of three formats: 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`. For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name"; for `db_name.table_name`, the table would be registered into the current catalog of the execution table environment and database named "db_name"; for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

**Notes:** The table registered with `CREATE TABLE` statement can be used as both table source and table sink, we can not decide if it is used as a source or sink until it is referenced in the DMLs.

{% top %}

### Drop Table

{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

{% top %}

## Data Types

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
| `Types.ROW`            | `ROW`                       | `org.apache.flink.types.Row` |

Generic types and (nested) composite types (e.g., POJOs, tuples, rows, Scala case classes) can be fields of a row as well.

Fields of composite types with arbitrary nesting can be accessed with [value access functions](functions.html#value-access-functions).

Generic types are treated as a black box and can be passed on or processed by [user-defined functions](udfs.html).

For DDLs, we support full data types defined in page [Data Types]({{ site.baseurl }}/dev/table/types.html).

**Notes:** Some of the data types are not supported in the sql query(the cast expression or literals). E.G. `STRING`, `BYTES`, `TIME(p) WITHOUT TIME ZONE`, `TIME(p) WITH LOCAL TIME ZONE`, `TIMESTAMP(p) WITHOUT TIME ZONE`, `TIMESTAMP(p) WITH LOCAL TIME ZONE`, `ARRAY`, `MULTISET`, `ROW`.

{% top %}

## Reserved Keywords

Although not every SQL feature is implemented yet, some string combinations are already reserved as keywords for future use. If you want to use one of the following strings as a field name, make sure to surround them with backticks (e.g. `` `value` ``, `` `count` ``).

{% highlight sql %}

A, ABS, ABSOLUTE, ACTION, ADA, ADD, ADMIN, AFTER, ALL, ALLOCATE, ALLOW, ALTER, ALWAYS, AND, ANY, ARE, ARRAY, AS, ASC, ASENSITIVE, ASSERTION, ASSIGNMENT, ASYMMETRIC, AT, ATOMIC, ATTRIBUTE, ATTRIBUTES, AUTHORIZATION, AVG, BEFORE, BEGIN, BERNOULLI, BETWEEN, BIGINT, BINARY, BIT, BLOB, BOOLEAN, BOTH, BREADTH, BY, C, CALL, CALLED, CARDINALITY, CASCADE, CASCADED, CASE, CAST, CATALOG, CATALOG_NAME, CEIL, CEILING, CENTURY, CHAIN, CHAR, CHARACTER, CHARACTERISTICS, CHARACTERS, CHARACTER_LENGTH, CHARACTER_SET_CATALOG, CHARACTER_SET_NAME, CHARACTER_SET_SCHEMA, CHAR_LENGTH, CHECK, CLASS_ORIGIN, CLOB, CLOSE, COALESCE, COBOL, COLLATE, COLLATION, COLLATION_CATALOG, COLLATION_NAME, COLLATION_SCHEMA, COLLECT, COLUMN, COLUMN_NAME, COMMAND_FUNCTION, COMMAND_FUNCTION_CODE, COMMIT, COMMITTED, CONDITION, CONDITION_NUMBER, CONNECT, CONNECTION, CONNECTION_NAME, CONSTRAINT, CONSTRAINTS, CONSTRAINT_CATALOG, CONSTRAINT_NAME, CONSTRAINT_SCHEMA, CONSTRUCTOR, CONTAINS, CONTINUE, CONVERT, CORR, CORRESPONDING, COUNT, COVAR_POP, COVAR_SAMP, CREATE, CROSS, CUBE, CUME_DIST, CURRENT, CURRENT_CATALOG, CURRENT_DATE, CURRENT_DEFAULT_TRANSFORM_GROUP, CURRENT_PATH, CURRENT_ROLE, CURRENT_SCHEMA, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_TRANSFORM_GROUP_FOR_TYPE, CURRENT_USER, CURSOR, CURSOR_NAME, CYCLE, DATA, DATABASE, DATE, DATETIME_INTERVAL_CODE, DATETIME_INTERVAL_PRECISION, DAY, DEALLOCATE, DEC, DECADE, DECIMAL, DECLARE, DEFAULT, DEFAULTS, DEFERRABLE, DEFERRED, DEFINED, DEFINER, DEGREE, DELETE, DENSE_RANK, DEPTH, DEREF, DERIVED, DESC, DESCRIBE, DESCRIPTION, DESCRIPTOR, DETERMINISTIC, DIAGNOSTICS, DISALLOW, DISCONNECT, DISPATCH, DISTINCT, DOMAIN, DOUBLE, DOW, DOY, DROP, DYNAMIC, DYNAMIC_FUNCTION, DYNAMIC_FUNCTION_CODE, EACH, ELEMENT, ELSE, END, END-EXEC, EPOCH, EQUALS, ESCAPE, EVERY, EXCEPT, EXCEPTION, EXCLUDE, EXCLUDING, EXEC, EXECUTE, EXISTS, EXP, EXPLAIN, EXTEND, EXTERNAL, EXTRACT, FALSE, FETCH, FILTER, FINAL, FIRST, FIRST_VALUE, FLOAT, FLOOR, FOLLOWING, FOR, FOREIGN, FORTRAN, FOUND, FRAC_SECOND, FREE, FROM, FULL, FUNCTION, FUSION, G, GENERAL, GENERATED, GET, GLOBAL, GO, GOTO, GRANT, GRANTED, GROUP, GROUPING, HAVING, HIERARCHY, HOLD, HOUR, IDENTITY, IMMEDIATE, IMPLEMENTATION, IMPORT, IN, INCLUDING, INCREMENT, INDICATOR, INITIALLY, INNER, INOUT, INPUT, INSENSITIVE, INSERT, INSTANCE, INSTANTIABLE, INT, INTEGER, INTERSECT, INTERSECTION, INTERVAL, INTO, INVOKER, IS, ISOLATION, JAVA, JOIN, K, KEY, KEY_MEMBER, KEY_TYPE, LABEL, LANGUAGE, LARGE, LAST, LAST_VALUE, LATERAL, LEADING, LEFT, LENGTH, LEVEL, LIBRARY, LIKE, LIMIT, LN, LOCAL, LOCALTIME, LOCALTIMESTAMP, LOCATOR, LOWER, M, MAP, MATCH, MATCHED, MAX, MAXVALUE, MEMBER, MERGE, MESSAGE_LENGTH, MESSAGE_OCTET_LENGTH, MESSAGE_TEXT, METHOD, MICROSECOND, MILLENNIUM, MIN, MINUTE, MINVALUE, MOD, MODIFIES, MODULE, MONTH, MORE, MULTISET, MUMPS, NAME, NAMES, NATIONAL, NATURAL, NCHAR, NCLOB, NESTING, NEW, NEXT, NO, NONE, NORMALIZE, NORMALIZED, NOT, NULL, NULLABLE, NULLIF, NULLS, NUMBER, NUMERIC, OBJECT, OCTETS, OCTET_LENGTH, OF, OFFSET, OLD, ON, ONLY, OPEN, OPTION, OPTIONS, OR, ORDER, ORDERING, ORDINALITY, OTHERS, OUT, OUTER, OUTPUT, OVER, OVERLAPS, OVERLAY, OVERRIDING, PAD, PARAMETER, PARAMETER_MODE, PARAMETER_NAME, PARAMETER_ORDINAL_POSITION, PARAMETER_SPECIFIC_CATALOG, PARAMETER_SPECIFIC_NAME, PARAMETER_SPECIFIC_SCHEMA, PARTIAL, PARTITION, PASCAL, PASSTHROUGH, PATH, PERCENTILE_CONT, PERCENTILE_DISC, PERCENT_RANK, PLACING, PLAN, PLI, POSITION, POWER, PRECEDING, PRECISION, PREPARE, PRESERVE, PRIMARY, PRIOR, PRIVILEGES, PROCEDURE, PUBLIC, QUARTER, RANGE, RANK, READ, READS, REAL, RECURSIVE, REF, REFERENCES, REFERENCING, REGR_AVGX, REGR_AVGY, REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX, REGR_SXY, REGR_SYY, RELATIVE, RELEASE, REPEATABLE, RESET, RESTART, RESTRICT, RESULT, RETURN, RETURNED_CARDINALITY, RETURNED_LENGTH, RETURNED_OCTET_LENGTH, RETURNED_SQLSTATE, RETURNS, REVOKE, RIGHT, ROLE, ROLLBACK, ROLLUP, ROUTINE, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_SCHEMA, ROW, ROWS, ROW_COUNT, ROW_NUMBER, SAVEPOINT, SCALE, SCHEMA, SCHEMA_NAME, SCOPE, SCOPE_CATALOGS, SCOPE_NAME, SCOPE_SCHEMA, SCROLL, SEARCH, SECOND, SECTION, SECURITY, SELECT, SELF, SENSITIVE, SEQUENCE, SERIALIZABLE, SERVER, SERVER_NAME, SESSION, SESSION_USER, SET, SETS, SIMILAR, SIMPLE, SIZE, SMALLINT, SOME, SOURCE, SPACE, SPECIFIC, SPECIFICTYPE, SPECIFIC_NAME, SQL, SQLEXCEPTION, SQLSTATE, SQLWARNING, SQL_TSI_DAY, SQL_TSI_FRAC_SECOND, SQL_TSI_HOUR, SQL_TSI_MICROSECOND, SQL_TSI_MINUTE, SQL_TSI_MONTH, SQL_TSI_QUARTER, SQL_TSI_SECOND, SQL_TSI_WEEK, SQL_TSI_YEAR, SQRT, START, STATE, STATEMENT, STATIC, STDDEV_POP, STDDEV_SAMP, STREAM, STRUCTURE, STYLE, SUBCLASS_ORIGIN, SUBMULTISET, SUBSTITUTE, SUBSTRING, SUM, SYMMETRIC, SYSTEM, SYSTEM_USER, TABLE, TABLESAMPLE, TABLE_NAME, TEMPORARY, THEN, TIES, TIME, TIMESTAMP, TIMESTAMPADD, TIMESTAMPDIFF, TIMEZONE_HOUR, TIMEZONE_MINUTE, TINYINT, TO, TOP_LEVEL_COUNT, TRAILING, TRANSACTION, TRANSACTIONS_ACTIVE, TRANSACTIONS_COMMITTED, TRANSACTIONS_ROLLED_BACK, TRANSFORM, TRANSFORMS, TRANSLATE, TRANSLATION, TREAT, TRIGGER, TRIGGER_CATALOG, TRIGGER_NAME, TRIGGER_SCHEMA, TRIM, TRUE, TYPE, UESCAPE, UNBOUNDED, UNCOMMITTED, UNDER, UNION, UNIQUE, UNKNOWN, UNNAMED, UNNEST, UPDATE, UPPER, UPSERT, USAGE, USER, USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_CODE, USER_DEFINED_TYPE_NAME, USER_DEFINED_TYPE_SCHEMA, USING, VALUE, VALUES, VARBINARY, VARCHAR, VARYING, VAR_POP, VAR_SAMP, VERSION, VIEW, WEEK, WHEN, WHENEVER, WHERE, WIDTH_BUCKET, WINDOW, WITH, WITHIN, WITHOUT, WORK, WRAPPER, WRITE, XML, YEAR, ZONE

{% endhighlight %}

{% top %}

