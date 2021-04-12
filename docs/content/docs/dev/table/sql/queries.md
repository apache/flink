---
title: "Queries"
weight: 2
type: docs
aliases:
  - /dev/table/sql/queries.html
  - /dev/table/sql.html
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

# Queries


`SELECT` statements and `VALUES` statements are specified with the `sqlQuery()` method of the `TableEnvironment`.
The method returns the result of the SELECT statement (or the VALUES statements) as a `Table`.
A `Table` can be used in [subsequent SQL and Table API queries]({{< ref "docs/dev/table/common" >}}#mixing-table-api-and-sql), be [converted into a DataStream]({{< ref "docs/dev/table/common" >}}#integration-with-datastream), or [written to a TableSink]({{< ref "docs/dev/table/common" >}}#emit-a-table).
SQL and Table API queries can be seamlessly mixed and are holistically optimized and translated into a single program.

In order to access a table in a SQL query, it must be [registered in the TableEnvironment]({{< ref "docs/dev/table/common" >}}#register-tables-in-the-catalog).
A table can be registered from a [TableSource]({{< ref "docs/dev/table/common" >}}#register-a-tablesource), [Table]({{< ref "docs/dev/table/common" >}}#register-a-table), [CREATE TABLE statement](#create-table), [DataStream]({{< ref "docs/dev/table/common" >}}#register-a-datastream).
Alternatively, users can also [register catalogs in a TableEnvironment]({{< ref "docs/dev/table/catalogs" >}}) to specify the location of the data sources.

For convenience, `Table.toString()` automatically registers the table under a unique name in its `TableEnvironment` and returns the name.
So, `Table` objects can be directly inlined into SQL queries as shown in the examples below.

**Note:** Queries that include unsupported SQL features cause a `TableException`.
The supported features of SQL on batch and streaming tables are listed in the following sections.

## Specifying a Query

The following examples show how to specify a SQL queries on registered and inlined tables.

{{< tabs "f5adf0e8-aae8-4eb4-84a7-ceb156d173e9" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// ingest a DataStream from an external source
DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);

// SQL query with an inlined (unregistered) table
Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
Table result = tableEnv.sqlQuery(
  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");

// SQL query with a registered table
// register the DataStream as view "Orders"
tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"));
// run a SQL query on the Table and retrieve the result as a new Table
Table result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// create and register a TableSink
final Schema schema = new Schema()
    .field("product", DataTypes.STRING())
    .field("amount", DataTypes.INT());

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(...)
    .withSchema(schema)
    .createTemporaryTable("RubberOrders");

// run an INSERT SQL on the Table and emit the result to the TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// read a DataStream from an external source
val ds: DataStream[(Long, String, Integer)] = env.addSource(...)

// SQL query with an inlined (unregistered) table
val table = ds.toTable(tableEnv, $"user", $"product", $"amount")
val result = tableEnv.sqlQuery(
  s"SELECT SUM(amount) FROM $table WHERE product LIKE '%Rubber%'")

// SQL query with a registered table
// register the DataStream under the name "Orders"
tableEnv.createTemporaryView("Orders", ds, $"user", $"product", $"amount")
// run a SQL query on the Table and retrieve the result as a new Table
val result2 = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")

// create and register a TableSink
val schema = new Schema()
    .field("product", DataTypes.STRING())
    .field("amount", DataTypes.INT())

tableEnv.connect(new FileSystem().path("/path/to/file"))
    .withFormat(...)
    .withSchema(schema)
    .createTemporaryTable("RubberOrders")

// run an INSERT SQL on the Table and emit the result to the TableSink
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# SQL query with an inlined (unregistered) table
# elements data type: BIGINT, STRING, BIGINT
table = table_env.from_elements(..., ['user', 'product', 'amount'])
result = table_env \
    .sql_query("SELECT SUM(amount) FROM %s WHERE product LIKE '%%Rubber%%'" % table)

# create and register a TableSink
t_env.connect(FileSystem().path("/path/to/file")))
    .with_format(Csv()
                 .field_delimiter(',')
                 .deriveSchema())
    .with_schema(Schema()
                 .field("product", DataTypes.STRING())
                 .field("amount", DataTypes.BIGINT()))
    .create_temporary_table("RubberOrders")

# run an INSERT SQL on the Table and emit the result to the TableSink
table_env \
    .execute_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Execute a Query


A SELECT statement or a VALUES statement can be executed to collect the content to local through the `TableEnvironment.executeSql()` method. The method returns the result of the SELECT statement (or the VALUES statement) as a `TableResult`. Similar to a SELECT statement, a `Table` object can be executed using the `Table.execute()` method to collect the content of the query to the local client.
`TableResult.collect()` method returns a closeable row iterator. The select job will not be finished unless all result data has been collected. We should actively close the job to avoid resource leak through the `CloseableIterator#close()` method. 
We can also print the select result to client console through the `TableResult.print()` method. The result data in `TableResult` can be accessed only once. Thus, `collect()` and `print()` must not be called after each other.

`TableResult.collect()` and `TableResult.print()` have slightly different behaviors under different checkpointing settings (to enable checkpointing for a streaming job, see [checkpointing config]({{< ref "docs/deployment/config" >}}#checkpointing)).
* For batch jobs or streaming jobs without checkpointing, `TableResult.collect()` and `TableResult.print()` have neither exactly-once nor at-least-once guarantee. Query results are immediately accessible by the clients once they're produced, but exceptions will be thrown when the job fails and restarts.
* For streaming jobs with exactly-once checkpointing, `TableResult.collect()` and `TableResult.print()` guarantee an end-to-end exactly-once record delivery. A result will be accessible by clients only after its corresponding checkpoint completes.
* For streaming jobs with at-least-once checkpointing, `TableResult.collect()` and `TableResult.print()` guarantee an end-to-end at-least-once record delivery. Query results are immediately accessible by the clients once they're produced, but it is possible for the same result to be delivered multiple times.

{{< tabs "88a003e1-16ea-43cc-9d42-d43ef1351e53" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// execute SELECT statement
TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM Orders");
// use try-with-resources statement to make sure the iterator will be closed automatically
try (CloseableIterator<Row> it = tableResult1.collect()) {
    while(it.hasNext()) {
        Row row = it.next();
        // handle row
    }
}

// execute Table
TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute();
tableResult2.print();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tableEnv = StreamTableEnvironment.create(env, settings)
// enable checkpointing
tableEnv.getConfig.getConfiguration.set(
  ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
tableEnv.getConfig.getConfiguration.set(
  ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))

tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// execute SELECT statement
val tableResult1 = tableEnv.executeSql("SELECT * FROM Orders")
val it = tableResult1.collect()
try while (it.hasNext) {
  val row = it.next
  // handle row
}
finally it.close() // close the iterator to avoid resource leak

// execute Table
val tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute()
tableResult2.print()

```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, settings)
# enable checkpointing
table_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10s")

table_env.execute_sql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

# execute SELECT statement
table_result1 = table_env.execute_sql("SELECT * FROM Orders")
table_result1.print()

# execute Table
table_result2 = table_env.sql_query("SELECT * FROM Orders").execute()
table_result2.print()

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}


## Syntax

Flink parses SQL using [Apache Calcite](https://calcite.apache.org/docs/reference.html), which supports standard ANSI SQL.

The following BNF-grammar describes the superset of supported SQL features in batch and streaming queries. The [Operations](#operations) section shows examples for the supported features and indicates which features are only supported for batch or streaming queries.

{{< expand Grammar >}}
```sql
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
  [ TABLE ] tablePath [ dynamicTableOptions ] [systemTimePeriod] [[AS] correlationName]
  | LATERAL TABLE '(' functionName '(' expression [, expression ]* ')' ')'
  | UNNEST '(' expression ')'

tablePath:
  [ [ catalogName . ] schemaName . ] tableName

systemTimePeriod:
  FOR SYSTEM_TIME AS OF dateTimeExpression

dynamicTableOptions:
  /*+ OPTIONS(key=val [, key=val]*) */

key:
  stringLiteral

val:
  stringLiteral

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

```
{{< /expand >}}

Flink SQL uses a lexical policy for identifier (table, attribute, function names) similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. <code>"SELECT a AS `my field` FROM t"</code>).

String literals must be enclosed in single quotes (e.g., `SELECT 'Hello World'`). Duplicate a single quote for escaping (e.g., `SELECT 'It''s me.'`). Unicode characters are supported in string literals. If explicit unicode code points are required, use the following syntax:

- Use the backslash (`\`) as escaping character (default): `SELECT U&'\263A'`
- Use a custom escaping character: `SELECT U&'#263A' UESCAPE '#'`

{{< top >}}

## Operations

### Scan, Projection, and Filter

#### Scan / Select / As

{{< label Batch >}} {{< label Streaming >}}

```sql
SELECT * FROM Orders

SELECT a, c AS d FROM Orders
```

#### Where

{{< label Batch >}} {{< label Streaming >}}

```sql
SELECT * FROM Orders WHERE b = 'red'

SELECT * FROM Orders WHERE a % 2 = 0
```

#### User-defined Scalar Functions (Scalar UDF)

{{< label Batch >}} {{< label Streaming >}}

```sql 
SELECT PRETTY_PRINT(user) FROM Orders;
```

### Aggregations

#### Group By Aggregation

{{< label Batch >}} {{< label Streaming >}} {{< label "Result Updating" >}}

```sql
SELECT a, SUM(b) as d
FROM Orders
GROUP BY a
```

#### Group By Window Aggregation

Use a [group window](#group-windows) to compute a single result row per group.

```sql
SELECT user, SUM(amount)
FROM Orders
GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user
```

#### Over Window Aggregation

{{< label Streaming >}}

All aggregates must be defined over the same window, i.e., same partitioning, sorting, and range. Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported. Ranges with FOLLOWING are not supported yet. ORDER BY must be specified on a single time attribute

```sql
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
```

#### Distinct 

```sql
SELECT DISTINCT users FROM Orders
```

{{< query_state_warning >}}

#### Grouping Sets, Rollup, Cube

{{< label Batch >}} {{< label Streaming >}} {{< label "Result Updating" >}}

```sql 
SELECT SUM(amount)
FROM Orders
GROUP BY GROUPING SETS ((user), (product))
```

#### Having 

{{< label Batch >}} {{< label Streaming >}} 

```sql
SELECT SUM(amount)
FROM Orders
GROUP BY users
HAVING SUM(amount) > 50
```

####  User-defined Aggregate Functions (UDAGG)

{{< label Batch >}} {{< label Streaming >}} 

```sql
SELECT MyAggregate(amount)
FROM Orders
GROUP BY users
```

### Joins

#### Inner Equi-Join

{{< label Batch >}} {{< label Streaming >}}

Currently, only equi-joins are supported, i.e., joins that have at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.

Note: The order of joins is not optimized. Tables are joined in the order in which they are specified in the FROM clause. Make sure to specify tables in an order that does not yield a cross join (Cartesian product) which are not supported and would cause a query to fail.

```sql
SELECT *
FROM Orders INNER JOIN Product ON Orders.productId = Product.id
```

{{< query_state_warning >}}

#### Outer Equi-Join

{{< label Batch >}} {{< label Streaming >}} {{< label "Result Updating" >}}

Currently, only equi-joins are supported, i.e., joins that have at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.

Note: The order of joins is not optimized. Tables are joined in the order in which they are specified in the FROM clause. Make sure to specify tables in an order that does not yield a cross join (Cartesian product) which are not supported and would cause a query to fail.

```sql
SELECT *
FROM Orders LEFT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders RIGHT JOIN Product ON Orders.productId = Product.id

SELECT *
FROM Orders FULL OUTER JOIN Product ON Orders.productId = Product.id
```

{{< query_state_warning >}}

#### Inner/Outer Interval Join

{{< label Batch >}} {{< label Streaming >}}

Interval joins are a subset of regular joins that can be processed in a streaming fashion. Both inner and outer joins are supported.

A interval join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Such a condition can be defined by two appropriate range predicates (<, <=, >=, >), a BETWEEN predicate, or a single equality predicate that compares time attributes of the same type (i.e., processing time or event time) of both input tables.

```sql
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
```

The example above will join all orders with their corresponding shipments if the order was shipped four hours after the order was received. 

#### Expanding Arrays

{{< label Batch >}} {{< label Streaming >}}

```sql
-- Array elements are basic type.
SELECT users, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
-- Array elements are ROW type. (eg. tags ARRAY<ROW<tag_id INT, tag_name STRING>>)
SELECT users, tag_id, tag_name
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag_id, tag_name)
```

Unnesting `WITH ORDINALITY` is not supported yet.

#### Join with Table Function 

{{< label Batch >}} {{< label Streaming >}}

Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function.

User-defined table functions (UDTFs) must be registered before use.

**Inner Join**

A row of the left (outer) table is dropped, if its table function call returns an empty result.

```sql
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags)) AS t(tag)
-- from 1.11, we can also do it like below:
SELECT users, tag
FROM Orders, LATERAL TABLE(unnest_udtf(tags))
```

**Left Outer Join**

If a table function call returns an empty result, the corresponding outer row is preserved and the result padded with null values.

```sql
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) AS t(tag) ON TRUE
-- from 1.11, we can also do it like below:
SELECT users, tag
FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) ON TRUE
```

Currently, only literal TRUE is supported as predicate for a left outer join against a lateral table.

#### Join with Temporal Table

{{< label Batch >}} {{< label Streaming >}}

Temporal Tables are tables that track changes over time. A Temporal Table provides access to the versions of a temporal table at a specific point in time.

Processing-time temporal join and event-time temporal join are supported, inner join and left join are supported.

The event-time temporal join is not suppored in Batch

The following example assumes that LatestRates is a Temporal Table which is materialized with the latest rate.

```sql
SELECT
  o.amount, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
```

### Set Operations

#### Union 

{{< label Batch >}}

```sql 
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION
    (SELECT user FROM Orders WHERE b = 0)
)
```

#### Union All

{{< label Batch >}} {{< label Streaming >}}

```sql
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  UNION ALL
    (SELECT user FROM Orders WHERE b = 0)
)
```

####  Intersect / Except

{{< label Batch >}}

```sql
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  INTERSECT
    (SELECT user FROM Orders WHERE b = 0)
)
```

```sql
SELECT *
FROM (
    (SELECT user FROM Orders WHERE a % 2 = 0)
  EXCEPT
    (SELECT user FROM Orders WHERE b = 0)
)
```

#### In 

{{< label Batch >}} {{< label Streaming >}}

Returns true if an expression exists in a given table sub-query. The sub-query table must 
consist of one column. This column must have the same data type as the expression.

```sql
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
```

{{< query_state_warning >}}

#### Exists 

{{< label Batch >}} {{< label Streaming >}}

```sql
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
```

Returns true if the sub-query returns at least one row. Only supported if the operation can be rewritten in a join and group operation.

{{< query_state_warning >}}

### OrderBy & Limit

#### Order By 

{{< label Batch >}} {{< label Streaming >}}

```sql
SELECT *
FROM Orders
ORDER BY orderTime
```

The result of streaming queries must be primarily sorted on an ascending time attribute. Additional sorting attributes are supported.

#### Limit 

{{< label Batch >}}

```sql
SELECT * 
FROM Orders
ORDER BY orderTime
LIMIT 3
```

The LIMIT clause requires an ORDER BY clause.

### Top-N

Top-N queries ask for the N smallest or largest values ordered by columns. Both smallest and largest values sets are considered Top-N queries. Top-N queries are useful in cases where the need is to display only the N bottom-most or the N top-
most records from batch/streaming table on a condition. This result set can be used for further analysis.

Flink uses the combination of a OVER window clause and a filter condition to express a Top-N query. With the power of OVER window `PARTITION BY` clause, Flink also supports per group Top-N. For example, the top five products per category that have the maximum sales in realtime. Top-N queries are supported for SQL on batch and streaming tables.

The following shows the syntax of the TOP-N statement:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one, according to the ordering of rows within the partition. Currently, we only support `ROW_NUMBER` as the over window function. In the future, we will support `RANK()` and `DENSE_RANK()`.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns. Each partition will have a Top-N result.
- `ORDER BY col1 [asc|desc][, col2 [asc|desc]...]`: Specifies the ordering columns. The ordering directions can be different on different columns.
- `WHERE rownum <= N`: The `rownum <= N` is required for Flink to recognize this query is a Top-N query. The N represents the N smallest or largest records will be retained.
- `[AND conditions]`: It is free to add other conditions in the where clause, but the other conditions can only be combined with `rownum <= N` using `AND` conjunction.

{{< hint info >}}
The TopN query is <span class="label label-info">Result Updating</span>. Flink SQL will sort the input data stream according to the order key, so if the top N records have been changed, the changed ones will be sent as retraction/update records to downstream.
It is recommended to use a storage which supports updating as the sink of Top-N query. In addition, if the top N records need to be stored in external storage, the result table should have the same unique key with the Top-N query.
{{< /hint >}}

The unique keys of Top-N query is the combination of partition columns and rownum column. Top-N query can also derive the unique key of upstream. Take following job as an example, say `product_id` is the unique key of the `ShopSales`, then the unique keys of the Top-N query are [`category`, `rownum`] and [`product_id`].

The following examples show how to specify SQL queries with Top-N on streaming tables. This is an example to get "the top five products per category that have the maximum sales in realtime" we mentioned above.

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

SELECT * 
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```

#### No Ranking Output Optimization

As described above, the `rownum` field will be written into the result table as one field of the unique key, which may lead to a lot of records being written to the result table. For example, when the record (say `product-1001`) of ranking 9 is updated and its rank is upgraded to 1, all the records from ranking 1 ~ 9 will be output to the result table as update messages. If the result table receives too many data, it will become the bottleneck of the SQL job.

The optimization way is omitting rownum field in the outer SELECT clause of the Top-N query. This is reasonable because the number of the top N records is usually not large, thus the consumers can sort the records themselves quickly. Without rownum field, in the example above, only the changed record (`product-1001`) needs to be sent to downstream, which can reduce much IO to the result table.

The following example shows how to optimize the above Top-N example in this way:

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

-- omit row_num field from the output
SELECT product_id, category, product_name, sales
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```

<span class="label label-danger">Attention in Streaming Mode</span> In order to output the above query to an external storage and have a correct result, the external storage must have the same unique key with the Top-N query. In the above example query, if the `product_id` is the unique key of the query, then the external table should also has `product_id` as the unique key.

{{< top >}}

### Deduplication

Deduplication is removing rows that duplicate over a set of columns, keeping only the first one or the last one. In some cases, the upstream ETL jobs are not end-to-end exactly-once, this may result in there are duplicate records in the sink in case of failover. However, the duplicate records will affect the correctness of downstream analytical jobs (e.g. `SUM`, `COUNT`). So a deduplication is needed before further analysis.

Flink uses `ROW_NUMBER()` to remove duplicates just like the way of Top-N query. In theory, deduplication is a special case of Top-N which the N is one and order by the processing time or event time.

The following shows the syntax of the Deduplication statement:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns, i.e. the deduplicate key.
- `ORDER BY time_attr [asc|desc]`: Specifies the ordering column, it must be a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}). Currently Flink supports [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) and [event time atttribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time). Ordering by ASC means keeping the first row, ordering by DESC means keeping the last row.
- `WHERE rownum = 1`: The `rownum = 1` is required for Flink to recognize this query is deduplication.

The following examples show how to specify SQL queries with Deduplication on streaming tables.

```sql
CREATE TABLE Orders (
  order_time  STRING,
  user        STRING,
  product     STRING,
  num         BIGINT,
  proctime AS PROCTIME()
) WITH (...);

-- remove duplicate rows on order_id and keep the first occurrence row,
-- because there shouldn't be two orders with the same order_id.
SELECT order_id, user, product, num
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num
  FROM Orders)
WHERE row_num = 1
```
{{< top >}}

Deduplication can keep the time attribute of input stream, this is very helpful when the downstream operation is window aggregation or join operation.
Both processing-time deduplication and event-time deduplication support working on mini-batch mode, this is more performance friendly, please see also [mini-batch configuration]({{< ref "docs/dev/table/config" >}}#table-exec-mini-batch-enabled) for how to enable mini-batch mode..

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

For SQL queries on streaming tables, the `time_attr` argument of the group window function must refer to a valid time attribute that specifies the processing time or event time of rows. See the [documentation of time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) to learn how to define time attributes.

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
        <p><b>Note:</b> The exclusive upper bound timestamp <i>cannot</i> be used as a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a> in subsequent time-based operations, such as <a href="#joins">interval joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_ROWTIME(time_attr, interval)</code><br/>
        <code>HOP_ROWTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_ROWTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns the timestamp of the <i>inclusive</i> upper bound of the corresponding tumbling, hopping, or session window.</p>
      <p>The resulting attribute is a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}">rowtime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">interval joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
    <tr>
      <td>
        <code>TUMBLE_PROCTIME(time_attr, interval)</code><br/>
        <code>HOP_PROCTIME(time_attr, interval, interval)</code><br/>
        <code>SESSION_PROCTIME(time_attr, interval)</code><br/>
      </td>
      <td><p>Returns a <a href="{{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time">proctime attribute</a> that can be used in subsequent time-based operations such as <a href="#joins">interval joins</a> and <a href="#aggregations">group window or over window aggregations</a>.</p></td>
    </tr>
  </tbody>
</table>

*Note:* Auxiliary functions must be called with exactly same arguments as the group window function in the `GROUP BY` clause.

The following examples show how to specify SQL queries with group windows on streaming tables.

```sql
CREATE TABLE Orders (
  user       BIGINT,
  product    STIRNG, 
  amount     INT,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '1' MINUTE
) WITH (...);

SELECT 
  user,
  TUMBLE_START(order_time, INTERVAL '1' DAY) AS wStart,
  SUM(amount) FROM Orders
GROUP BY 
  TUMBLE(order_time, INTERVAL '1' DAY), 
  user
```

{{< top >}}

### Pattern Recognition

#### MATCH_RECOGNIZE

{{< label Streaming >}}

Searches for a given pattern in a streaming table according to the `MATCH_RECOGNIZE` [ISO standard](https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip). 
This makes it possible to express complex event processing (CEP) logic in SQL queries.
For a more detailed description, see the dedicated page for [detecting patterns]({{< ref "docs/dev/table/concepts/match_recognize" >}}) in tables

```sql
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
```
{{< top >}}
