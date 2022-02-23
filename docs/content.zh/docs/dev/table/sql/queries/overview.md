---
title: "概览"
weight: 1
type: docs
aliases:
  - /zh/dev/table/sql/queries.html
  - /zh/dev/table/queries/
  - /zh/dev/table/sql.html
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

# 查询


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
final Schema schema = Schema.newBuilder()
    .column("product", DataTypes.STRING())
    .column("amount", DataTypes.INT())
    .build();

final TableDescriptor sinkDescriptor = TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", ",")
        .build())
    .build();

tableEnv.createTemporaryTable("RubberOrders", sinkDescriptor);

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
val schema = Schema.newBuilder()
  .column("product", DataTypes.STRING())
  .column("amount", DataTypes.INT())
  .build()

val sinkDescriptor = TableDescriptor.forConnector("filesystem")
  .schema(schema)
  .format(FormatDescriptor.forFormat("csv")
    .option("field-delimiter", ",")
    .build())
  .build()

tableEnv.createTemporaryTable("RubberOrders", sinkDescriptor)

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
schema = Schema.new_builder()
    .column("product", DataTypes.STRING())
    .column("amount", DataTypes.INT())
    .build()

sink_descriptor = TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .format(FormatDescriptor.for_format("csv")
        .option("field-delimiter", ",")
        .build())
    .build()

t_env.create_temporary_table("RubberOrders", sink_descriptor)

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
  | WITH withItem [ , withItem ]* query
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

withItem:
    name
    [ '(' column [, column ]* ')' ]
    AS '(' query ')'

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
  | [ LATERAL ] '(' query ')'
  | UNNEST '(' expression ')'

tablePath:
    [ [ catalogName . ] databaseName . ] tableName

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
  | '*?'
  | '+'
  | '+?'
  | '?'
  | '??'
  | '{' { [ minRepeat ], [ maxRepeat ] } '}' ['?']
  | '{' repeat '}'
```
{{< /expand >}}

Flink SQL uses a lexical policy for identifier (table, attribute, function names) similar to Java:

- The case of identifiers is preserved whether or not they are quoted.
- After which, identifiers are matched case-sensitively.
- Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters (e.g. <code>"SELECT a AS `my field` FROM t"</code>).

String literals must be enclosed in single quotes (e.g., `SELECT 'Hello World'`). Duplicate a single quote for escaping (e.g., `SELECT 'It''s me'`).

```text
Flink SQL> SELECT 'Hello World', 'It''s me';
+-------------+---------+
|      EXPR$0 |  EXPR$1 |
+-------------+---------+
| Hello World | It's me |
+-------------+---------+
1 row in set
```

Unicode characters are supported in string literals. If explicit unicode code points are required, use the following syntax:

- Use the backslash (`\`) as escaping character (default): `SELECT U&'\263A'`
- Use a custom escaping character: `SELECT U&'#263A' UESCAPE '#'`

{{< top >}}

## Operations

- [WITH clause]({{< ref "docs/dev/table/sql/queries/with" >}})
- [SELECT & WHERE]({{< ref "docs/dev/table/sql/queries/select" >}})
- [SELECT DISTINCT]({{< ref "docs/dev/table/sql/queries/select-distinct" >}})
- [Windowing TVF]({{< ref "docs/dev/table/sql/queries/window-tvf" >}})
- [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}})
- [Group Aggregation]({{< ref "docs/dev/table/sql/queries/group-agg" >}})
- [Over Aggregation]({{< ref "docs/dev/table/sql/queries/over-agg" >}})
- [Joins]({{< ref "docs/dev/table/sql/queries/joins" >}})
- [Set Operations]({{< ref "docs/dev/table/sql/queries/set-ops" >}})
- [ORDER BY clause]({{< ref "docs/dev/table/sql/queries/orderby" >}})
- [LIMIT clause]({{< ref "docs/dev/table/sql/queries/limit" >}})
- [Top-N]({{< ref "docs/dev/table/sql/queries/topn" >}})
- [Window Top-N]({{< ref "docs/dev/table/sql/queries/window-topn" >}})
- [Deduplication]({{< ref "docs/dev/table/sql/queries/deduplication" >}})
- [Pattern Recognition]({{< ref "docs/dev/table/sql/queries/match_recognize" >}})

{{< top >}}
