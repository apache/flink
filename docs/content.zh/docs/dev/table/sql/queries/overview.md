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


`TableEnvironment` 的 `sqlQuery()` 方法可以执行 `SELECT` 和 `VALUES` 语句。
这个方法把 `SELECT` 语句(或 `VALUES` 语句)的结果作为一个 `Table` 返回。
`Table`可以用在[后续 SQL 和 Table API 查询]({{< ref "docs/dev/table/common" >}}#mixing-table-api-and-sql)中，可以[转换为 DataStream]({{< ref "docs/dev/table/common" >}}#integration-with-datastream)， 或者 [写入到TableSink]({{< ref "docs/dev/table/common" >}}#emit-a-table)。
SQL 和 Table API 查询可以无缝混合，并进行整体优化并转换为单个程序。

为了在SQL查询中访问表，它必须[注册在 TableEnvironment]({{< ref "docs/dev/table/common" >}}#register-tables-in-the-catalog)。
表使用下列方式注册：[TableSource]({{< ref "docs/dev/table/common" >}}#register-a-tablesource)， [Table]({{< ref "docs/dev/table/common" >}}#register-a-table)，[CREATE TABLE 语句](#create-table)，[DataStream]({{< ref "docs/dev/table/common" >}}#register-a-datastream)。
也可以通过[在 TableEnvironment 中注册 Catalog]({{< ref "docs/dev/table/catalogs" >}}) 来指定数据源的位置。

为了方便起见，`Table.toString()` 自动在 `TableEnvironment` 中注册一个名称唯一的表，并返回表名。
所以`Table`对象可以直接内嵌入 SQL 中查询使用，如下示例所示。

**注意:** 查询如果包含不支持的 SQL 特性，会抛出`TableException`异常。
下面的章节中列出了批处理和流处理上支持的 SQL 特性。

## 指定查询

下面的示例演示如何在一个注册的表和内联(inlined)的表上指定SQL查询。

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

## 执行查询


通过 `TableEnvironment.executeSql()` 方法可以执行 `SELECT` 或 `VALUES` 语句，并把结果收集到本地。它将`SELECT`语句（或`VALUES`语句）的结果作为 `TableResult` 返回。和 `SELECT` 语句相似，`Table.execute()` 方法可以执行`Table`对象，并把结果收集到本地客户端。
`TableResult.collect()` 方法返回一个可关闭的行迭代器（row iterator）。除非所有结果数据都被收集完成了，否则`SELECT`作业不会停止，所以应该主动使用 `CloseableIterator#close()` 方法关闭作业，以防止资源泄露。`TableResult.print()` 可以打印 `SELECT` 的结果到客户端的控制台中。 `TableResult` 上的结果数据只能被访问一次。因此 `collect()` 和 `print()` 只能二选一。

`TableResult.collect()` 和 `TableResult.print()`在不同的 checkpointing 设置下有一些差异。(流式作业开启 checkpointing，参见 [checkpointing 设置]({{< ref "docs/deployment/config" >}}#checkpointing))。

*   对于没有开启 checkpoint 的批作业或流作业，`TableResult.collect()` 和 `TableResult.print()` 既不保证精确一次（exactly-once）也不保证至少一次（at-least-once）。查询结果一旦产生，客户端可以立即访问，但是，作业失败或重启将抛出异常。
*   对于 checkpoint 设置为精确一次（exactly-once）的流式作业， `TableResult.collect()` 和 `TableResult.print()` 保证端到端的数据只传递一次。相应的checkpoint完成后，客户端才能访问结果。
*   对于 checkpoint 设置为至少一次（at-least-once）的流式作业， `TableResult.collect()` 和 `TableResult.print()` 保证端到端的数据至少传递一次，查询结果一旦产生，客户端可以立即访问，但是可能会有同一条数据出现多次的情况。

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
tableEnv.getConfig
  .set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
tableEnv.getConfig
  .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))

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
table_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().set("execution.checkpointing.interval", "10s")

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


## 语法

Flink使用支持标准 ANSI SQL 的 [Apache Calcite](https://calcite.apache.org/docs/reference.html) 解析 SQL。

下面的 BNF-grammar 描述了批处理和流处理查询中所支持 SQL 特性的超集。[操作](#操作)展示了支持的功能以及示例，并指示了哪些功能仅支持批处理或流处理查询。

{{< details Grammar >}}
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
{{< /details >}}

Flink SQL使用的标识符词法规则(table，attribute，function names)和Java相似。

*   大写或小写的标识符都是保留的，就算没有被引用。
*   标识符的匹配区分大小写。
*   和Java不同，反引号(`\`)允许标识符包含非字母数字（no-alphanumeric）字符(例如：<code>"SELECT a AS \`my field\` FROM t"</code>)。

字符串必须被单引号括起来（例如： `SELECT 'Hello World'`）。两个单引号用于转义（例如:`SELECT 'It''s me'`）。

```text
Flink SQL> SELECT 'Hello World', 'It''s me';
+-------------+---------+
|      EXPR$0 |  EXPR$1 |
+-------------+---------+
| Hello World | It's me |
+-------------+---------+
1 row in set
```

字符串支持Unicode字符。 下面是显式使用Unicode编码的语法：

- 使用反斜杠(`\`)作为转义字符 (默认)：`SELECT U&'\263A'`
- 使用自定义的转义字符：`SELECT U&'#263A' UESCAPE '#'`

{{< top >}}

## 操作

- [WITH 子句]({{< ref "docs/dev/table/sql/queries/with" >}})
- [SELECT & WHERE]({{< ref "docs/dev/table/sql/queries/select" >}})
- [SELECT DISTINCT]({{< ref "docs/dev/table/sql/queries/select-distinct" >}})
- [窗口函数]({{< ref "docs/dev/table/sql/queries/window-tvf" >}})
- [窗口聚合]({{< ref "docs/dev/table/sql/queries/window-agg" >}})
- [分组聚合]({{< ref "docs/dev/table/sql/queries/group-agg" >}})
- [Over聚合]({{< ref "docs/dev/table/sql/queries/over-agg" >}})
- [Joins]({{< ref "docs/dev/table/sql/queries/joins" >}})
- [集合操作]({{< ref "docs/dev/table/sql/queries/set-ops" >}})
- [ORDER BY 语句]({{< ref "docs/dev/table/sql/queries/orderby" >}})
- [LIMIT 语句]({{< ref "docs/dev/table/sql/queries/limit" >}})
- [Top-N]({{< ref "docs/dev/table/sql/queries/topn" >}})
- [窗口 Top-N]({{< ref "docs/dev/table/sql/queries/window-topn" >}})
- [去重]({{< ref "docs/dev/table/sql/queries/deduplication" >}})
- [模式识别]({{< ref "docs/dev/table/sql/queries/match_recognize" >}})
- [时间旅行]({{< ref "docs/dev/table/sql/queries/time-travel" >}})

{{< top >}}
