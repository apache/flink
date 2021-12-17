---
title: "EXPLAIN 语句"
weight: 9
type: docs
aliases:
  - /dev/table/sql/explain.html
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

<a name="explain-statements"></a>

# EXPLAIN 语句

EXPLAIN 语句用于解释 query 或 INSERT 语句的执行逻辑，也用于优化 query 语句的查询计划。

<a name="run-an-explain-statement"></a>

## 执行 EXPLAIN 语句

{{< tabs "explain" >}}
{{< tab "Java" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 EXPLAIN 语句。如果 EXPLAIN 操作执行成功，`executeSql()` 方法会返回解释结果，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 EXPLAIN 语句。

{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 EXPLAIN 语句。如果 EXPLAIN 操作执行成功，`executeSql()` 方法会返回解释结果，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 EXPLAIN 语句。
{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 的 `execute_sql()` 方法执行 EXPLAIN 语句。如果 EXPLAIN 操作执行成功，`execute_sql()` 方法会返回解释结果，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 EXPLAIN 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

EXPLAIN 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 EXPLAIN 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "c83a84e4-4b3a-420b-9a27-94dc640dfcce" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 注册名为 “Orders” 的表
tEnv.executeSql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')");
tEnv.executeSql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')");

// 调用 TableEnvironment.explainSql() 来解释 SELECT 语句
String explanation = tEnv.explainSql(
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2");
System.out.println(explanation);

// 调用 TableEnvironment.executeSql() 来解释 SELECT 语句
TableResult tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2");
tableResult.print();

TableResult tableResult2 = tEnv.executeSql(
  "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2");
tableResult2.print();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// 注册名为 “Orders” 的表
tEnv.executeSql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')")
tEnv.executeSql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')")

// 调用 TableEnvironment.explainSql() 来解释 SELECT 语句
val explanation = tEnv.explainSql(
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2")
println(explanation)

// 调用 TableEnvironment.executeSql() 来解释 SELECT 语句
val tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2")
tableResult.print()

val tableResult2 = tEnv.executeSql(
  "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2")
tableResult2.print()

```
{{< /tab >}}
{{< tab "Python" >}}
```python
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

t_env.execute_sql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')")
t_env.execute_sql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen')")

# 调用 TableEnvironment.explain_sql() 来解释 SELECT 语句
explanation1 = t_env.explain_sql(
    "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2")
print(explanation1)

# 调用 TableEnvironment.execute_sql() 来解释 SELECT 语句
table_result = t_env.execute_sql(
    "EXPLAIN PLAN FOR "
    "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2")
table_result.print()

table_result2 = t_env.execute_sql(
    "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN "
    "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2")
table_result2.print()

```
{{< /tab >}}
{{< tab "SQL CLI" >}}

```sql
Flink SQL> CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen');
[INFO] Table has been created.

Flink SQL> CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen');
[INFO] Table has been created.

Flink SQL> EXPLAIN PLAN FOR SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' 
> UNION ALL 
> SELECT `count`, word FROM MyTable2;
                                  
Flink SQL> EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN SELECT `count`, word FROM MyTable1 
> WHERE word LIKE 'F%' 
> UNION ALL 
> SELECT `count`, word FROM MyTable2;
```
{{< /tab >}}
{{< /tabs >}}

`EXPLAIN` 的结果如下:

{{< tabs "explain result" >}}

{{< tab "EXPLAIN PLAN" >}}

```text
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
:- LogicalProject(count=[$0], word=[$1])
:  +- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
:     +- LogicalTableScan(table=[[default_catalog, default_database, MyTable1]])
+- LogicalProject(count=[$0], word=[$1])
   +- LogicalTableScan(table=[[default_catalog, default_database, MyTable2]])

== Optimized Physical Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
+- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Optimized Execution Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
+- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])
```

{{< /tab >}}

{{< tab "EXPLAIN PLAN WITH DETAILS" >}}

```text
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
:- LogicalProject(count=[$0], word=[$1])
:  +- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
:     +- LogicalTableScan(table=[[default_catalog, default_database, MyTable1]])
+- LogicalProject(count=[$0], word=[$1])
   +- LogicalTableScan(table=[[default_catalog, default_database, MyTable2]])

== Optimized Physical Plan ==
Union(all=[true], union=[count, word], changelogMode=[I]): rowcount = 1.05E8, cumulative cost = {3.1E8 rows, 3.05E8 cpu, 4.0E9 io, 0.0 network, 0.0 memory}
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')], changelogMode=[I]): rowcount = 5000000.0, cumulative cost = {1.05E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}
:  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}
+- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}

== Optimized Execution Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
+- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Physical Execution Plan ==
{
  "nodes" : [ {
    "id" : 37,
    "type" : "Source: TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])",
    "pact" : "Data Source",
    "contents" : "Source: TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])",
    "parallelism" : 1
  }, {
    "id" : 38,
    "type" : "Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])",
    "pact" : "Operator",
    "contents" : "Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : 37,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : 39,
    "type" : "Source: TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])",
    "pact" : "Data Source",
    "contents" : "Source: TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])",
    "parallelism" : 1
  } ]
```

{{< /tab >}}

{{< /tabs >}}

{{< top >}}

<a name="explaindetails"></a>

## ExplainDetails

```text
使用指定的 explainDetail 类型来打印语句的计划。

ESTIMATED_COST：生成优化器（optimizer）估算的物理节点相关的成本信息, 
例如：TableSourceScan(..., cumulative cost ={1.0E8 rows, 1.0E8 cpu, 2.4E9 io, 0.0 network, 0.0 memory})

CHANGELOG_MODE：为每个物理 RelNode 生成 changelog mode。
例如：GroupAggregate(..., changelogMode=[I,UA,D])

JSON_EXECUTION_PLAN：生成 json 格式的程序执行计划。
```

<a name="syntax"></a>

## 语法

```sql
EXPLAIN [([ExplainDetail[, ExplainDetail]*]) | PLAN FOR] <query_statement_or_insert_statement>
```


关于 query 的语法，请查阅 [Queries]({{< ref "docs/dev/table/sql/queries" >}}#supported-syntax) 页面。
关于 INSERT 的语法，请查阅 [INSERT]({{< ref "docs/dev/table/sql/insert" >}}) 页面。
