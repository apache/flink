---
title: "EXPLAIN 语句"
weight: 9
type: docs
aliases:
  - /zh/dev/table/sql/explain.html
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
String explanation =
                tEnv.explainSql(
                        "SELECT `count`, COUNT(word) FROM ("
                                + "MyTable1 WHERE word LIKE 'F%' "
                                + "UNION ALL "
                                + "SELECT `count`, word FROM MyTable2) tmp"
                                + "GROUP BY `count`");
System.out.println(explanation);

// 调用 TableEnvironment.executeSql() 来解释 SELECT 语句
TableResult tableResult =
                tEnv.executeSql(
                        "EXPLAIN PLAN FOR "
                                + "SELECT `count`, COUNT(word) FROM ("
                                + "MyTable1 WHERE word LIKE 'F%' "
                                + "UNION ALL "
                                + "SELECT `count`, word FROM MyTable2) tmp GROUP BY `count`");
tableResult.print();

TableResult tableResult2 =
                tEnv.executeSql(
                        "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, PLAN_ADVICE, JSON_EXECUTION_PLAN "
                                + "SELECT `count`, COUNT(word) FROM ("
                                + "MyTable1 WHERE word LIKE 'F%' "
                                + "UNION ALL "
                                + "SELECT `count`, word FROM MyTable2) tmp GROUP BY `count`");
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
"""
  |SELECT `count`, COUNT(word)
  |FROM (
  |  SELECT `count`, word FROM MyTable1
  |  WHERE word LIKE 'F%'
  |  UNION ALL 
  |  SELECT `count`, word FROM MyTable2 ) tmp
  |GROUP BY `count`
  |""".stripMargin)
println(explanation)

// 调用 TableEnvironment.executeSql() 来解释 SELECT 语句
val tableResult = tEnv.executeSql(
"""
  |EXPLAIN PLAN FOR
  |SELECT `count`, COUNT(word)
  |FROM (
  |  SELECT `count`, word FROM MyTable1
  |  WHERE word LIKE 'F%'
  |  UNION ALL
  |  SELECT `count`, word FROM MyTable2 ) tmp
  |GROUP BY `count`
  |""".stripMargin)
tableResult.print()

val tableResult2 = tEnv.executeSql(
"""
  |EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, PLAN_ADVICE, JSON_EXECUTION_PLAN
  |SELECT `count`, COUNT(word)
  |FROM (
  |  SELECT `count`, word FROM MyTable1
  |  WHERE word LIKE 'F%'
  |  UNION ALL
  |  SELECT `count`, word FROM MyTable2 ) tmp
  |GROUP BY `count`
  |""".stripMargin)
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
    "SELECT `count`, COUNT(word) FROM (" 
    "MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2) tmp GROUP BY `count`")
print(explanation1)

# 调用 TableEnvironment.execute_sql() 来解释 SELECT 语句
table_result = t_env.execute_sql(
    "EXPLAIN PLAN FOR "
    "SELECT `count`, COUNT(word) FROM (" 
    "MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2) tmp GROUP BY `count`")
table_result.print()

table_result2 = t_env.execute_sql(
    "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, PLAN_ADVICE, JSON_EXECUTION_PLAN "
    "SELECT `count`, COUNT(word) FROM (" 
    "MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2) tmp GROUP BY `count`")
table_result2.print()

```
{{< /tab >}}
{{< tab "SQL CLI" >}}

```sql
Flink SQL> CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen');
[INFO] Table has been created.

Flink SQL> CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256)) WITH ('connector' = 'datagen');
[INFO] Table has been created.

Flink SQL> EXPLAIN PLAN FOR SELECT `count`, COUNT(word) FROM
> ( SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' 
>   UNION ALL 
>   SELECT `count`, word FROM MyTable2 ) tmp GROUP BY `count`;
                                  
Flink SQL> EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, PLAN_ADVICE, JSON_EXECUTION_PLAN SELECT `count`, COUNT(word) FROM
> ( SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' 
>   UNION ALL 
>   SELECT `count`, word FROM MyTable2 ) tmp GROUP BY `count`;
```
{{< /tab >}}
{{< /tabs >}}

`EXPLAIN` 的结果如下:

{{< tabs "explain result" >}}

{{< tab "EXPLAIN PLAN" >}}

```text
== Abstract Syntax Tree ==
LogicalAggregate(group=[{0}], EXPR$1=[COUNT($1)])
+- LogicalUnion(all=[true])
   :- LogicalProject(count=[$0], word=[$1])
   :  +- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :     +- LogicalTableScan(table=[[default_catalog, default_database, MyTable1]])
   +- LogicalProject(count=[$0], word=[$1])
      +- LogicalTableScan(table=[[default_catalog, default_database, MyTable2]])

== Optimized Physical Plan ==
GroupAggregate(groupBy=[count], select=[count, COUNT(word) AS EXPR$1])
+- Exchange(distribution=[hash[count]])
   +- Union(all=[true], union=[count, word])
      :- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
      :  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
      +- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Optimized Execution Plan ==
GroupAggregate(groupBy=[count], select=[count, COUNT(word) AS EXPR$1])
+- Exchange(distribution=[hash[count]])
   +- Union(all=[true], union=[count, word])
      :- Calc(select=[count, word], where=[LIKE(word, 'F%')])
      :  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
      +- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])
```

{{< /tab >}}

{{< tab "EXPLAIN PLAN WITH DETAILS" >}}

```text
== Abstract Syntax Tree ==
LogicalAggregate(group=[{0}], EXPR$1=[COUNT($1)])
+- LogicalUnion(all=[true])
   :- LogicalProject(count=[$0], word=[$1])
   :  +- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :     +- LogicalTableScan(table=[[default_catalog, default_database, MyTable1]])
   +- LogicalProject(count=[$0], word=[$1])
      +- LogicalTableScan(table=[[default_catalog, default_database, MyTable2]])

== Optimized Physical Plan With Advice ==
GroupAggregate(advice=[1], groupBy=[count], select=[count, COUNT(word) AS EXPR$1], changelogMode=[I,UA]): rowcount = 1.05E8, cumulative cost = {5.2E8 rows, 1.805E10 cpu, 4.0E9 io, 2.1E9 network, 0.0 memory}
+- Exchange(distribution=[hash[count]], changelogMode=[I]): rowcount = 1.05E8, cumulative cost = {4.15E8 rows, 1.7945E10 cpu, 4.0E9 io, 2.1E9 network, 0.0 memory}
   +- Union(all=[true], union=[count, word], changelogMode=[I]): rowcount = 1.05E8, cumulative cost = {3.1E8 rows, 3.05E8 cpu, 4.0E9 io, 0.0 network, 0.0 memory}
      :- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')], changelogMode=[I]): rowcount = 5000000.0, cumulative cost = {1.05E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}
      :  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}
      +- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word], changelogMode=[I]): rowcount = 1.0E8, cumulative cost = {1.0E8 rows, 1.0E8 cpu, 2.0E9 io, 0.0 network, 0.0 memory}

advice[1]: [ADVICE] You might want to enable local-global two-phase optimization by configuring ('table.exec.mini-batch.enabled' to 'true', 'table.exec.mini-batch.allow-latency' to a positive long value, 'table.exec.mini-batch.size' to a positive long value).

== Optimized Execution Plan ==
GroupAggregate(groupBy=[count], select=[count, COUNT(word) AS EXPR$1])
+- Exchange(distribution=[hash[count]])
   +- Union(all=[true], union=[count, word])
      :- Calc(select=[count, word], where=[LIKE(word, 'F%')])
      :  +- TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
      +- TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Physical Execution Plan ==
{
  "nodes" : [ {
    "id" : 17,
    "type" : "Source: MyTable1[15]",
    "pact" : "Data Source",
    "contents" : "[15]:TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])",
    "parallelism" : 1
  }, {
    "id" : 18,
    "type" : "Calc[16]",
    "pact" : "Operator",
    "contents" : "[16]:Calc(select=[count, word], where=[LIKE(word, 'F%')])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : 17,
      "ship_strategy" : "FORWARD",
      "side" : "second"
    } ]
  }, {
    "id" : 19,
    "type" : "Source: MyTable2[17]",
    "pact" : "Data Source",
    "contents" : "[17]:TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])",
    "parallelism" : 1
  }, {
    "id" : 22,
    "type" : "GroupAggregate[20]",
    "pact" : "Operator",
    "contents" : "[20]:GroupAggregate(groupBy=[count], select=[count, COUNT(word) AS EXPR$1])",
    "parallelism" : 1,
    "predecessors" : [ {
      "id" : 18,
      "ship_strategy" : "HASH",
      "side" : "second"
    }, {
      "id" : 19,
      "ship_strategy" : "HASH",
      "side" : "second"
    } ]
  } ]
}
```

{{< /tab >}}

{{< /tabs >}}

{{< top >}}

<a name="explaindetails"></a>

## ExplainDetails
使用指定的 `ExplainDetail` 类型来打印语句的计划。

**ESTIMATED_COST**

指定 `ESTIMATED_COST` 将使得优化器（optimizer）将估算出的成本信息附加在每个物理节点上输出。

```text
== Optimized Physical Plan ==
TableSourceScan(..., cumulative cost ={1.0E8 rows, 1.0E8 cpu, 2.4E9 io, 0.0 network, 0.0 memory})
```

**CHANGELOG_MODE**

指定 `CHANGELOG_MODE` 将使得优化器（optimizer）将 changelog mode 附加在每个物理节点上输出。
关于 changelog mode 更多信息请参阅 [动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#table-to-stream-conversion)。

```text
== Optimized Physical Plan ==
GroupAggregate(..., changelogMode=[I,UA,D])
```

**PLAN_ADVICE**
{{< hint info >}}
从 Flink 1.17 版本开始支持 `PLAN_ADVICE`。
{{< /hint >}}

指定 `PLAN_ADVICE` 将使得优化器（optimizer）分析优化后的物理执行计划并提供潜在的数据风险预警或性能调优建议。
此时输出标题将会从 “Optimized Physical Plan” 变为 “Optimized Physical Plan with Advice” 作为提示。

针对物理计划的建议按照 **类型** 和 **范围** 来区分。

| 建议类型    | 说明             |
|:--------|:---------------|
| WARNING | 给出潜在的数据正确性风险   |
| ADVICE  | 给出可能的 SQL 调优建议 |


| 建议范围        | 说明           |
|:------------|:-------------|
| QUERY_LEVEL | 针对整个 SQL 的建议 |
| NODE_LEVEL  | 针对单个物理节点的建议  |

`PLAN_ADVICE` 提供针对如下问题的建议
- 分组聚合（Group Aggregation）时产生的数据倾斜（更多信息请参阅 [分组聚合]({{< ref "docs/dev/table/sql/queries/group-agg" >}}#group-aggregation) 和 [性能调优]({{< ref "docs/dev/table/tuning" >}}#local-global-aggregation)）
- 非确定性更新（*abbr.* NDU，更多信息请参阅 [流上的确定性]({{< ref "docs/dev/table/concepts/determinism" >}}#3-determinism-in-streaming-processing)）


若检测到分组聚合可以启用两阶段优化但未开启时，优化器（optimizer）将会把建议 id 附在 `GroupAggregate` 节点内作为索引，在最后附上建议内容。
{{< tabs "Data Skewness" >}}

{{< tab "SQL1" >}}
```sql
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '200';
SET 'table.optimizer.agg-phase-strategy' = 'ONE_PHASE';

CREATE TABLE MyTable (
  a BIGINT,
  b INT NOT NULL,
  c VARCHAR,
  d BIGINT
) WITH (
  'connector' = 'values',
  'bounded' = 'false');

EXPLAIN PLAN_ADVICE
SELECT
  AVG(a) AS avg_a,
  COUNT(*) AS cnt,
  COUNT(b) AS cnt_b,
  MIN(b) AS min_b,
  MAX(c) FILTER (WHERE a > 1) AS max_c
FROM MyTable;
```
{{< /tab>}}

{{< tab "NODE_LEVEL ADVICE" >}}
```text
== Optimized Physical Plan With Advice ==
Calc(select=[avg_a, cnt, cnt AS cnt_b, min_b, max_c])
+- GroupAggregate(advice=[1], select=[AVG(a) AS avg_a, COUNT(*) AS cnt, MIN(b) AS min_b, MAX(c) FILTER $f3 AS max_c])
   +- Exchange(distribution=[single])
      +- Calc(select=[a, b, c, IS TRUE(>(a, 1)) AS $f3])
         +- MiniBatchAssigner(interval=[10000ms], mode=[ProcTime])
            +- TableSourceScan(table=[[default_catalog, default_database, MyTable, project=[a, b, c], metadata=[]]], fields=[a, b, c])

advice[1]: [ADVICE] You might want to enable local-global two-phase optimization by configuring ('table.optimizer.agg-phase-strategy' to 'AUTO').
```
{{< /tab>}}

{{< /tabs>}}


若检测到存在 NDU 问题风险时，优化器（optimizer）将会把建议内容附在最后。
{{< tabs "Non-deterministic Updates" >}}

{{< tab "SQL2" >}}
```sql
CREATE TABLE MyTable (
  a INT,
  b BIGINT,
  c STRING,
  d INT,
  `day` AS DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd'),
  PRIMARY KEY (a, c) NOT ENFORCED
) WITH (
  'connector' = 'values',
  'changelog-mode' = 'I,UA,UB,D'  
);

CREATE TABLE MySink (
 a INT,
 b BIGINT,
 c STRING,
 PRIMARY KEY (a) NOT ENFORCED
) WITH (
 'connector' = 'values',
 'sink-insert-only' = 'false'
);

EXPLAIN PLAN_ADVICE
INSERT INTO MySink
SELECT a, b, `day`
FROM MyTable
WHERE b > 100;
```

{{< /tab>}}

{{< tab "QUERY_LEVEL WARNING" >}}
```text
== Optimized Physical Plan With Advice ==
Sink(table=[default_catalog.default_database.MySink], fields=[a, b, day], upsertMaterialize=[true])
+- Calc(select=[a, b, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day], where=[>(b, 100)])
   +- TableSourceScan(table=[[default_catalog, default_database, MyTable, filter=[], project=[a, b], metadata=[]]], fields=[a, b])

advice[1]: [WARNING] The column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism requirement for correctly processing update message('UB'/'UA'/'D' in changelogMode, not 'I' only), this usually happens when input node has no upsertKey(upsertKeys=[{}]) or current node outputs non-deterministic update messages. Please consider removing these non-deterministic columns or making them deterministic by using deterministic functions.

related rel plan:
Calc(select=[a, b, DATE_FORMAT(CURRENT_TIMESTAMP(), _UTF-16LE'yyMMdd') AS day], where=[>(b, 100)], changelogMode=[I,UB,UA,D])
+- TableSourceScan(table=[[default_catalog, default_database, MyTable, filter=[], project=[a, b], metadata=[]]], fields=[a, b], changelogMode=[I,UB,UA,D])
```
{{< /tab>}}

{{< /tabs>}}


若未检测到问题，优化器（optimizer）将会在计划最后附上 “No available advice” 作为提示。
{{< tabs "Default" >}}

{{< tab "SQL3" >}}
```sql
CREATE TABLE MyTable (
  a INT,
  b BIGINT,
  c STRING,
  d INT
) WITH (
  'connector' = 'values',
  'changelog-mode' = 'I'  
);

EXPLAIN PLAN_ADVICE
SELECT * FROM MyTable WHERE b > 100;
```

{{< /tab>}}

{{< tab "NO_AVAILABLE ADVICE" >}}
```text
== Optimized Physical Plan With Advice ==
Calc(select=[a, b, c, d], where=[>(b, 100)])
+- TableSourceScan(table=[[default_catalog, default_database, MyTable]], fields=[a, b, c, d])

No available advice...

```
{{< /tab>}}

{{< /tabs>}}

**JSON_EXECUTION_PLAN**

生成 json 格式的程序执行计划。
{{< top >}}

<a name="syntax"></a>

## 语法

```sql
EXPLAIN [([ExplainDetail[, ExplainDetail]*]) | PLAN FOR] <query_statement_or_insert_statement_or_statement_set>

statement_set:
STATEMENT SET
BEGIN
insert_statement;
...
insert_statement;
END;
```

关于 query 的语法，请查阅 [Queries]({{< ref "docs/dev/table/sql/queries" >}}#supported-syntax) 页面。
关于 INSERT 的语法，请查阅 [INSERT]({{< ref "docs/dev/table/sql/insert" >}}) 页面。
