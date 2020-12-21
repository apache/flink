---
title: "EXPLAIN 语句"
nav-parent_id: sql
nav-pos: 8
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

* This will be replaced by the TOC
{:toc}

EXPLAIN 语句用来解释一条 query 语句或者 INSERT 语句的逻辑计划和优化后的计划。


## 运行一条 EXPLAIN 语句

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

EXPLAIN 语句可以通过 `TableEnvironment` 的 `executeSql()` 执行。 若 EXPLAIN 操作执行成功，`executeSql()` 方法返回解释的结果，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 EXPLAIN 语句。

</div>

<div data-lang="python" markdown="1">

EXPLAIN 语句可以通过 `TableEnvironment` 的 `execute_sql()` 执行。 若 EXPLAIN 操作执行成功，`execute_sql()` 方法返回解释的结果，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 EXPLAIN 语句。

</div>

<div data-lang="SQL CLI" markdown="1">

可以在 [SQL CLI]({% link dev/table/sqlClient.zh.md %}) 中执行 EXPLAIN 语句。

以下的例子展示了如何在 SQL CLI 中执行一条 EXPLAIN 语句。

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256) WITH (...)");
tEnv.executeSql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256) WITH (...)");

// explain SELECT statement through TableEnvironment.explainSql()
String explanation = tEnv.explainSql(
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2");
System.out.println(explanation);

// explain SELECT statement through TableEnvironment.executeSql()
TableResult tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2");
tableResult.print();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256) WITH (...)")
tEnv.executeSql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256) WITH (...)")

// explain SELECT statement through TableEnvironment.explainSql()
val explanation = tEnv.explainSql(
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2")
println(explanation)

// explain SELECT statement through TableEnvironment.executeSql()
val tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT `count`, word FROM MyTable2")
tableResult.print()

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

t_env.execute_sql("CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256) WITH (...)")
t_env.execute_sql("CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256) WITH (...)")

# explain SELECT statement through TableEnvironment.explain_sql()
explanation1 = t_env.explain_sql(
    "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2")
print(explanation1)

# explain SELECT statement through TableEnvironment.execute_sql()
table_result = t_env.execute_sql(
    "EXPLAIN PLAN FOR "
    "SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT `count`, word FROM MyTable2")
table_result.print()

{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE MyTable1 (`count` bigint, word VARCHAR(256);
[INFO] Table has been created.

Flink SQL> CREATE TABLE MyTable2 (`count` bigint, word VARCHAR(256);
[INFO] Table has been created.

Flink SQL> EXPLAIN PLAN FOR SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' 
> UNION ALL 
> SELECT `count`, word FROM MyTable2;

{% endhighlight %}
</div>
</div>

执行 `EXPLAIN` 语句后的结果为：
<div class="codetabs" markdown="1">
<div data-lang="Blink Planner" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    LogicalTableScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
  LogicalTableScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Optimized Physical Plan ==
Union(all=[true], union all=[count, word])
  Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
  TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Optimized Execution Plan ==
Union(all=[true], union all=[count, word])
  Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
  TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])
{% endhighlight %}
</div>

<div data-lang="Legacy Planner" markdown="1">
{% highlight text %}
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalTableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
  FlinkLogicalTableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    TableSourceScan(table=[[default_catalog, default_database, MyTable1]], fields=[count, word])
  TableSourceScan(table=[[default_catalog, default_database, MyTable2]], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

Stage 2 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 3 : Operator
		content : from: (count, word)
		ship_strategy : REBALANCE

		Stage 4 : Operator
			content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
			ship_strategy : FORWARD

			Stage 5 : Operator
				content : from: (count, word)
				ship_strategy : REBALANCE
{% endhighlight %}
</div>
</div>

{% top %}

## 语法

{% highlight sql %}
EXPLAIN PLAN FOR <query_statement_or_insert_statement>
{% endhighlight %}

请参阅 [Queries]({% link dev/table/sql/queries.zh.md %}#supported-syntax) 页面获得 query 的语法。
请参阅 [INSERT]({% link dev/table/sql/insert.zh.md %}) 页面获得 INSERT 的语法。

{% top %}
