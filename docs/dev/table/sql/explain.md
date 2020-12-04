---
title: "EXPLAIN Statements"
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

EXPLAIN statements are used to explain the logical and optimized query plans of a query or an INSERT statement.


## Run an EXPLAIN statement

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

EXPLAIN statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns explain result for a successful EXPLAIN operation, otherwise will throw an exception.

The following examples show how to run an EXPLAIN statement in `TableEnvironment`.

</div>

<div data-lang="python" markdown="1">

EXPLAIN statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns explain result for a successful EXPLAIN operation, otherwise will throw an exception.

The following examples show how to run an EXPLAIN statement in `TableEnvironment`.

</div>

<div data-lang="SQL CLI" markdown="1">

EXPLAIN statements can be executed in [SQL CLI]({% link dev/table/sqlClient.md %}).

The following examples show how to run an EXPLAIN statement in SQL CLI.

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE MyTable1 (count bigint, work VARCHAR(256) WITH (...)");
tEnv.executeSql("CREATE TABLE MyTable2 (count bigint, work VARCHAR(256) WITH (...)");

// explain SELECT statement through TableEnvironment.explainSql()
String explanation = tEnv.explainSql(
  "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT count, word FROM MyTable2");
System.out.println(explanation);

// explain SELECT statement through TableEnvironment.executeSql()
TableResult tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT count, word FROM MyTable2");
tableResult.print();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE MyTable1 (count bigint, work VARCHAR(256) WITH (...)")
tEnv.executeSql("CREATE TABLE MyTable2 (count bigint, work VARCHAR(256) WITH (...)")

// explain SELECT statement through TableEnvironment.explainSql()
val explanation = tEnv.explainSql(
  "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT count, word FROM MyTable2")
println(explanation)

// explain SELECT statement through TableEnvironment.executeSql()
val tableResult = tEnv.executeSql(
  "EXPLAIN PLAN FOR " + 
  "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' " +
  "UNION ALL " + 
  "SELECT count, word FROM MyTable2")
tableResult.print()

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

t_env.execute_sql("CREATE TABLE MyTable1 (count bigint, work VARCHAR(256) WITH (...)")
t_env.execute_sql("CREATE TABLE MyTable2 (count bigint, work VARCHAR(256) WITH (...)")

# explain SELECT statement through TableEnvironment.explain_sql()
explanation1 = t_env.explain_sql(
    "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT count, word FROM MyTable2")
print(explanation1)

# explain SELECT statement through TableEnvironment.execute_sql()
table_result = t_env.execute_sql(
    "EXPLAIN PLAN FOR "
    "SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' "
    "UNION ALL "
    "SELECT count, word FROM MyTable2")
table_result.print()

{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE MyTable1 (count bigint, work VARCHAR(256);
[INFO] Table has been created.

Flink SQL> CREATE TABLE MyTable2 (count bigint, work VARCHAR(256);
[INFO] Table has been created.

Flink SQL> EXPLAIN PLAN FOR SELECT count, word FROM MyTable1 WHERE word LIKE 'F%' 
> UNION ALL 
> SELECT count, word FROM MyTable2;

{% endhighlight %}
</div>
</div>

The `EXPLAIN` result is:
<div>
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

{% top %}

## Syntax

{% highlight sql %}
EXPLAIN PLAN FOR <query_statement_or_insert_statement>
{% endhighlight %}

For query syntax, please refer to [Queries]({% link dev/table/sql/queries.md %}#supported-syntax) page.
For INSERT, please refer to [INSERT]({% link dev/table/sql/insert.md %}) page.
