---
title: "Compiled Plans"
weight: 5
type: docs
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

# Compiled Plans

The planner translates a Table API or SQL program into a pipeline of operators every time you
submit it. A compiled plan is the result of this translation, written to a JSON file. It contains
the execution nodes of the pipeline with their IDs and versions, the edges between them, the tables
and functions they use, and the values of the options that the nodes depend on. When you run a
plan, Flink doesn't plan the query again, so the job gets the same operators with the same IDs
every time, including on a newer Flink version within the limits in
[Flink Versions](#flink-versions).

Use a compiled plan when you upgrade a job to a newer Flink version and need it to keep its state,
see [Upgrading the Flink Version]({{< ref "docs/ops/upgrading" >}}#upgrading-the-flink-version), and to set the state
TTL of each stateful operator, see
[Configure Operator-level State TTL]({{< ref "docs/concepts/sql-table-concepts/overview" >}}#配置算子粒度的状态-ttl).

## 生成 Compiled Plan {#compile-a-plan}

使用 `COMPILE PLAN` 语句生成一个 JSON 文件，它表示了序列化后的执行计划。
{{< hint info >}}
`COMPILE PLAN` 不支持查询语句 `SELECT... FROM...` 。 
{{< /hint >}}

- 执行 `COMPILE PLAN` 语句

{{< tabs "compile-plan" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
tableEnv.executeSql(
    "CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...)");
tableEnv.executeSql(
    "CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...)");
tableEnv.executeSql(
    "CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...)");

// CompilePlan#writeToFile only supports a local file path, if you need to write to remote filesystem,
// please use tableEnv.executeSql("COMPILE PLAN 'hdfs://path/to/plan.json' FOR ...")
CompiledPlan compiledPlan = 
    tableEnv.compilePlanSql(
        "INSERT INTO enriched_orders \n" 
       + "SELECT a.order_id, a.order_line_id, b.order_status, ... \n" 
       + "FROM orders a JOIN line_orders b ON a.order_line_id = b.order_line_id");

compiledPlan.writeToFile("/path/to/plan.json");

```

{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode())
tableEnv.executeSql(
    "CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...)")
tableEnv.executeSql(
    "CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...)")
tableEnv.executeSql(
    "CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...)")

val compiledPlan = 
    tableEnv.compilePlanSql(
       """
        |INSERT INTO enriched_orders
        |SELECT a.order_id, a.order_line_id, b.order_status, ...
        |FROM orders a JOIN line_orders b ON a.order_line_id = b.order_line_id
        |""".stripMargin)
// CompilePlan#writeToFile only supports a local file path, if you need to write to remote filesystem,
// please use tableEnv.executeSql("COMPILE PLAN 'hdfs://path/to/plan.json' FOR ...")
compiledPlan.writeToFile("/path/to/plan.json")
```
{{< /tab >}}
{{< tab "SQL CLI" >}}

```sql
Flink SQL> CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> COMPILE PLAN 'file:///path/to/plan.json' FOR INSERT INTO enriched_orders
> SELECT a.order_id, a.order_line_id, b.order_status, ...
> FROM orders a JOIN line_orders b ON a.order_line_id = b.order_line_id;
[INFO] Execute statement succeeded.
```

{{< /tab >}}
{{< /tabs >}}


- SQL 语法

    ```sql
    COMPILE PLAN <plan_file_path> [IF NOT EXISTS] FOR <insert_statement>|<statement_set>;
    
    statement_set:
        STATEMENT SET
        BEGIN
        insert_statement;
        ...
        insert_statement;
        END;
    
    insert_statement:
        <insert_from_select>|<insert_from_values>
    ```
    该语句会在指定位置 `/path/to/plan.json` 生成一个 JSON 文件。

{{< hint info >}}
`COMPILE PLAN` 语句支持写入 `hdfs://` 或 `s3://` 等 Flink 支持的[文件系统]({{< ref "docs/deployment/filesystems/overview" >}})。
请确保已为目标写入路径设置了写入权限。
{{< /hint >}}

If the file already exists, `COMPILE PLAN` fails:

```text
org.apache.flink.table.api.TableException: Cannot overwrite the plan file 'file:///path/to/plan.json'. Either manually remove the file or, if you're debugging your job, set the option 'table.plan.force-recompile' to true.
```

With `IF NOT EXISTS`, `COMPILE PLAN` keeps the existing file and doesn't compile the statement.
`CompiledPlan#writeToFile(path, true)` also keeps an existing file. With
`table.plan.force-recompile` = `true`, `COMPILE PLAN` overwrites the file. Only use this while you
develop a job: a new compilation can assign other operator IDs than the ones in a savepoint.

The SQL Client or SQL Gateway that runs the statement reads and writes the plan file, so it needs
access to the path. With a remote SQL Gateway, a local path is a path on the machine of the
Gateway.

## 执行 Compiled Plan {#execute-a-plan}

`EXECUTE PLAN` 语句将会反序列化上述 JSON 文件，进一步生成 JobGraph 并提交作业。

- 执行 `EXECUTE PLAN` 语句
{{< tabs "execute-plan" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
tableEnv.executeSql(
    "CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...)");
tableEnv.executeSql(
    "CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...)");
tableEnv.executeSql(
    "CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...)");

// PlanReference#fromFile only supports a local file path, if you need to read from remote filesystem,
// please use tableEnv.executeSql("EXECUTE PLAN 'hdfs://path/to/plan.json'").await();
tableEnv.loadPlan(PlanReference.fromFile("/path/to/plan.json")).execute().await();

```

{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode())
tableEnv.executeSql(
    "CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...)")
tableEnv.executeSql(
    "CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...)")
tableEnv.executeSql(
    "CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...)")

// PlanReference#fromFile only supports a local file path, if you need to read from remote filesystem,
// please use tableEnv.executeSql("EXECUTE PLAN 'hdfs://path/to/plan.json'").await()
tableEnv.loadPlan(PlanReference.fromFile("/path/to/plan.json")).execute().await()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}

```sql
Flink SQL> CREATE TABLE orders (order_id BIGINT, order_line_id BIGINT, buyer_id BIGINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> CREATE TABLE line_orders (order_line_id BIGINT, order_status TINYINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> CREATE TABLE enriched_orders (order_id BIGINT, order_line_id BIGINT, order_status TINYINT, ...);
[INFO] Execute statement succeeded.

Flink SQL> EXECUTE PLAN 'file:///path/to/plan.json';
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 79fbe3fa497e4689165dd81b1d225ea8
```

{{< /tab >}}
{{< /tabs >}}

- SQL 语法

    ```sql
    EXECUTE PLAN <plan_file_path>;
    ```
    该语句反序列化指定的 JSON 文件，并提交作业。

To run a plan that your Table API program ships as a resource in its JAR, read the resource
and use `PlanReference.fromJsonString`, as in the Java example in
[Upgrading the Flink Version]({{< ref "docs/ops/upgrading" >}}#upgrading-the-flink-version).
`PlanReference.fromResource` fails for a file inside a JAR.

### Tables and Functions {#tables-and-functions}

With the default `table.plan.compile.catalog-objects` = `ALL`, a plan stores each catalog table
with its schema and options. `EXECUTE PLAN` then also works in a new session in which the tables
haven't been created. If a table is in the catalog, a restore takes the options that its connector
allows to change from there, such as the `path` of the filesystem connector. See
`table.plan.restore.catalog-objects`.

Temporary tables and functions are stored only by their name. Create them again before you run
the plan, otherwise `EXECUTE PLAN` fails:

```text
org.apache.flink.table.api.TableException: Cannot resolve table 'default_catalog.default_database.orders' and the persisted plan does not include all required catalog table metadata. Make sure a registered catalog contains the table when restoring or the table is available as a temporary table. Otherwise regenerate the plan with 'table.plan.compile.catalog-objects' != 'IDENTIFIER' and make sure the table was not compiled as a temporary table.
```

The plan assumes that its tables and functions don't change. If a table in the catalog has another
schema than in the plan, `EXECUTE PLAN` fails with `The schema of table '...' from the persisted plan
does not match the schema loaded from the catalog`. A change of the signature of a function isn't
supported either.

### Options {#options}

For each node, the plan stores the values of the options that the node uses in its
`configuration` entry. For example, the sink of the plan above:

```json
"configuration" : {
  "table.exec.sink.keyed-shuffle" : "AUTO",
  "table.exec.sink.not-null-enforcer" : "ERROR",
  "table.exec.sink.rowtime-inserter" : "ENABLED",
  "table.exec.sink.type-length-enforcer" : "IGNORE",
  "table.exec.sink.upsert-materialize" : "AUTO"
}
```

The job uses these values, not the ones of the session. With
`SET 'table.exec.sink.not-null-enforcer' = 'DROP'` before `EXECUTE PLAN`, the sink still fails on a
NULL value in a `NOT NULL` column. To use another value, compile a new plan, see
[Changing the Query]({{< ref "docs/ops/upgrading" >}}#changing-the-query) for what that means for the state of a job.

## Compile and Execute the Plan in One Statement {#compile-and-execute-the-plan-in-one-statement}

`COMPILE AND EXECUTE PLAN` compiles the statement, writes the plan to the file, and runs it. If the
file already exists, it runs the plan from the file and ignores the statement. Running the same
statement again, including on a newer Flink version, runs the plan that was compiled first. If you
change the statement, the change is ignored without a warning. Compile a changed statement to a new
file.

The statement is still validated, so unlike `EXECUTE PLAN`, it needs its tables in the session.

- SQL Syntax

    ```sql
    COMPILE AND EXECUTE PLAN <plan_file_path> FOR <insert_statement>|<statement_set>;
    ```

## What Can't Be Compiled {#what-cant-be-compiled}

A plan can't contain:

* a table created from a `DataStream`, for example with `StreamTableEnvironment#fromDataStream`.
* legacy table sources and sinks (`TableSource`, `TableSink`).
* table aggregate functions.
* process table functions, in Flink versions before 2.1.

`COMPILE PLAN` fails for these. In the Table API, `compilePlan()` succeeds, and the error comes when
you write the plan, for example with `writeToFile` or `asJsonString`. For a table from a
`DataStream`, the error is:

```text
Cannot serialize the table as it's an external inline table. This might be caused by a usage of StreamTableEnvironment#fromDataStream or TableResult#collect, which are not supported in compiled plans.
```

For a table aggregate function, the error is the following, although it's not a bug: table
aggregate functions aren't supported in a plan.

```text
Can not serialize ExecNode with id: 6. Missing type, this is a bug, please file a ticket.
```

Writing a plan doesn't check that it can be loaded again. Run the plan once with `EXECUTE PLAN`,
for example on a test cluster, before you rely on it for an upgrade.

## Flink Versions {#flink-versions}

The plan stores the Flink version that compiled it, and the type of each node includes its
version, such as `stream-exec-join_1`. A newer Flink version runs the plan as long as it still
supports these node versions, otherwise `EXECUTE PLAN` fails to load the plan. Flink guarantees
that a plan runs after an upgrade by one minor version, for example from 2.2 to 2.3. Over more
versions, this isn't guaranteed, although the tests of Flink restore plans that were compiled with
Flink 1.19 and later.

Flink doesn't migrate a plan to a newer version: `EXECUTE PLAN` and `COMPILE AND EXECUTE PLAN`
don't rewrite the plan file. After two upgrades, a job still runs the plan of the first version.

A plan from a newer Flink version doesn't load on an older one:

```text
org.apache.flink.table.api.ValidationException: Unknown Flink version '2.5'
```

The JSON format of the plan is internal to Flink. Don't change a plan by hand, except for the state
TTL as described in [Configure Operator-level State TTL]({{< ref "docs/concepts/sql-table-concepts/overview" >}}#配置算子粒度的状态-ttl).
