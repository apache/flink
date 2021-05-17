---
title: "INSERT Statement"
weight: 7
type: docs
aliases:
  - /dev/table/sql/insert.html
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

# INSERT Statement



INSERT statements are used to add rows to a table.

## Run an INSERT statement

{{< tabs "insert" >}}
{{< tab "Java" >}}
Single INSERT statement can be executed through the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method for INSERT statement will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job. 
Multiple INSERT statements can be executed through the `addInsertSql()` method of the `StatementSet` which can be created by the `TableEnvironment.createStatementSet()` method. The `addInsertSql()` method is a lazy execution, they will be executed only when `StatementSet.execute()` is invoked.

The following examples show how to run a single INSERT statement in `TableEnvironment`, run multiple INSERT statements in `StatementSet`.

{{< /tab >}}
{{< tab "Scala" >}}
Single INSERT statement can be executed through the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method for INSERT statement will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job. 
Multiple INSERT statements can be executed through the `addInsertSql()` method of the `StatementSet` which can be created by the `TableEnvironment.createStatementSet()` method. The `addInsertSql()` method is a lazy execution, they will be executed only when `StatementSet.execute()` is invoked.

The following examples show how to run a single INSERT statement in `TableEnvironment`, run multiple INSERT statements in `StatementSet`.
{{< /tab >}}
{{< tab "Python" >}}

Single INSERT statement can be executed through the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method for INSERT statement will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job. 
Multiple INSERT statements can be executed through the `add_insert_sql()` method of the `StatementSet` which can be created by the `TableEnvironment.create_statement_set()` method. The `add_insert_sql()` method is a lazy execution, they will be executed only when `StatementSet.execute()` is invoked.

The following examples show how to run a single INSERT statement in `TableEnvironment`, run multiple INSERT statements in `StatementSet`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

Single INSERT statement can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a single INSERT statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "15bc87ce-93fd-4fdd-8c51-3301a432c048" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tEnv = TableEnvironment.create(settings);

// register a source table named "Orders" and a sink table named "RubberOrders"
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
tEnv.executeSql("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");

// run a single INSERT query on the registered source table and emit the result to registered sink table
TableResult tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
// get job status through TableResult
System.out.println(tableResult1.getJobClient().get().getJobStatus());

//----------------------------------------------------------------------------
// register another sink table named "GlassOrders" for multiple INSERT queries
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");

// run multiple INSERT queries on the registered source table and emit the result to registered sink tables
StatementSet stmtSet = tEnv.createStatementSet();
// only single INSERT query can be accepted by `addInsertSql` method
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'");
// execute all statements together
TableResult tableResult2 = stmtSet.execute();
// get job status through TableResult
System.out.println(tableResult2.getJobClient().get().getJobStatus());

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val settings = EnvironmentSettings.newInstance()...
val tEnv = TableEnvironment.create(settings)

// register a source table named "Orders" and a sink table named "RubberOrders"
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
tEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

// run a single INSERT query on the registered source table and emit the result to registered sink table
val tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
// get job status through TableResult
println(tableResult1.getJobClient().get().getJobStatus())

//----------------------------------------------------------------------------
// register another sink table named "GlassOrders" for multiple INSERT queries
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)")

// run multiple INSERT queries on the registered source table and emit the result to registered sink tables
val stmtSet = tEnv.createStatementSet()
// only single INSERT query can be accepted by `addInsertSql` method
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
// execute all statements together
val tableResult2 = stmtSet.execute()
// get job status through TableResult
println(tableResult2.getJobClient().get().getJobStatus())

```
{{< /tab >}}
{{< tab "Python" >}}
```python
settings = EnvironmentSettings.new_instance()...
table_env = TableEnvironment.create(settings)

# register a source table named "Orders" and a sink table named "RubberOrders"
table_env.execute_sql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
table_env.execute_sql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

# run a single INSERT query on the registered source table and emit the result to registered sink table
table_result1 = table_env \
    .execute_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
# get job status through TableResult
print(table_result1get_job_client().get_job_status())

#----------------------------------------------------------------------------
# register another sink table named "GlassOrders" for multiple INSERT queries
table_env.execute_sql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)")

# run multiple INSERT queries on the registered source table and emit the result to registered sink tables
stmt_set = table_env.create_statement_set()
# only single INSERT query can be accepted by `add_insert_sql` method
stmt_set \
    .add_insert_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmt_set \
    .add_insert_sql("INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
# execute all statements together
table_result2 = stmt_set.execute()
# get job status through TableResult
print(table_result2.get_job_client().get_job_status())

```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...);

Flink SQL> SHOW TABLES;
Orders
RubberOrders

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
[INFO] Table update statement has been successfully submitted to the cluster:
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Insert from select queries

Query Results can be inserted into tables by using the insert clause.

### Syntax

```sql

INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] [column_list] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

column_list:
  (col_name1 [, column_name2, ...])
```

**OVERWRITE**

`INSERT OVERWRITE` will overwrite any existing data in the table or partition. Otherwise, new data is appended.

**PARTITION**

`PARTITION` clause should contain static partition columns of this inserting.

**COLUMN LIST**

Given a table T(a INT, b INT, c INT), Flink supports INSERT INTO T(c, b) SELECT x, y FROM S. The expectation is
that 'x' is written to column 'c' and 'y' is written to column 'b' and 'a' is set to NULL (assuming column 'a' is nullable). 

### Examples

```sql
-- Creates a partitioned table
CREATE TABLE country_page_view (user STRING, cnt INT, date STRING, country STRING)
PARTITIONED BY (date, country)
WITH (...)

-- Appends rows into the static partition (date='2019-8-30', country='China')
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Appends rows into partition (date, country), where date is static partition with value '2019-8-30',
-- country is dynamic partition whose value is dynamic determined by each row.
INSERT INTO country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;

-- Overwrites rows into static partition (date='2019-8-30', country='China')
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- Overwrites rows into partition (date, country), where date is static partition with value '2019-8-30',
-- country is dynamic partition whose value is dynamic determined by each row.
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;

-- Appends rows into the static partition (date='2019-8-30', country='China')
-- the column cnt is set to NULL
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China') (user)
  SELECT user FROM page_view_source;
```


## Insert values into tables

The INSERT...VALUES statement can be used to insert data into tables directly from SQL.

### Syntax

```sql
INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES values_row [, values_row ...]

values_row:
    : (val1 [, val2, ...])
```

**OVERWRITE**

`INSERT OVERWRITE` will overwrite any existing data in the table. Otherwise, new data is appended.

### Examples

```sql

CREATE TABLE students (name STRING, age INT, gpa DECIMAL(3, 2)) WITH (...);

INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);

```

{{< top >}}
