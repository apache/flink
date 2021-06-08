---
title: "INSERT 语句"
weight: 7
type: docs
aliases:
  - /zh/dev/table/sql/insert.html
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

# INSERT 语句



INSERT 语句用来向表中添加行。

## 执行 INSERT 语句

{{< tabs "execute" >}}
{{< tab "Java" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `createStatementSet` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `addInsertSql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。
{{< /tab >}}
{{< tab "Scala" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `createStatementSet` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `addInsertSql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。

{{< /tab >}}
{{< tab "Python" >}}

单条 INSERT 语句，可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行。`execute_sql()` 方法执行 INSERT 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。
多条 INSERT 语句，使用 `TableEnvironment` 中的 `create_statement_set` 创建一个 `StatementSet` 对象，然后使用 `StatementSet` 中的 `add_insert_sql()` 方法添加多条 INSERT 语句，最后通过 `StatementSet` 中的 `execute()` 方法来执行。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 INSERT 语句，或者通过 `StatementSet` 执行多条 INSERT 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 INSERT 语句

以下的例子展示了如何在 SQL CLI 中执行一条 INSERT 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "77ed5a01-effa-432c-b089-f922c3964c88" >}}
{{< tab "Java" >}}
```java
TableEnvironment tEnv = TableEnvironment.create(...);

// 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product VARCHAR, amount INT) WITH (...)");
tEnv.executeSql("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH (...)");

// 运行一条 INSERT 语句，将源表的数据输出到结果表中
TableResult tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
// 通过 TableResult 来获取作业状态
System.out.println(tableResult1.getJobClient().get().getJobStatus());

//----------------------------------------------------------------------------
// 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");

// 运行多条 INSERT 语句，将原表数据输出到多个结果表中
StatementSet stmtSet = tEnv.createStatementSet();
// `addInsertSql` 方法每次只接收单条 INSERT 语句
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'");
// 执行刚刚添加的所有 INSERT 语句
TableResult tableResult2 = stmtSet.execute();
// 通过 TableResult 来获取作业状态
System.out.println(tableResult1.getJobClient().get().getJobStatus());

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tEnv = TableEnvironment.create(...)

// 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
tEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
tEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

// 运行一个 INSERT 语句，将源表的数据输出到结果表中
val tableResult1 = tEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
// 通过 TableResult 来获取作业状态
println(tableResult1.getJobClient().get().getJobStatus())

//----------------------------------------------------------------------------
// 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)");

// 运行多个 INSERT 语句，将原表数据输出到多个结果表中
val stmtSet = tEnv.createStatementSet()
// `addInsertSql` 方法每次只接收单条 INSERT 语句
stmtSet.addInsertSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmtSet.addInsertSql(
  "INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
// 执行刚刚添加的所有 INSERT 语句
val tableResult2 = stmtSet.execute()
// 通过 TableResult 来获取作业状态
println(tableResult1.getJobClient().get().getJobStatus())
  
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 注册一个 "Orders" 源表，和 "RubberOrders" 结果表
table_env.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
table_env.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")

# 运行一条 INSERT 语句，将源表的数据输出到结果表中
table_result1 = table_env \
    .executeSql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
# 通过 TableResult 来获取作业状态
print(table_result1.get_job_client().get_job_status())

#----------------------------------------------------------------------------
# 注册一个 "GlassOrders" 结果表用于运行多 INSERT 语句
table_env.execute_sql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH (...)")

# 运行多条 INSERT 语句，将原表数据输出到多个结果表中
stmt_set = table_env.create_statement_set()
# `add_insert_sql` 方法每次只接收单条 INSERT 语句
stmt_set \
    .add_insert_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
stmt_set \
    .add_insert_sql("INSERT INTO GlassOrders SELECT product, amount FROM Orders WHERE product LIKE '%Glass%'")
# 执行刚刚添加的所有 INSERT 语句
table_result2 = stmt_set.execute()
# 通过 TableResult 来获取作业状态
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

## 将 SELECT 查询数据插入表中

通过 INSERT 语句，可以将查询的结果插入到表中，

### 语法

```sql

INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement

part_spec:
  (part_col_name1=val1 [, part_col_name2=val2, ...])

```

**OVERWRITE**

`INSERT OVERWRITE` 将会覆盖表中或分区中的任何已存在的数据。否则，新数据会追加到表中或分区中。

**PARTITION**

`PARTITION` 语句应该包含需要插入的静态分区列与值。

### 示例

```sql
-- 创建一个分区表
CREATE TABLE country_page_view (user STRING, cnt INT, date STRING, country STRING)
PARTITIONED BY (date, country)
WITH (...)

-- 追加行到该静态分区中 (date='2019-8-30', country='China')
INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- 追加行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
INSERT INTO country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;

-- 覆盖行到静态分区 (date='2019-8-30', country='China')
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
  SELECT user, cnt FROM page_view_source;

-- 覆盖行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
  SELECT user, cnt, country FROM page_view_source;
```

## 将值插入表中

通过 INSERT 语句，也可以直接将值插入到表中，

### 语法

```sql
INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES values_row [, values_row ...]

values_row:
    : (val1 [, val2, ...])
```

**OVERWRITE**

`INSERT OVERWRITE` 将会覆盖表中的任何已存在的数据。否则，新数据会追加到表中。

### 示例

```sql

CREATE TABLE students (name STRING, age INT, gpa DECIMAL(3, 2)) WITH (...);

INSERT INTO students
  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);

```

{{< top >}}
