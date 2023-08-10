---
title: "TRUNCATE 语句"
weight: 8
type: docs
aliases:
- /zh/dev/table/sql/truncate.html
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

<a name="truncate-statements"></a>

# TRUNCATE 语句

{{< label Batch >}}

TRUNCATE 语句用于删除表中的全部数据，但不会删除表本身。

<span class="label label-danger">注意</span> 目前, `TRUNCATE` 语句仅支持批模式, 并且要求目标表实现了 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsTruncate.java" name="SupportsTruncate" >}} 接口。
如果在一个没有实现该接口的表上执行 `TRUNCATE` 语句，则会抛异常。

<a name="run-a-truncate-statement"></a>

## 执行 TRUNCATE 语句

{{< tabs "truncate" >}}
{{< tab "Java" >}}
可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 TRUNCATE 语句。如果 TRUNCATE 操作执行失败，`executeSql()` 方法会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 TRUNCATE 语句。
{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 TRUNCATE 语句。如果 TRUNCATE 操作执行失败，`executeSql()` 方法会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 TRUNCATE 语句。

{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 TRUNCATE 语句。如果 TRUNCATE 操作执行失败，`execute_sql()` 方法会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 TRUNCATE 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

TRUNCATE 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 TRUNCATE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);

// 注册 "Orders" 表
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");

// 插入数据
tEnv.executeSql("INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 2), ('Mr.White', 'Chicken', 3)").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           2 |
// |                       Mr.White |                        Chicken |           3 |
// +--------------------------------+--------------------------------+-------------+
// 3 rows in set
        
// 清理 "Orders" 表数据
tEnv.executeSql("TRUNCATE TABLE Orders").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// 返回空结果
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val settings = EnvironmentSettings.newInstance().inBatchMode().build()
val tEnv = StreamTableEnvironment.create(env, settings)

// 注册一个 "Orders" 表
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)")
// 插入原始数据
tEnv.executeSql("INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 2), ('Mr.White', 'Chicken', 3)").await()
tEnv.executeSql("SELECT * FROM Orders").print()
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           2 |
// |                       Mr.White |                        Chicken |           3 |
// +--------------------------------+--------------------------------+-------------+
// 3 rows in set
// 全表删除数据
tEnv.executeSql("TRUNCATE TABLE Orders").await()
tEnv.executeSql("SELECT * FROM Orders").print()
// Empty set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# 注册一个 "Orders" 表
table_env.execute_sql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)")
# 插入原始数据
table_env.execute_sql("INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 2), ('Mr.White', 'Chicken', 3)").wait()
table_env.execute_sql("SELECT * FROM Orders").print()
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           1 |
# |                        Jessica |                         Banana |           2 |
# |                       Mr.White |                        Chicken |           3 |
# +--------------------------------+--------------------------------+-------------+
# 3 rows in set
# 全表删除数据
table_env.execute_sql("TRUNCATE TABLE Orders").wait()
table_env.execute_sql("SELECT * FROM Orders").print()
# Empty set
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> SET 'execution.runtime-mode' = 'batch';
[INFO] Session property has been set.

Flink SQL> CREATE TABLE Orders (`user` STRING, product STRING, amount INT) with (...);
[INFO] Execute statement succeed.

Flink SQL> INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1), ('Mr.White', 'Chicken', 3);
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: bd2c46a7b2769d5c559abd73ecde82e9

Flink SQL> SELECT * FROM Orders;
    user                         product      amount
    Lili                           Apple           1
 Jessica                          Banana           2
Mr.White                         Chicken           3

Flink SQL> TRUNCATE TABLE Orders;
[INFO] Execute statement succeed.

Flink SQL> SELECT * FROM Orders;
// Empty set
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

<a name="syntax"></a>

## 语法

```sql
TRUNCATE TABLE [catalog_name.][db_name.]table_name
```
