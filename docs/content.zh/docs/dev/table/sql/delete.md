---
title: "DELETE 语句"
weight: 18
type: docs
aliases:
  - /zh/dev/table/sql/delete.html
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

# DELETE 语句

`DELETE` 语句可以用于根据条件来删除表中的数据

<span class="label label-danger">注意</span> 目前, `DELETE` 语句仅支持批模式, 并且要求目标表实现了 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsRowLevelDelete.java" name="SupportsRowLevelDelete" >}} 接口。
如果在一个没有实现该接口的表上执行 `DELETE`，则会抛异常。目前 Flink 内置的连接器还没有实现该接口。

## 执行删除语句

{{< tabs "delete statement" >}}

{{< tab "Java" >}}

DELETE 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 DELETE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 DELETE 语句。

{{< /tab >}}
{{< tab "Scala" >}}

DELETE 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 DELETE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 DELETE 语句。

{{< /tab >}}
{{< tab "Python" >}}

DELETE 语句，可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行。`execute_sql()` 方法执行 DELETE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 DELETE 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 DELETE 语句

以下的例子展示了如何在 SQL CLI 中执行一条 DELETE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "9bcbe6f1-9a62-41ff-c7be-568ec5908a29" >}}

{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
// 注册一个 "Orders" 表
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
// 插入原始数据
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
// 根据where条件删除
tEnv.executeSql("DELETE FROM Orders WHERE `user` = 'Lili'").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                        Jessica |                         Banana |           2 |
// |                       Mr.White |                        Chicken |           3 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 全表删除
tEnv.executeSql("DELETE FROM Orders").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// Empty set
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val settings = EnvironmentSettings.newInstance().inBatchMode().build()
val tEnv = StreamTableEnvironment.create(env, settings)

// 注册一个 "Orders" 表
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
// 插入原始数据
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
// 根据where条件删除
tEnv.executeSql("DELETE FROM Orders WHERE `user` = 'Lili'").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                        Jessica |                         Banana |           2 |
// |                       Mr.White |                        Chicken |           3 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 全表删除
tEnv.executeSql("DELETE FROM Orders").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// Empty set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# 注册一个 "Orders" 表
table_env.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
# 插入原始数据
table_env.executeSql("INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 2), ('Mr.White', 'Chicken', 3)").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           1 |
# |                        Jessica |                         Banana |           2 |
# |                       Mr.White |                        Chicken |           3 |
# +--------------------------------+--------------------------------+-------------+
# 3 rows in set
# 根据where条件删除
table_env.executeSql("DELETE FROM Orders WHERE `user` = 'Lili'").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                        Jessica |                         Banana |           2 |
# |                       Mr.White |                        Chicken |           3 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
# 全表删除
table_env.executeSql("DELETE FROM Orders").wait();
table_env.executeSql("SELECT * FROM Orders").print();
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

Flink SQL> DELETE FROM Orders WHERE `user` = 'Lili';

    user                         product      amount
 Jessica                          Banana           2
Mr.White                         Chicken           3

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## 语法

```sql
DELETE FROM [catalog_name.][db_name.]table_name [ WHERE condition ]
```

