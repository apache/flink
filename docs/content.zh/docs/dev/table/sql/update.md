---
title: "UPDATE 语句"
weight: 17
type: docs
aliases:
- /zh/dev/table/sql/update.html
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

# UPDATE 语句

`UPDATE` 语句可以用于根据条件更新表的数据。

<span class="label label-danger">注意</span> 目前, `UPDATE` 语句仅支持批模式, 并且要求目标表实现了 {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsRowLevelUpdate.java" name="SupportsRowLevelUpdate" >}} 接口。
如果在一个没有实现该接口的表上执行 `UPDATE`，则会抛异常。目前 Flink 内置的连接器还没有实现该接口。

## 执行更新语句

{{< tabs "update statement" >}}
{{< tab "Java" >}}

UPDATE 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 UPDATE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 UPDATE 语句。

{{< /tab >}}
{{< tab "Scala" >}}

UPDATE 语句，可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 UPDATE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 UPDATE 语句。

{{< /tab >}}
{{< tab "Python" >}}

UPDATE 语句，可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行。`execute_sql()` 方法执行 UPDATE 语句时会立即提交一个 Flink 作业，并且返回一个 TableResult 对象，通过该对象可以获取 JobClient 方便的操作提交的作业。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 UPDATE 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 UPDATE 语句

以下的例子展示了如何在 SQL CLI 中执行一条 UPDATE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "dc09cb72-eebd-eb88-c29b-43bc3973f9f4" >}}

{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
// 注册一个 "Orders" 表
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
// 插入原始数据
tEnv.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           1 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 更新所有的amount字段
tEnv.executeSql("UPDATE Orders SET `amount` = `amount` * 2").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 根据where条件更新
tEnv.executeSql("UPDATE Orders SET `product` = 'Orange' WHERE `user` = 'Lili'").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                         Orange |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
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
tEnv.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           1 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 更新所有的amount字段
tEnv.executeSql("UPDATE Orders SET `amount` = `amount` * 2").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// 根据where条件更新
tEnv.executeSql("UPDATE Orders SET `product` = 'Orange' WHERE `user` = 'Lili'").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                         Orange |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# 注册一个 "Orders" 表
table_env.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
# 插入原始数据
table_env.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           1 |
# |                        Jessica |                         Banana |           1 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
# 更新所有的amount字段
table_env.executeSql("UPDATE Orders SET `amount` = `amount` * 2").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           2 |
# |                        Jessica |                         Banana |           2 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
# 根据where条件更新
table_env.executeSql("UPDATE Orders SET `product` = 'Orange' WHERE `user` = 'Lili'").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                         Orange |           2 |
# |                        Jessica |                         Banana |           2 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> SET 'execution.runtime-mode' = 'batch';
[INFO] Session property has been set.

Flink SQL> CREATE TABLE Orders (`user` STRING, product STRING, amount INT) with (...);
[INFO] Execute statement succeed.

Flink SQL> INSERT INTO Orders VALUES ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1);
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: bd2c46a7b2769d5c559abd73ecde82e9

Flink SQL> SELECT * FROM Orders;
   user                        product      amount
   Lili                          Apple           1
Jessica                         Banana           1

Flink SQL> UPDATE Orders SET amount = 2;

   user                        product      amount
   Lili                          Apple           2
Jessica                         Banana           2

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## 语法

```sql
UPDATE [catalog_name.][db_name.]table_name SET column_name1 = expression1 [, column_name2 = expression2, ...][ WHERE condition ]
```

