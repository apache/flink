---
title: "UPDATE Statements"
weight: 17
type: docs
aliases:
- /dev/table/sql/update.html
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

# UPDATE Statements

`UPDATE` statement is used to perform row-level updating on the target table according to the filter if provided.

<span class="label label-danger">Attention</span> Currently, `UPDATE` statement only supports in batch mode, and it requires the target table connector implements the {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsRowLevelUpdate.java" name="SupportsRowLevelUpdate" >}}
interface to support the row-level update. An exception will be thrown if trying to `UPDATE` the table which has not implements the related interface. Currently, there is no existing connector maintained by flink has supported UPDATE yet.

## Run a UPDATE statement

{{< tabs "update statement" >}}
{{< tab "Java" >}}

UPDATE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job.

The following examples show how to run a single UPDATE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}

UPDATE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job.

The following examples show how to run a single UPDATE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

UPDATE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `executeSql()` will submit a Flink job immediately, and return a `TableResult` instance which associates the submitted job.

The following examples show how to run a single UPDATE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

UPDATE statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a UPDATE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "dc09cb72-eebd-eb88-c29b-43bc3973f9f4" >}}

{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);
// register a table named "Orders"
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
// insert values
tEnv.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           1 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// update all the amount
tEnv.executeSql("UPDATE Orders SET `amount` = `amount` * 2").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// update by filter
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

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
// insert values
tEnv.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           1 |
// |                        Jessica |                         Banana |           1 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// update all the amount
tEnv.executeSql("UPDATE Orders SET `amount` = `amount` * 2").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// +--------------------------------+--------------------------------+-------------+
// |                           user |                        product |      amount |
// +--------------------------------+--------------------------------+-------------+
// |                           Lili |                          Apple |           2 |
// |                        Jessica |                         Banana |           2 |
// +--------------------------------+--------------------------------+-------------+
// 2 rows in set
// update by filter
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

# register a table named "Orders"
table_env.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");
# insert values
table_env.executeSql("insert into Orders values ('Lili', 'Apple', 1), ('Jessica', 'Banana', 1)").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           1 |
# |                        Jessica |                         Banana |           1 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
# update all the amount
table_env.executeSql("UPDATE Orders SET `amount` = `amount` * 2").wait();
table_env.executeSql("SELECT * FROM Orders").print();
# +--------------------------------+--------------------------------+-------------+
# |                           user |                        product |      amount |
# +--------------------------------+--------------------------------+-------------+
# |                           Lili |                          Apple |           2 |
# |                        Jessica |                         Banana |           2 |
# +--------------------------------+--------------------------------+-------------+
# 2 rows in set
# update by filter
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

## UPDATE ROWS

```sql
UPDATE [catalog_name.][db_name.]table_name SET column_name1 = expression1 [, column_name2 = expression2, ...][ WHERE condition ]
```

