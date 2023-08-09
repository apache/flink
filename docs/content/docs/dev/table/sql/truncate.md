---
title: "TRUNCATE Statements"
weight: 8
type: docs
aliases:
- /dev/table/sql/truncate.html
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

# TRUNCATE Statements

{{< label Batch >}}

TRUNCATE statements are used to delete all rows from a table without dropping the table itself.

<span class="label label-danger">Attention</span> Currently, `TRUNCATE` statement is supported in batch mode, and it requires the target table connector implements the  {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsTruncate.java" name="SupportsTruncate" >}}
interface to support the row-level delete. An exception will be thrown if trying to `TRUNCATE` a table which has not implemented the related interface.


## Run a TRUNCATE statement

{{< tabs "truncate" >}}
{{< tab "Java" >}}
TRUNCATE statement can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method will throw an exception when there is any error for the operation.

The following examples show how to run a TRUNCATE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Scala" >}}

TRUNCATE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` will throw an exception when there is any error for the operation.

The following examples show how to run a single TRUNCATE statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

TRUNCATE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` will throw an exception when there is any error for the operation.

The following examples show how to run a single TRUNCATE statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

TRUNCATE statement can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a TRUNCATE statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "a5de1760-e363-4b8d-9d6f-0bacb35b9dcf" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
TableEnvironment tEnv = TableEnvironment.create(settings);

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)");

// insert values
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
        
// truncate the table "Orders"
tEnv.executeSql("TRUNCATE TABLE Orders").await();
tEnv.executeSql("SELECT * FROM Orders").print();
// Empty set
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val settings = EnvironmentSettings.newInstance().inBatchMode().build()
val tEnv = StreamTableEnvironment.create(env, settings)

// register a table named "Orders"
tEnv.executeSql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)")
// insert values
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
// truncate the table "Orders"
tEnv.executeSql("TRUNCATE TABLE Orders").await()
tEnv.executeSql("SELECT * FROM Orders").print()
// Empty set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# register a table named "Orders"
table_env.execute_sql("CREATE TABLE Orders (`user` STRING, product STRING, amount INT) WITH (...)")
# insert values
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
# truncate the table "Orders"
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

## Syntax

```sql
TRUNCATE TABLE [catalog_name.][db_name.]table_name
```
