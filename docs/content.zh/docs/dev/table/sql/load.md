---
title: "LOAD 语句"
weight: 12
type: docs
aliases:
  - /zh/dev/table/sql/load.html
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

# LOAD Statements

LOAD statements are used to load a built-in or user-defined module.

## Run a LOAD statement

{{< tabs "load statement" >}}
{{< tab "Java" >}}

LOAD statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful LOAD operation; otherwise, it will throw an exception.

The following examples show how to run a LOAD statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}

LOAD statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful LOAD operation; otherwise, it will throw an exception.

The following examples show how to run a LOAD statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

LOAD statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful LOAD operation; otherwise, it will throw an exception.

The following examples show how to run a LOAD statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

LOAD statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a LOAD statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "load modules" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// load a hive module
tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.2')");
tEnv.executeSql("SHOW MODULES").print();
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// load a hive module
tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.2')")
tEnv.executeSql("SHOW MODULES").print()
// +-------------+
// | module name |
// +-------------+
// |        core |
// |        hive |
// +-------------+

```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = StreamTableEnvironment.create(...)

# load a hive module
table_env.execute_sql("LOAD MODULE hive WITH ('hive-version' = '3.1.2')")
table_env.execute_sql("SHOW MODULES").print()
# +-------------+
# | module name |
# +-------------+
# |        core |
# |        hive |
# +-------------+

```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> LOAD MODULE hive WITH ('hive-version' = '3.1.2');
[INFO] Load module succeeded!

Flink SQL> SHOW MODULES;
+-------------+
| module name |
+-------------+
|        core |
|        hive |
+-------------+

```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## LOAD MODULE

The following grammar gives an overview of the available syntax:
```sql
LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)]
```
{{< hint warning >}}
`module_name` is a simple identifier. It is case-sensitive and should be identical to the module type defined in the module factory because it is used to perform module discovery.
Properties `('key1' = 'val1', 'key2' = 'val2', ...)` is a map that contains a set of key-value pairs (except for the key `'type'`) and passed to the discovery service to instantiate the corresponding module.
{{< /hint >}}
