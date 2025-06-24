---
title: "UNLOAD Statements"
weight: 13
type: docs
aliases:
  - /dev/table/sql/unload.html
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

# UNLOAD Statements

UNLOAD statements are used to unload a built-in or user-defined module.

## Run a UNLOAD statement

{{< tabs "unload statement" >}}
{{< tab "Java" >}}

UNLOAD statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful UNLOAD operation; otherwise it will throw an exception.

The following examples show how to run a UNLOAD statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}

UNLOAD statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful UNLOAD operation; otherwise it will throw an exception.

The following examples show how to run a UNLOAD statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

UNLOAD statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns 'OK' for a successful UNLOAD operation; otherwise it will throw an exception.

The following examples show how to run a UNLOAD statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

UNLOAD statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a UNLOAD statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "unload modules" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// unload a core module
tEnv.executeSql("UNLOAD MODULE core");
tEnv.executeSql("SHOW MODULES").print();
// Empty set
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// unload a core module
tEnv.executeSql("UNLOAD MODULE core")
tEnv.executeSql("SHOW MODULES").print()
// Empty set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = StreamTableEnvironment.create(...)

# unload a core module
table_env.execute_sql("UNLOAD MODULE core")
table_env.execute_sql("SHOW MODULES").print()
# Empty set
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> UNLOAD MODULE core;
[INFO] Unload module succeeded!

Flink SQL> SHOW MODULES;
Empty set
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## UNLOAD MODULE

The following grammar gives an overview of the available syntax:
```sql
UNLOAD MODULE module_name
```
