---
title: "UNLOAD 语句"
weight: 13
type: docs
aliases:
  - /zh/dev/table/sql/unload.html
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

<a name="unload-statements"></a>

# UNLOAD 语句

UNLOAD 语句用于卸载内置的或用户自定义的模块。

<a name="run-a-unload-statement"></a>

## 执行 UNLOAD 语句

{{< tabs "unload statement" >}}
{{< tab "Java" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 UNLOAD 语句。如果 UNLOAD 操作执行成功，`executeSql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 UNLOAD 语句。

{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 UNLOAD 语句。如果 UNLOAD 操作执行成功，`executeSql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 UNLOAD 语句。
{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 的 `execute_sql()` 方法执行 UNLOAD 语句。如果 UNLOAD 操作执行成功，`execute_sql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 UNLOAD 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

UNLOAD 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 UNLOAD 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "unload modules" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 卸载 core 模块
tEnv.executeSql("UNLOAD MODULE core");
tEnv.executeSql("SHOW MODULES").print();
// Empty set
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// 卸载 core 模块
tEnv.executeSql("UNLOAD MODULE core")
tEnv.executeSql("SHOW MODULES").print()
// Empty set
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = StreamTableEnvironment.create(...)

# 卸载 core 模块
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

<a name="unload-module"></a>

## UNLOAD MODULE

以下语法概述了可用的语法规则：
```sql
UNLOAD MODULE module_name
```
