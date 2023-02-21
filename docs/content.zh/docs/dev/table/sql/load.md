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

<a name="load-statements"></a>

# LOAD 语句

LOAD 语句用于加载内置的或用户自定义的模块。

<a name="run-a-load-statement"></a>

## 执行 LOAD 语句

{{< tabs "load statement" >}}
{{< tab "Java" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 LOAD 语句。如果 LOAD 操作执行成功，`executeSql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 LOAD 语句。

{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 的 `executeSql()` 方法执行 LOAD 语句。如果 LOAD 操作执行成功，`executeSql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 LOAD 语句。{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 的 `execute_sql()` 方法执行 LOAD 语句。如果 LOAD 操作执行成功，`execute_sql()` 方法会返回 'OK'，否则会抛出异常。

以下示例展示了如何在 `TableEnvironment` 中执行一条 LOAD 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

LOAD 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 LOAD 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "load modules" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// 加载 hive 模块
tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.3')");
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

// 加载 hive 模块
tEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '3.1.3')")
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

# 加载 hive 模块
table_env.execute_sql("LOAD MODULE hive WITH ('hive-version' = '3.1.3')")
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
Flink SQL> LOAD MODULE hive WITH ('hive-version' = '3.1.3');
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

<a name="load-module"></a>

## LOAD MODULE

以下语法概述了可用的语法规则：
```sql
LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)]
```
{{< hint warning >}}
`module_name` 是一个简单的标识符。它是区分大小写的，由于它被用于执行模块发现，因此也要与模块工厂（module factory）中定义的模块类型相同。属性 `('key1' = 'val1', 'key2' = 'val2', ...)` 是一个 map 结构，它包含一组键值对（不包括 'type' 的键），这些属性会被传递给模块发现服务以实例化相应的模块。
{{< /hint >}}
