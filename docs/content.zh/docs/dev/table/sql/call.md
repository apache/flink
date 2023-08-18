---
title: "CALL 语句"
weight: 19
type: docs
aliases:
- /zh/dev/table/sql/call.html
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

# Call Statements

`Call` 语句用来调用存储过程。存储过程通常是用来执行一些数据操作和管理任务的。

<span class="label label-danger">注意</span> 目前 `Call` 语句要求被调用的存储过程在对应的 Catalog 中。所以，在调用存储过程前，请确保该存储过程在对应的 Catalog 中。
否则就会抛出异常。你可能需要查看对应 Catalog 的文档来知道该 Catalog 有哪些存储过程可用。要实现一个存储过程，请参阅 [存储过程]({{< ref "docs/dev/table/procedures" >}})。

## Run a CALL statement

{{< tabs "call statement" >}}

{{< tab "Java" >}}

CALL 语句可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 CALL 语句时会立即调用这个存储过程，并且返回一个 `TableResult` 对象，通过该对象可以获取调用存储过程的结果。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 CALL 语句。

{{< /tab >}}
{{< tab "Scala" >}}

CALL 语句可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行。`executeSql()` 方法执行 CALL 语句时会立即调用这个存储过程，并且返回一个 `TableResult` 对象，通过该对象可以获取调用存储过程的结果。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 CALL 语句。
{{< /tab >}}
{{< tab "Python" >}}

CALL 语句可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行。`execute_sql()` 方法执行 CALL 语句时会立即调用这个存储过程，并且返回一个 `TableResult` 对象，通过该对象可以获取调用存储过程的结果。

以下的例子展示了如何在 `TableEnvironment` 中执行一条 CALL 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 CALL 语句。

以下的例子展示了如何在 SQL CLI 中执行一条 CALL 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "da4af028-303d-11ee-be56-0242ac120002" >}}

{{< tab "Java" >}}
```java
TableEnvironment tEnv = TableEnvironment.create(...);

// 假设存储过程 `generate_n` 已经存在于当前 catalog 的 `system` 数据库
tEnv.executeSql("CALL `system`.generate_n(4)").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tEnv = TableEnvironment.create(...)

// 假设存储过程 `generate_n` 已经存在于当前 catalog 的 `system` 数据库
tEnv.executeSql("CALL `system`.generate_n(4)").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 假设存储过程 `generate_n` 已经存在于当前 catalog 的 `system` 数据库
table_env.execute_sql().print()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
-- 假设存储过程 `generate_n` 已经存在于当前 catalog 的 `system` 数据库
Flink SQL> CALL `system`.generate_n(4);
+--------+
| result |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |    
+--------+
4 rows in set
!ok
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

## Syntax

```sql
CALL [catalog_name.][database_name.]procedure_name ([ expression [, expression]* ] )
```

