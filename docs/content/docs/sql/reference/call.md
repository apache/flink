---
title: "CALL Statements"
weight: 19
type: docs
aliases:
- /dev/table/sql/call.html
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

`Call` statements are used to call a stored procedure which is usually provided to perform data manipulation or administrative tasks.

<span class="label label-danger">Attention</span> Currently, `Call` statements require the procedure called to exist in the corresponding catalog. So, please make sure the procedure exists in the catalog. 
If it doesn't exist, it'll throw an exception. You may need to refer to the doc of the catalog to see the available procedures. To implement an procedure, please refer to [Procedure]({{< ref "docs/dev/table/procedures" >}}).

## Run a CALL statement

{{< tabs "call statement" >}}

{{< tab "Java" >}}

CALL statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` will immediately call the procedure, and return a `TableResult` instance which associates the procedure.

The following examples show how to execute a CALL statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "Scala" >}}

CALL statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` will immediately call the procedure, and return a `TableResult` instance which associates the procedure.

The following examples show how to execute a single CALL statement in `TableEnvironment`.
{{< /tab >}}
{{< tab "Python" >}}

CALL statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `executeSql()` will immediately call the procedure, and return a `TableResult` instance which associates the procedure.

The following examples show how to execute a single CALL statement in `TableEnvironment`.

{{< /tab >}}
{{< tab "SQL CLI" >}}

CALL statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to execute a CALL statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "dcaced3c-3036-11ee-be56-0242ac120002" >}}

{{< tab "Java" >}}
```java
TableEnvironment tEnv = TableEnvironment.create(...);

// assuming the procedure `generate_n` has existed in `system` database of the current catalog
tEnv.executeSql("CALL `system`.generate_n(4)").print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tEnv = TableEnvironment.create(...)

// assuming the procedure `generate_n` has existed in `system` database of the current catalog
tEnv.executeSql("CALL `system`.generate_n(4)").print()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# assuming the procedure `generate_n` has existed in `system` database of the current catalog
table_env.execute_sql().print()
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
// assuming the procedure `generate_n` has existed in `system` database of the current catalog
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

