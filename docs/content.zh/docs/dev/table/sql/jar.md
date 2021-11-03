---
title: "JAR Statements"
weight: 16
type: docs
aliases:
  - /dev/table/sql/jar.html
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

<a name="jar-statements"></a>

# JAR 语句

JAR 语句用于将用户 jar 添加到 classpath、或将用户 jar 从 classpath 中删除或展示运行时 classpath 中添加的 jar。

目前 Flink SQL 支持以下 JAR 语句：
- ADD JAR
- REMOVE JAR
- SHOW JARS

<span class="label label-danger">注意</span> JAR 语句仅适用于 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}})。

<a name="run-a-jar-statement"></a>

## 执行 JAR 语句

{{< tabs "add jar statement" >}}
{{< tab "SQL CLI" >}}

以下示例展示了如何在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中运行 JAR 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "add jar" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> ADD JAR '/path/hello.jar';
[INFO] The specified jar is added into session classloader.

Flink SQL> SHOW JARS;
/path/hello.jar

Flink SQL> REMOVE JAR '/path/hello.jar';
[INFO] The specified jar is removed from session classloader.
```
{{< /tab >}}
{{< /tabs >}}

<a name="add-jar"></a>

## ADD JAR

```sql
ADD JAR '<path_to_filename>.jar'
```

目前只支持将本地 jar 添加到会话类类加载器（session classloader）中。

<a name="remove-jar"></a>

## REMOVE JAR

```sql
REMOVE JAR '<path_to_filename>.jar'
```

目前只支持删除 [`ADD JAR`](#add-jar) 语句添加的 jar。

<a name="show-jars"></a>

## SHOW JARS

```sql
SHOW JARS
```

展示会话类类加载器（session classloader）中所有基于 [`ADD JAR`](#add-jar) 语句添加的 jar。

{{< top >}}
