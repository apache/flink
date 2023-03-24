---
title: "JAR 语句"
weight: 16
type: docs
aliases:
  - /zh/dev/table/sql/jar.html
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
- SHOW JARS
- REMOVE JAR

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
[INFO] Execute statement succeed.

Flink SQL> ADD JAR 'hdfs:///udf/common-udf.jar';
[INFO] Execute statement succeed.

Flink SQL> SHOW JARS;
+----------------------------+
|                       jars |
+----------------------------+
|            /path/hello.jar |
| hdfs:///udf/common-udf.jar |
+----------------------------+

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

添加一个 JAR 文件到资源列表中，该 jar 应该位于 Flink 当前支持的本地或远程[文件系统]({{< ref "docs/deployment/filesystems/overview" >}}) 中。添加的 JAR 文件可以使用 [`SHOW JARS`](#show-jars) 语句列出。

### 限制

请不要通过 `ADD JAR` 语句来加载 Hive 的source、sink、function、catalog。这是 Hive connector 的一个已知限制，且会在将来版本中修复。当前，建议跟随这个指南来[安装 Hive 的集成]({{< ref "docs/connectors/table/hive/overview" >}}#dependencies)。

<a name="show-jars"></a>

## SHOW JARS

```sql
SHOW JARS
```

展示所有通过 [`ADD JAR`](#add-jar) 语句添加的 jar。

<a name="remove-jar"></a>

## REMOVE JAR

```sql
REMOVE JAR '<path_to_filename>.jar'
```

删除由 [`ADD JAR`](#add-jar) 语句添加的指定 jar。

<span class="label label-danger">注意</span> REMOVE JAR 语句仅适用于 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}})。

{{< top >}}
