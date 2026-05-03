---
title: "概览"
is_beta: false
weight: 1
type: docs
aliases:
  - /zh/dev/table/
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

# Table API

Table API 是用于 Java、Scala 和 Python 语言的查询 API，它可以用一种非常直观的方式来组合使用选取、过滤、join 等关系型算子。Table API 与 [Flink SQL]({{< ref "docs/sql/overview" >}}) 共享同一个底层引擎，无论输入是连续的（流式）还是有界的（批处理），在两个接口中指定的查询都具有相同的语义，并指定相同的结果。

Table API 和 SQL 两种 API 是紧密集成的，以及 DataStream API。你可以在这些 API 之间，以及一些基于这些 API 的库之间轻松的切换。例如，你可以使用 [`MATCH_RECOGNIZE` 子句]({{< ref "docs/sql/reference/queries/match_recognize" >}})从表中检测模式，然后使用 DataStream API 基于匹配的模式构建告警。

## 何时使用 Table API

在以下情况下使用 Table API：
- 在 Java、Scala 或 Python 代码中编写关系型查询
- 使用类型安全的算子以编程方式组合查询
- 将 SQL 语句与编程式表操作相结合
- 与 DataStream API 集成以实现混合用例

如需纯 SQL 使用而无需编程，请参阅 [Flink SQL]({{< ref "docs/sql/overview" >}})。

## Table 程序依赖

您需要将 Table API 作为依赖项添加到项目中，以便用 Table API 定义数据管道。

有关如何为 Java 和 Scala 配置这些依赖项的更多细节，请查阅[项目配置]({{< ref "docs/dev/configuration/overview" >}})小节。

如果您使用 Python，请查阅 [PyFlink 文档]({{< ref "docs/dev/python" >}})。

## 接下来？

* [公共概念和 API]({{< ref "docs/dev/table/common" >}}): Table API 和 SQL 公共概念以及 API。
* [数据类型]({{< ref "docs/sql/reference/data-types" >}}): 内置数据类型以及它们的属性
* [流式概念]({{< ref "docs/concepts/sql-table-concepts/overview" >}}): Table API 和 SQL 中流式相关的文档，比如配置时间属性和如何处理更新结果。
* [连接外部系统]({{< ref "docs/connectors/table/overview" >}}): 读写外部系统的连接器和格式。
* [Table API 操作]({{< ref "docs/dev/table/tableApi" >}}): Table API 支持的操作。
* [SQL 参考]({{< ref "docs/sql/reference/overview" >}}): SQL 支持的操作和语法。
* [内置函数]({{< ref "docs/sql/functions/built-in-functions" >}}): Table API 和 SQL 中的内置函数。
* [PyFlink]({{< ref "docs/dev/python" >}}): Python 相关的 Flink 文档。

{{< top >}}
