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

# Table API & SQL

Apache Flink 有两种关系型 API 来做流批统一处理：Table API 和 SQL。Table API 是用于 Scala 和 Java 语言的查询API，它可以用一种非常直观的方式来组合使用选取、过滤、join 等关系型算子。Flink SQL 是基于 [Apache Calcite](https://calcite.apache.org) 来实现的标准 SQL。无论输入是连续的（流式）还是有界的（批处理），在两个接口中指定的查询都具有相同的语义，并指定相同的结果。

Table API 和 SQL 两种 API 是紧密集成的，以及 DataStream API。你可以在这些 API 之间，以及一些基于这些 API 的库之间轻松的切换。比如，你可以先用 [CEP]({{< ref "docs/libs/cep" >}}) 从 DataStream 中做模式匹配，然后用 Table API 来分析匹配的结果；或者你可以用 SQL 来扫描、过滤、聚合一个批式的表，然后再跑一个 [Gelly 图算法]({{< ref "docs/libs/gelly/overview" >}}) 来处理已经预处理好的数据。

## Table 程序依赖

取决于你使用的编程语言，选择 Java 或者 Scala API 来构建你的 Table API 和 SQL 程序：

{{< tabs "94f8aceb-507f-4c8f-977e-df00fe903203" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Python" >}}
{{< stable >}}
```bash
$ python -m pip install apache-flink {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}
{{< /tab >}}
{{< /tabs >}}

除此之外，如果你想在 IDE 本地运行你的程序，你需要添加下面的模块，具体用哪个取决于你使用哪个 Planner：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

### 扩展依赖

如果你想实现[自定义格式或连接器]({{< ref "docs/dev/table/sourcesSinks" >}}) 用于（反）序列化行或一组[用户定义的函数]({{< ref "docs/dev/table/functions/udfs" >}})，下面的依赖就足够了，编译出来的 jar 文件可以直接给 SQL Client 使用：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```

{{< top >}}

接下来？
-----------------

* [公共概念和 API]({{< ref "docs/dev/table/common" >}}): Table API 和 SQL 公共概念以及 API。
* [数据类型]({{< ref "docs/dev/table/types" >}}): 内置数据类型以及它们的属性
* [流式概念]({{< ref "docs/dev/table/concepts/overview" >}}): Table API 和 SQL 中流式相关的文档，比如配置时间属性和如何处理更新结果。
* [连接外部系统]({{< ref "docs/connectors/table/overview" >}}): 读写外部系统的连接器和格式。
* [Table API]({{< ref "docs/dev/table/tableApi" >}}): Table API 支持的操作。
* [SQL]({{< ref "docs/dev/table/sql/overview" >}}): SQL 支持的操作和语法。
* [内置函数]({{< ref "docs/dev/table/functions/systemFunctions" >}}): Table API 和 SQL 中的内置函数。
* [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}): 不用编写代码就可以尝试 Flink SQL，可以直接提交 SQL 任务到集群上。

{{< top >}}
