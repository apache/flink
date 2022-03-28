---
title: "连接器和格式"
weight: 5
type: docs
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

# 连接器和格式

Flink 应用程序可以通过连接器读取和写入各种外部系统。它支持多种格式，以便对数据进行编码和解码以匹配 Flink 的数据结构。

[DataStream]({{< ref "docs/connectors/datastream/overview.zh.md" >}}) 和 [Table API/SQL]({{< ref "docs/connectors/table/overview.zh.md" >}}) 都提供了连接器和格式的概述。

## 可用的组件

为了使用连接器和格式，您需要确保 Flink 可以访问实现了这些功能的组件。对于 Flink 社区支持的每个连接器，我们在 [Maven Central](https://search.maven.org) 发布了两类组件：

* `flink-connector-<NAME>` 这是一个精简 JAR，仅包括连接器代码，但不包括最终的第三方依赖项；
* `flink-sql-connector-<NAME>` 这是一个包含连接器第三方依赖项的 uber JAR；

这同样适用于格式。请注意，某些连接器可能没有相应的 `flink-sql-connector-<NAME>` 组件，因为它们不需要第三方依赖项。

{{< hint info >}}
uber/fat JAR 主要与[SQL 客户端]({{< ref "docs/dev/table/sqlClient" >}})一起使用，但您也可以在任何 DataStream/Table 应用程序中使用它们。
{{< /hint >}}

## 使用组件

为了使用连接器/格式模块，您可以：

* 把精简 JAR 及其传递依赖项打包进您的作业 JAR；
* 把 uber JAR 打包进您的作业 JAR；
* 把 uber JAR 直接复制到 Flink 发行版的 `/lib` 文件夹内；

关于打包依赖项，请查看 [Maven]({{< ref "docs/dev/configuration/maven" >}}) 和 [Gradle]({{< ref "docs/dev/configuration/gradle" >}}) 指南。有关 Flink 发行版的参考，请查看[Flink 依赖剖析]({{< ref "docs/dev/configuration/overview" >}}#Flink-依赖剖析)。

{{< hint info >}}
决定是打成 uber JAR、精简 JAR 还是仅在发行版包含依赖项取决于您和您的使用场景。如果您使用 uber JAR，您将对作业里的依赖项版本有更多的控制权；如果您使用精简 JAR，由于您可以在不更改连接器版本的情况下更改版本（允许二进制兼容），您将对传递依赖项有更多的控制权；如果您直接在 Flink 发行版的 `/lib` 目录里内嵌连接器 uber JAR，您将能够在一处控制所有作业的连接器版本。
{{< /hint >}}
