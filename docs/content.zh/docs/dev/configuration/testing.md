---
title: "测试的依赖项"
weight: 6
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

# 用于测试的依赖项

Flink 提供了用于测试作业的实用程序，您可以将其添加为依赖项。

## DataStream API 测试

如果要为使用 DataStream API 构建的作业开发测试用例，则需要添加以下依赖项：

{{< artifact_tabs flink-test-utils withTestScope >}}

在各种测试实用程序中，该模块提供了 `MiniCluster` （一个可配置的轻量级 Flink 集群，能在 JUnit 测试中运行），可以直接执行作业。

有关如何使用这些实用程序的更多细节，请查看 [DataStream API 测试]({{< ref "docs/dev/datastream/testing" >}})。

## Table API 测试

如果您想在您的 IDE 中本地测试 Table API 和 SQL 程序，除了前述提到的 `flink-test-utils` 之外，您还要添加以下依赖项：

{{< artifact_tabs flink-table-test-utils withTestScope >}}

这将自动引入查询计划器和运行时，分别用于计划和执行查询。

{{< hint info >}}
`flink-table-test-utils` 模块已在 Flink 1.15 中引入，目前被认为是实验性的。
{{< /hint >}}
