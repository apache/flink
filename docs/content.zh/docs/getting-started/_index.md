---
title: 快速上手
icon: <i class="fa fa-rocket title appetizer" aria-hidden="true"></i>
bold: true
bookCollapseSection: true
weight: 1
aliases:
  - /zh/docs/getting-started/
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

# Apache Flink 快速上手

欢迎使用 Apache Flink！根据你的目标选择学习路径：

## 选择你的路径

| 教程 | 适合人群 | 前提条件 |
|------|---------|---------|
| [本地模式安装]({{< ref "docs/getting-started/local_installation" >}}) | **所有人** - 从这里开始安装 Flink | Java 11/17/21 或 Docker |
| [Flink SQL 教程]({{< ref "docs/getting-started/quickstart-sql" >}}) | 数据分析师、SQL 用户 | 运行中的 Flink 集群 |
| [Table API 教程]({{< ref "docs/getting-started/table_api" >}}) | 构建流处理管道的 Java/Scala 开发者 | Docker、Maven |
| [Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}}) | DevOps、平台工程师 | Docker |

## 概览

- **[本地模式安装]({{< ref "docs/getting-started/local_installation" >}})**：本地或通过 Docker 安装 Flink 并验证其运行。这是所有人的起点。

- **[Flink SQL 教程]({{< ref "docs/getting-started/quickstart-sql" >}})**：通过交互式 SQL 查询学习 Flink。无需编码 - 只需使用 SQL 客户端查询流数据。

- **[Table API 教程]({{< ref "docs/getting-started/table_api" >}})**：构建完整的流处理管道，从 Kafka 读取数据、处理数据、写入 MySQL，并在 Grafana 中可视化结果。

- **[Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}})**：学习在生产环境中运维 Flink - 故障恢复、扩缩容、升级和监控。

## 快速上手之后

完成教程后，可以探索：

- [概念]({{< ref "docs/concepts/overview" >}})：理解 Flink 的核心概念
- [学习 Flink]({{< ref "docs/learn-flink/overview" >}})：深入了解 Flink 的编程模型
- [Flink SQL]({{< ref "docs/sql/overview" >}})：完整的 SQL 参考和工具
