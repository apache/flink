---
title: 入门指南
nav-id: getting-started
nav-title: '<i class="fa fa-rocket title appetizer" aria-hidden="true"></i> Getting Started'
nav-parent_id: root
section-break: true
nav-show_overview: true
nav-pos: 1
always-expand: true
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

上手使用 Apache Flink 有很多方式，哪一个最适合你取决于你的目标和以前的经验。

* 通过阅读 **Docker Playgrounds** 小节中基于 Docker 的 Flink 实践来了解 Flink 的基本概念和功能。
* 可以通过 **Code Walkthroughs** 小节快速了解 Flink API。
* 可以通过 **Hands-on Training** 章节逐步全面的学习 Flink。
* 如果你已经了解 Flink 的基本概念并且想构建 Flink 项目，可以通过**项目构建设置**小节获取 Java/Scala 的项目模板或项目依赖。

### 初识 Flink

通过 **Docker Playgrounds** 提供沙箱的 Flink 环境，你只需花几分钟做些简单设置，就可以开始探索和使用 Flink。

* [**Flink Operations Playground**](./docker-playgrounds/flink-operations-playground.html) 向你展示如何使用 Flink 编写数据流应用程序。你可以体验 Flink 如何从故障中恢复应用程序，升级、提高并行度、降低并行度和监控运行的状态指标等特性。

<!--
* The [**Streaming SQL Playground**]() provides a Flink cluster with a SQL CLI client, tables which are fed by streaming data sources, and instructions for how to run continuous streaming SQL queries on these tables. This is the perfect environment for your first steps with streaming SQL.
-->

### Flink API 入门

**代码练习**是快速入门的最佳方式，通过代码练习可以逐步深入地理解 Flink API。每个示例都演示了如何构建基础的 Flink 代码框架，并如何逐步将其扩展为简单的应用程序。

<!--
* The [**DataStream API**]() code walkthrough shows how to implement a simple DataStream application and how to extend it to be stateful and use timers.
-->
* [**DataStream API 示例**](./walkthroughs/datastream_api.html) 展示了如何实现一个基本的 DataStream 应用程序，并把它扩展成有状态的应用程序。DataStream API 是 Flink 的主要抽象，可用于在 Java 或 Scala 语言中实现具有复杂时间语义的有状态数据流处理的应用程序。

* **Table API** 是 Flink 的语言嵌入式关系 API，用于在 Java，Scala 或 Python 中编写类 SQL 的查询，并且这些查询会自动进行优化。Table API 查询可以使用一致的语法和语义同时在批处理或流数据上运行。[Table API code walkthrough for Java and Scala](./walkthroughs/table_api.html) 演示了如何在批处理中简单的使用 Table API 进行查询，以及如何将其扩展为流处理中的查询。Python Table API 同上 [code walkthrough for the Python Table API](./walkthroughs/python_table_api.html)。

### 通过实操进一步探索 Flink

[Hands-on Training](/zh/training/index.html) 是一系列可供自主学习的练习课程。这些课程会循序渐进的介绍 Flink，包括如何使用 DataStream API 来满足常见的、真实的需求场景，并提供对 Flink 中并行数据流（parallel dataflows）、有状态流式处理（stateful stream processing）、Event Time、Watermarking、通过状态快照实现容错（fault tolerance via state snapshots）等基本概念的完整介绍。

<!--
### Starting a new Flink application

The **Project Setup** instructions show you how to create a project for a new Flink application in just a few steps.

* [**DataStream API**]()
* [**DataSet API**]()
* [**Table API / SQL**]()
 -->
