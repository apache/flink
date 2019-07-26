---
title: "Apache Flink 文档"
nav-pos: 0
nav-title: '<i class="fa fa-home title" aria-hidden="true"></i> Home'
nav-parent_id: root
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


本文档适用于 Apache Flink {{ site.version_title}} 版本。本页面最近更新于 {% build_time %}.

Apache Flink 是一个分布式流批一体化的开源平台。Flink 的核心是一个提供数据分发、通信以及自动容错的流计算引擎。Flink 在流计算之上构建批处理，并且原生的支持迭代计算，内存管理以及程序优化。

## 初步印象

- **概念**: 从 Flink 的 [数据流编程模型](concepts/programming-model.html) 和 [分布式执行环境](concepts/runtime.html) 开始了解最基本的概念。这能帮助你理解本文档的其他部分，包括如何搭建环境，进行程序编写等。建议你首先阅读此部分。
- **教程**:
  * [实现并运行一个 DataStream 作业](./getting-started/tutorials/datastream_api.html)
  * [搭建一个本地 Flink 集群](./getting-started/tutorials/local_setup.html)

- **编程指南**: 你可以从 [基本 API 概念](dev/api_concepts.html), [DataStream API](dev/datastream_api.html) 以及 [DataSet API](dev/batch/index.html) 着手学习如何编写你的第一个 Flink 作业。

## 部署
在线上环境运行你的 Flink 作业之前，请阅读 [生产环境注意事项检查清单](ops/production_ready.html)。

## 发布日志

发布日志包含了 Flink 版本之间的重大更新。请在你升级 Flink 之前仔细阅读相应的发布日志。

* [Flink 1.8 的发布日志](release-notes/flink-1.8.html)。
* [Flink 1.7 的发布日志](release-notes/flink-1.7.html)。
* [Flink 1.6 的发布日志](release-notes/flink-1.6.html)。
* [Flink 1.5 的发布日志](release-notes/flink-1.5.html)。

## 外部资源

- **Flink Forward**: 已举办的所有大会演讲均可在 [Flink Forward](http://flink-forward.org/) 官网以及 [YouTube](https://www.youtube.com/channel/UCY8_lgiZLZErZPF47a2hXMA)找到。[使用 Apache Flink 进行高可靠的流处理](http://2016.flink-forward.org/kb_sessions/robust-stream-processing-with-apache-flink/) 可以作为你第一个学习的资源。

- **培训**: [培训资料](https://training.ververica.com/) 包含讲义，练习以及示例程序。

- **博客**: [Apache Flink](https://flink.apache.org/blog/) 以及 [Ververica](https://www.ververica.com/blog) 的博客会经常更新一些有关 Flink 的技术文章。
