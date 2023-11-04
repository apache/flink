---
title: Apache Flink Documentation 
type: docs
bookToc: false
aliases:
  - /zh/examples/index.html
  - /zh/getting-started/examples/index.html
  - /zh/dev/execution_plans.html
  - /zh/docs/dev/execution/execution_plans/
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

# Apache Flink 文档

{{< center >}}
**Apache Flink** 是一个在*有界*数据流和*无界*数据流上进行有状态计算分布式处理引擎和框架。Flink 设计旨在*所有常见的集群环境*中运行，以*任意规模*和*内存*级速度执行计算。
{{< /center >}}

{{< columns >}}

### 尝试 Flink

如果你有兴趣使用 Flink，可以尝试以下任意教程：

* [基于 DataStream API 实现欺诈检测]({{< ref "docs/try-flink/datastream" >}})
* [基于 Table API 实现实时报表]({{< ref "docs/try-flink/table_api" >}})
* [PyFlink 介绍]({{< ref "docs/dev/python/overview" >}})
* [Flink 操作场景]({{< ref "docs/try-flink/flink-operations-playground" >}})

### 学习 Flink

* 为了更深入地研究，[实践训练]({{< ref "docs/learn-flink/overview" >}})包括一组课程和练习，它们提供了 Flink 的逐步介绍。

* 在浏览参考文档之前，[概念]({{< ref "docs/concepts/overview" >}})部分阐述了你需要了解的关于 Flink 的内容

### 获取有关 Flink 的帮助

如果你遇到困难，请查看我们的[社区支持资源](https://flink.apache.org/community.html)。特别是 Apache Flink 的用户邮件列表，Apache Flink 的用户邮件列表一直被列为所有 Apache 项目中最活跃的项目邮件列表之一，是快速获得帮助的好方法。

<--->

### 探索 Flink

参考文档涵盖了所有细节。一些起始点链接如下：

{{< columns >}}
* [DataStream API]({{< ref "docs/dev/datastream/overview" >}})
* [Table API & SQL]({{< ref "docs/dev/table/overview" >}})
* [Stateful Functions](https://nightlies.apache.org/flink/flink-statefun-docs-stable/)

<--->

* [配置参数]({{< ref "docs/deployment/config" >}})
* [Rest API]({{< ref "docs/ops/rest_api" >}})
* [命令行]({{< ref "docs/deployment/cli" >}})
{{< /columns >}}

### 部署 Flink

在将 Flink 作业投入到生产环境之前，请阅读[生产就绪情况核对清单]({{< ref "docs/ops/production_ready" >}})。
关于合理部署目标的概述，请参阅[集群和部署]({{< ref "docs/deployment/overview" >}}).

### 升级 Flink

发行说明涵盖了 Flink 版本之间的重要变化。如果你打算升级 Flink 设置，请仔细阅读发行说明。

<!--
For some reason Hugo will only allow linking to the 
release notes if there is a leading '/' and file extension.
-->
请参阅 [Flink 1.18]({{< ref "/release-notes/flink-1.18.md" >}}),
[Flink 1.17]({{< ref "/release-notes/flink-1.17.md" >}})，
[Flink 1.16]({{< ref "/release-notes/flink-1.16.md" >}})，
[Flink 1.15]({{< ref "/release-notes/flink-1.15.md" >}})，
[Flink 1.14]({{< ref "/release-notes/flink-1.14.md" >}})，
[Flink 1.13]({{< ref "/release-notes/flink-1.13.md" >}})， 
[Flink 1.12]({{< ref "/release-notes/flink-1.12.md" >}})，
[Flink 1.11]({{< ref "/release-notes/flink-1.11.md" >}})，
[Flink 1.10]({{< ref "/release-notes/flink-1.10.md" >}})，
[Flink 1.9]({{< ref "/release-notes/flink-1.9.md" >}})，
[Flink 1.8]({{< ref "/release-notes/flink-1.8.md" >}})，或者 
[Flink 1.7]({{< ref "/release-notes/flink-1.7.md" >}}) 的发行说明。

{{< /columns >}}

{{< build_time >}}
