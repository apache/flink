---
title: 概览
type: docs
weight: 1
bookToc: false
aliases: 
  - /zh/dev/python/
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

# Python API

{{< img src="/fig/pyflink.svg" alt="PyFlink" class="offset" width="50%" >}}

PyFlink 是 Apache Flink 的 Python API，你可以使用它构建可扩展的批处理和流处理任务，例如实时数据处理管道、大规模探索性数据分析、机器学习（ML）管道和 ETL 处理。
如果你对 Python 和 Pandas 等库已经比较熟悉，那么 PyFlink 可以让你更轻松地利用 Flink 生态系统的全部功能。
根据你需要的抽象级别的不同，有两种不同的 API 可以在 PyFlink 中使用：

* **PyFlink Table API** 允许你使用类似于 SQL 或者在 Python 中处理表格数据的方式编写强大的关系查询。
* 与此同时，**PyFlink DataStream API** 允许你对 Flink 的核心组件 [state]({{< ref "docs/concepts/stateful-stream-processing" >}}) 和 [time]({{< ref "docs/concepts/time" >}}) 进行细粒度的控制，以便构建更复杂的流处理应用。

{{< columns >}}

### 尝试 PyFlink

如果你有兴趣使用 PyFlink，可以尝试以下教程：

* [PyFlink DataStream API 介绍]({{< ref "docs/dev/python/datastream_tutorial" >}})
* [PyFlink Table API 介绍]({{< ref "docs/dev/python/table_api_tutorial" >}})

如果你想了解更多关于 PyFlink 的示例，可以参考 {{< gh_link file="flink-python/pyflink/examples" name="PyFlink 示例" >}}

<--->

### 深入 PyFlink

这些参考文档涵盖了 PyFlink 的所有细节，可以从以下链接入手：

* [PyFlink DataStream API]({{< ref "docs/dev/python/table/table_environment" >}})
* [PyFlink Table API &amp; SQL]({{< ref "docs/dev/python/datastream/operators" >}})

{{< /columns >}}

### 获取有关 PyFlink 的帮助

如果你遇到困难，请查看我们的[社区支持资源](https://flink.apache.org/community.html)，特别是 Apache Flink 的用户邮件列表，Apache Flink 的用户邮件列表一直是所有 Apache 项目中最活跃的项目邮件列表之一，是快速获得帮助的好方法。
