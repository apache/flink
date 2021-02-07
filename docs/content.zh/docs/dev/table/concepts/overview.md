---
title: "流式概念"
weight: 1
type: docs
aliases:
  - /zh/dev/table/streaming/
is_beta: false
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

# 流式概念

Flink 的 [Table API]({{< ref "docs/dev/table/tableApi" >}}) 和 [SQL]({{< ref "docs/dev/table/sql/overview" >}}) 是流批统一的 API。
这意味着 Table API & SQL 在无论有限的批式输入还是无限的流式输入下，都具有相同的语义。
因为传统的关系代数以及 SQL 最开始都是为了批式处理而设计的，
关系型查询在流式场景下不如在批式场景下容易懂。

下面这些页面包含了概念、实际的限制，以及流式数据处理中的一些特定的配置。

接下来？
-----------------

* [动态表]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}): 描述了动态表的概念。
* [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}): 解释了时间属性以及它是如何在 Table API & SQL 中使用的。
* [流上的 Join]({{< ref "docs/dev/table/concepts/joins" >}}): 支持的几种流上的 Join。
* [时态（temporal）表]({{< ref "docs/dev/table/concepts/versioned_tables" >}}): 描述了时态表的概念。
* [查询配置]({{< ref "docs/dev/table/config" >}}): Table API & SQL 特定的配置。

{{< top >}}
