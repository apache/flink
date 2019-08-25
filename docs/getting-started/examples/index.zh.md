---
title: 示例
nav-id: examples
nav-title: '<i class="fa fa-file-code-o title appetizer" aria-hidden="true"></i> 示例'
nav-parent_id: getting-started
nav-pos: 3
nav-show_overview: true
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


## 附带示例

在Flink源文件中，包含了许多 Flink 不同 API 接口的代码示例：

* DataStream 应用 ({% gh_link flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples "Java" %} / {% gh_link flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples "Scala" %}) 
* DataSet 应用 ({% gh_link flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java "Java" %} / {% gh_link flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala "Scala" %})
* Table API / SQL 查询 ({% gh_link flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java "Java" %} / {% gh_link flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala "Scala" %})

这些[代码示例]({{ site.baseurl }}/zh/dev/batch/examples.html#running-an-example)清晰的解释了如何运行一个 Flink 程序。

## 网上示例

在网上也有一些博客讨论了 Flink 应用示例

* [如何使用 Apache Flink 构建有状态流数据应用](https://www.infoworld.com/article/3293426/big-data/how-to-build-stateful-streaming-applications-with-apache-flink.html)，这篇博客提供了基于 DataStream API 以及两个用于流数据分析的 SQL 查询实现的事件驱动的应用程序。

* [使用 Apache Flink、Elasticsearch 和 Kibana 构建实时仪表板应用程序](https://www.elastic.co/blog/building-real-time-dashboard-applications-with-apache-flink-elasticsearch-and-kibana)是 Elastic 的一篇博客，它向我们提供了一种基于 Apache Flink、Elasticsearch 和 Kibana 构建流数据分析实时仪表板的解决方案。

* 来自 Ververica 的 [Flink 学习网站](https://training.ververica.com/)也有许多示例。你可以从中选取能够亲自实践的部分，并加以练习。

{% top %}
