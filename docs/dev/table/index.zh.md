---
title: "Table API & SQL"
nav-id: tableapi
nav-parent_id: dev
is_beta: false
nav-show_overview: true
nav-pos: 35
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

Apache Flink 有两种关系型 API 来做流批统一处理：Table API 和 SQL。Table API 是用于 Java，Scala 和 Python 语言的查询API，它可以用一种非常直观的方式来组合使用选取、过滤、join 等关系型算子。Flink SQL 是基于 [Apache Calcite](https://calcite.apache.org) 来实现的标准 SQL。这两种 API 中的查询对于连续（streaming）和有界（batch）的输入有相同的语义，也会产生同样的计算结果。

Table API 和 SQL 两种 API 是紧密集成的，这两种 API 与 DataStream 也是紧密集成的。你可以在这些 API 之间，以及一些基于这些 API 的库之间轻松的切换。例如，你可以使用 [`MATCH_RECOGNIZE` clause]({% link dev/table/streaming/match_recognize.zh.md %}) 子句从表中设置检测模式，然后使用 DataStream API 根据匹配的检测模式构建警报。

Table Planners
--------------------

Table Planners 负责将关系运算符转换为可执行的，优化后的 Flink 作业。
Flink 支持两种不同的 planner 实现；新版的 Blink planner 和旧版 planner。
对于生产环境，我们建议使用 Blink planner，它从 1.11 开始成为默认 planner。
有关如何在两个 planner 之间进行切换的更多信息，请参见 [common API]({% link dev/table/common.zh.md %}) 页面。

### Table 程序依赖

取决于你使用的编程语言，你可以选择 Java，Scala 或 Python API 来构建你的 Table API 和 SQL 程序。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="python">
{% highlight bash %}
{% if site.is_stable %}
$ python -m pip install apache-flink {{ site.version }}
{% else %}
$ python -m pip install apache-flink
{% endif %}
{% endhighlight %}
</div>
</div>

除此之外，如果你想在 IDE 本地运行你的程序，你需要添加下面的模块，具体用哪个取决于你使用哪个 Planner。

<div class="codetabs" markdown="1">
<div data-lang="Blink Planner" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="Legacy Planner" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
</div>

### 扩展依赖

如果你想实现[自定义格式]({% link dev/table/sourceSinks.zh.md %}#define-a-tablefactory)来解析 Kafka 数据，或者[自定义函数]({% link dev/table/functions/systemFunctions.zh.md %})，下面的依赖就足够了，编译出来的 jar 文件可以直接给 SQL Client 使用：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}

{% top %}

接下来？
-----------------

* [概念和通用 API]({% link dev/table/common.zh.md %}): Table API 和 SQL 的公共概念以及 API。
* [数据类型]({% link dev/table/types.zh.md %}): 内置数据类型以及它们的属性
* [流式概念]({% link dev/table/streaming/index.zh.md %}): Table API 和 SQL 中流式相关的文档，比如配置时间属性和如何处理更新结果。
* [连接外部系统]({% link dev/table/connect.zh.md %}): 读写外部系统的连接器和格式。
* [Table API]({% link dev/table/tableApi.zh.md %}): Table API 支持的操作。
* [SQL]({% link dev/table/sql/index.zh.md %}): SQL 支持的操作和语法。
* [内置函数]({% link dev/table/functions/systemFunctions.zh.md %}): Table API 和 SQL 中的内置函数。
* [SQL Client]({% link dev/table/sqlClient.zh.md %}): 不用编写代码就可以尝试 Flink SQL，可以直接提交 SQL 任务到集群上。

{% top %}
