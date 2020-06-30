---
title: "Table 和 SQL 连接器"
nav-id: sql-connectors
nav-parent_id: connectors-root
nav-pos: 2
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


Flink Table 和 SQL 可以连接到外部系统进行批和流的读写。table source 用于读取存储在外部系统（例如数据库、键值存储、消息队列或者文件系统）中的数据。table sink 可以将表存储到另一个外部的系统中。不同的 source 和 sink 支持不同的数据格式，例如 CSV、Avro、Parquet 或者 ORC。

本文档主要描述如何使用内置支持的连接器（connector）注册 table source 和 sink。在 source 或 sink 注册完成之后，就可以在 Table API 和 SQL 中访问它们了。

<span class="label label-info">注意</span> 如果你想要实现自定义 table source 或 sink， 可以查看 [自定义 source 和 sink]({% link dev/table/sourceSinks.zh.md %})。

<span class="label label-danger">注意</span> Flink Table & SQL 在 1.11.0 之后引入了一组新的连接器选项， 如果你现在还在使用遗留（legacy）连接器选项，可以查阅 [遗留文档]({% link dev/table/connect.zh.md %})。

* This will be replaced by the TOC
{:toc}

已经支持的连接器
------------

Flink 原生支持各种不同的连接器。下表列出了所有可用的连接器。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">名称</th>
        <th class="text-center">版本</th>
        <th class="text-center">源端</th>
        <th class="text-center">目标端</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><a href="{% link dev/table/connectors/filesystem.zh.md %}">Filesystem</a></td>
      <td></td>
      <td>有界和无界的扫描和查询</td>
      <td>流式，批处理</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/elasticsearch.zh.md %}">Elasticsearch</a></td>
      <td>6.x & 7.x</td>
      <td>不支持</td>
      <td>流式，批处理</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/kafka.zh.md %}">Apache Kafka</a></td>
      <td>0.10+</td>
      <td>无界的扫描</td>
      <td>流式，批处理</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/jdbc.zh.md %}">JDBC</a></td>
      <td></td>
      <td>有界的扫描和查询</td>
      <td>流式，批处理</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/hbase.zh.md %}">Apache HBase</a></td>
      <td>1.4.x</td>
      <td>有界的扫描和查询</td>
      <td>流式，批处理</td>
    </tr>
    </tbody>
</table>

{% top %}

如何使用连接器
--------

FLink 支持使用 SQL 建表语句来进行注册 table。可以定义表的名称、表结构、以及连接外部系统用的一些选项。

下面的代码展示了如何读取 Kafka 并且用 Json 解析数据的一个完整的例子。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyUserTable (
  -- 声明表结构
  `user` BIGINT,
  message STRING,
  ts TIMESTAMP,
  proctime AS PROCTIME(), -- 使用计算列定义处理时间属性
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- 使用 WATERMARK 语句定义事件时间属性
) WITH (
  -- 声明要连接的外部系统
  'connector' = 'kafka',
  'topic' = 'topic_name',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'   -- 声明此系统的格式
)
{% endhighlight %}
</div>
</div>

使用普遍性连接参数已经转变成了基于字符串的键值对。 其实表工厂 [table factories]({% link dev/table/sourceSinks.zh.md %}#define-a-tablefactory) 利用对应的键值对来创建相应的 table 源端，table 目标端，和相应的格式。所有的表工厂可以在 Java’s Service Provider Interfaces (SPI) 里面被找到。 现在已经经可能的考虑到所有各类涉及到的表对应匹配的表工厂设计。

如果没有找到对应的表工厂，或者没有找不到多个工厂指定属性参数的配置， 则将引发一个异常，其中包含有关考虑的工厂和支持的属性的附加信息。

{% top %}

结构映射
------------

SQL 创建表语句中有相应的定义列、约束和水印的名称和类型等语句。Flink 不保存数据，因此模式定义只声明如何将类型从外部系统映射到Flink的表示。映射可能不能按名称映射，这取决于格式和连接器的实现。例如，MySQL 数据库表是按字段名映射的（不区分大小写），CSV文件系统是按字段顺序映射的（字段名可以是任意的）。这将在每个连接器中解释。

下面的示例显示了一个没有时间属性和输入/输出到表列的一对一字段映射的简单模式。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

### 主键

主键约束告诉表的一列或一组列是唯一的，并且它们不包含空值。主键唯一标识表中的行。

源表的主键是用于优化的元数据信息。目标端 table 主键通常由目标端实现用于 upsperting。

SQL 标准指定可以强制或不强制约束。这将控制是否对传入/传出数据执行约束检查。Flink不拥有数据我们只想支持非强制模式。由用户来确保查询执行的完整性。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN,
  PRIMARY KEY (MyField1, MyField2) NOT ENFORCED  -- 声明在列上定义主键
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

### 时间属性

使用无边界流表时，时间属性非常重要。因此，proctime 和 rowtime 属性都可以定义为模式的一部分。

有关Flink中时间处理的更多信息，特别是事件时间，我们建议使用 [event-time section]({% link dev/table/streaming/time_attributes.zh.md %})。

#### 程序运行时间属性

为了在模式中声明 proctime 属性，可以使用计算列语法 [Computed Column syntax]({% link dev/table/sql/create.zh.md %}#create-table) 声明从 proctime（）内置函数生成的计算列。
计算列是虚拟的，并不是存储在物理表列中，是不具备真实意义的列。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
  MyField4 AS PROCTIME() -- 声明 proctime 属性
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

#### 行事件时间属性

为了控制表的事件时间行为，Flink 提供了预定义的时间戳抽取器和水印策略。

有关在 DDL 中定义时间属性的更多信息，请参阅 [CREATE TABLE statements]({% link dev/table/sql/create.zh.md %}#create-table)。

支持以下时间戳提取程序：

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
-- 使用模式中现有的 TIMESTAMP（3）字段作为 rowtime 属性
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)

-- 使用系统函数或 udf 或表达式来提取预期的时间戳（3）rowtime 字段
CREATE TABLE MyTable (
  log_ts STRING,
  ts_field AS TO_TIMESTAMP(log_ts),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

支持以下水印策略：

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
-- 为严格递增的行时属性设置水印策略。发出
-- 迄今最大监视到的最大时间戳。时间戳小于最大时间戳的行
-- 不会迟到的。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field
) WITH (
  ...
)

-- 为升序行时属性设置水印策略。发出最大值的水印
-- 到目前为止监视到的时间戳减1。时间戳等于最大时间戳的行
-- 不会迟到的。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '0.001' SECOND
) WITH (
  ...
)

-- 针对行时间属性在有界时间间隔内无序的情况，设置一种水印策略。
-- 发出最大监视时间戳减去指定延迟（例如2秒）的水印。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '2' SECOND
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

确保始终声明时间戳和水印。触发基于时间的操作需要水印。

### SQL 类型

请参阅数据类型页 [Data Types]({% link dev/table/types.zh.md %})，了解如何在SQL中声明类型。

{% top %}
