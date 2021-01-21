---
title: "SQL Hints"
nav-parent_id: sql
nav-pos: 6
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

* This will be replaced by the TOC
{:toc}

可以将 SQL 提示（hints）与 SQL 语句一起使用改变执行计划（execution plans）。本章介绍如何使用 SQL 提示增强各种方法。

SQL 提示一般可以用于以下：

- Enforce planner：没有十分契合的 planner，所以实现提示让用户更好地控制执行是十分有意义的；
- 增加元数据（或者统计信息）：如 "table index for scan" 和 "skew info of some shuffle keys" 的一些统计数据对于查询来说是动态的，用提示来配置它们会非常方便，因为我们从 planner 获得的计划元数据通常不那么准确；
- 算子（Operator）资源约束：在许多情况下，我们会为执行算子提供默认的资源配置，即最小并行度或托管内存（UDF 资源消耗）或特殊资源需求（GPU 或 SSD 磁盘）等，可以使用 SQL 提示非常灵活地为每个查询（非作业）配置资源。

<a name="dynamic-table-options"></a>
## 动态表（Dynamic Table）选项
动态表选项允许动态地指定或覆盖表选项，不同于用 SQL DDL 或 连接 API 定义的静态表选项，这些选项可以在每个查询的每个表范围内灵活地指定。

因此，它非常适合用于交互式终端中的特定查询，例如，在 SQL-CLI 中，你可以通过添加动态选项`/*+ OPTIONS('csv.ignore-parse-errors'='true') */`来指定忽略 CSV 源的解析错误。

<b>注意：</b>动态表选项默认值禁止使用，因为它可能会更改查询的语义。你需要将配置项 `table.dynamic-table-options.enabled` 显式设置为 `true`（默认值为 false），请参阅 <a href="{% link dev/table/config.zh.md %}">Configuration</a> 了解有关如何设置配置选项的详细信息。

<a name="syntax"></a>
### 语法
为了不破坏 SQL 兼容性，我们使用 Oracle 风格的 SQL 提示语法：
{% highlight sql %}
table_path /*+ OPTIONS(key=val [, key=val]*) */

key:
    stringLiteral
val:
    stringLiteral

{% endhighlight %}

<a name="examples"></a>
### 示例

{% highlight sql %}

CREATE TABLE kafka_table1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE kafka_table2 (id BIGINT, name STRING, age INT) WITH (...);

-- 在查询源中覆盖表选项
select id, name from kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;

-- 在 join 中覆盖表选项
select * from
    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
    join
    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
    on t1.id = t2.id;

-- 对 INSERT 目标表使用覆盖表选项
insert into kafka_table1 /*+ OPTIONS('sink.partitioner'='round-robin') */ select * from kafka_table2;

{% endhighlight %}

{% top %}
