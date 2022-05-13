---
title: "Hints"
weight: 2
type: docs
aliases:
  - /zh/dev/table/sql/hints.html
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

# Hints

{{< label Batch >}} {{< label Streaming >}}


SQL hints 是和 SQL 语句一起使用来改变执行计划的。本章介绍如何使用 SQL hints 增强各种方法。

SQL hints 一般可以用于以下：

- 增强 planner：没有完美的 planner，所以实现 SQL hints 让用户更好地控制执行是非常有意义的；
- 增加元数据（或者统计信息）：如"已扫描的表索引"和"一些混洗键（shuffle keys）的倾斜信息"的一些统计数据对于查询来说是动态的，用 hints 来配置它们会非常方便，因为我们从 planner 获得的计划元数据通常不那么准确；
- 算子（Operator）资源约束：在许多情况下，我们会为执行算子提供默认的资源配置，即最小并行度或托管内存（UDF 资源消耗）或特殊资源需求（GPU 或 SSD 磁盘）等，可以使用 SQL hints 非常灵活地为每个查询（非作业）配置资源。

<a name="dynamic-table-options"></a>
## 动态表（Dynamic Table）选项
动态表选项允许动态地指定或覆盖表选项，不同于用 SQL DDL 或 连接 API 定义的静态表选项，这些选项可以在每个查询的每个表范围内灵活地指定。

因此，它非常适合用于交互式终端中的特定查询，例如，在 SQL-CLI 中，你可以通过添加动态选项`/*+ OPTIONS('csv.ignore-parse-errors'='true') */`来指定忽略 CSV 源的解析错误。

<a name="syntax"></a>
### 语法
为了不破坏 SQL 兼容性，我们使用 Oracle 风格的 SQL hints 语法：
```sql
table_path /*+ OPTIONS(key=val [, key=val]*) */

key:
    stringLiteral
val:
    stringLiteral

```

<a name="examples"></a>
### 示例

```sql

CREATE TABLE kafka_table1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE kafka_table2 (id BIGINT, name STRING, age INT) WITH (...);

-- 覆盖查询语句中源表的选项
select id, name from kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;

-- 覆盖 join 中源表的选项
select * from
    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
    join
    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
    on t1.id = t2.id;

-- 覆盖插入语句中结果表的选项
insert into kafka_table1 /*+ OPTIONS('sink.partitioner'='round-robin') */ select * from kafka_table2;

```

{{< top >}}
