---
title: "向量搜素"
weight: 7
type: docs
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

# 向量搜索

{{< label Batch >}} {{< label Streaming >}}

Flink SQL 提供了 `VECTOR_SEARCH` 表值函数 (TVF) 来在 SQL 查询中执行向量搜索。该函数允许您根据高维向量搜索相似的行。

## VECTOR_SEARCH 函数

`VECTOR_SEARCH` 使用处理时间属性 (processing-time attribute) 将行与外部表中的最新版本数据关联起来。它与 Flink SQL 中的 lookup-join 非常相似，但区别在于 `VECTOR_SEARCH` 使用输入数据向量与外部表中的数据比较相似度，并返回 top-k 个最相似的行。

### 语法

```sql
SELECT * FROM input_table, LATERAL TABLE(VECTOR_SEARCH(
   TABLE vector_table, 
   input_table.vector_column, 
   DESCRIPTOR(index_column),
   top_k,
   [CONFIG => MAP['key', 'value']]
   ))
```

### 参数

* `input_table`: 包含待处理数据的输入表。
* `vector_table`: 允许通过向量进行搜索的外部表的名称。
* `vector_column`: 输入表中的列名，其类型应为 FLOAT ARRAY 或 DOUBLE ARRAY。
* `index_column`: 一个描述符 (descriptor)，指定应使用向量表 (vector_table) 中的哪一列与输入数据进行相似度比较。
* `top_k`: 要返回的 top-k 个最相似行的数量。
* `config`: (可选) 用于向量搜索的配置选项。

### 配置选项

可以在 config map 中指定以下配置选项：

{{< generated/vector_search_runtime_config_configuration >}}

### 示例

```sql
-- 基本用法
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  input_table.vector_column,
  DESCRIPTOR(index_column),
  10
));

-- 带配置选项
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  input_table.vector_column,
  DESCRIPTOR(index_column),
  10,
  MAP['async', 'true', 'timeout', '100s']
));

-- 使用命名参数
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  SEARCH_TABLE => TABLE vector_table,
  COLUMN_TO_QUERY => input_table.vector_column,
  COLUMN_TO_SEARCH => DESCRIPTOR(index_column),
  TOP_K => 10,
  CONFIG => MAP['async', 'true', 'timeout', '100s']
));

-- 使用常量值搜索
SELECT * FROM TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  ARRAY[10, 20],
  DESCRIPTOR(index_column),
  10
));
```

### 输出

输出表包含输入表的所有列、向量搜索表 (vector search table) 的列，以及一个名为 `score` 的列，用于表示输入行与匹配行之间的相似度。

### 注意事项

1.  向量表 (vector table) 的实现必须实现 `org.apache.flink.table.connector.source.VectorSearchTableSource` 接口。详情请参阅 [Vector Search Table Source]({{< ref "/docs/dev/table/sourcesSinks" >}}#vector-search-table-source)。
2.  `VECTOR_SEARCH` 仅支持读取仅 append-only 表。
3.  当函数调用与其它表没有关联时，`VECTOR_SEARCH` 不需要 `LATERAL` 关键字。例如，如果搜索列是一个常量或字面值 (literal value)，`LATERAL` 可以被省略。

{{< top >}}
