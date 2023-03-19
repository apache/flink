---
title: "去重"
weight: 16
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

# 去重
{{< label Batch >}} {{< label Streaming >}}

去重是去掉重复的行，只保留第一或者最后一行。有时，上游的 ETL 生成的数据不是端到端精确一次的。在发生故障恢复后，可能会导致结果下游中出现重复记录。这些重复记录会影响下游的分析工作，比如：`SUM`、`COUNT`的结果会偏大，所以需要先去重再进行下一步的分析。

Flink 使用 `ROW_NUMBER()` 去除重复数据，就像 Top-N 查询一样。其实，去重就是 Top-N 在 N 为 1 时的特例，并且去重必须要求按照[处理或者事件时间]({{< ref "docs/dev/table/concepts/time_attributes" >}})排序。

下面的例子展示了去重语句的语法：

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1
```

**参数说明：**

- `ROW_NUMBER()`：为每一行分配一个唯一且连续的数字，从 1 开始。
- `PARTITION BY col1[, col2...]`：指定分区键，即需要去重的键。
- `ORDER BY time_attr [asc|desc]`：指定排序列，必须是 [时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})。目前 Flink 支持 [处理时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) 和 [事件时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time)。Order by ASC 保留第一行，DESC 保留最后一行。
- `WHERE rownum = 1`：Flink 需要这个条件来识别去重语句。

{{< hint info >}}
注意：上述格式必须严格遵守，否则优化器无法识别。
{{< /hint >}}

下面的例子展示了在流表上如何指定去重 SQL 查询：

```sql
CREATE TABLE Orders (
  order_id  STRING,
  user        STRING,
  product     STRING,
  num         BIGINT,
  proctime AS PROCTIME()
) WITH (...);

-- remove duplicate rows on order_id and keep the first occurrence row,
-- because there shouldn't be two orders with the same order_id.
SELECT order_id, user, product, num
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num
  FROM Orders)
WHERE row_num = 1
```

{{< top >}}
