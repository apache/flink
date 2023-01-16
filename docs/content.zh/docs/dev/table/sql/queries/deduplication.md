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

# Deduplication
{{< label Batch >}} {{< label Streaming >}}

Deduplication removes rows that duplicate over a set of columns, keeping only the first one or the last one. In some cases, the upstream ETL jobs are not end-to-end exactly-once; this may result in duplicate records in the sink in case of failover. However, the duplicate records will affect the correctness of downstream analytical jobs - e.g. `SUM`, `COUNT` - so deduplication is needed before further analysis.

去重是去掉重复的行,只保留第一个或者最后一个.有时候,上游的ETL生成的数据不是端到端精确一次的(exactly-once); 在发生故障转移(failover)后,可能会导致Sink出现重复记录.这些重复记录会影响下游的分析工作,比如: `SUM`, `COUNT`,所以需要先去重再进行下一步的分析.

Flink uses `ROW_NUMBER()` to remove duplicates, just like the way of Top-N query. In theory, deduplication is a special case of Top-N in which the N is one and order by the processing time or event time.

Flink 使用`ROW_NUMBER()`移除重复数据,就像Top-N查询一样.理论上,去重是Top-N的特殊情况:N是1,并且是通过处理或者事件时间排序的.

The following shows the syntax of the Deduplication statement:

下面的例子展示了去重语句的语法:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr [asc|desc]) AS rownum
   FROM table_name)
WHERE rownum = 1
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns, i.e. the deduplicate key.
- `ORDER BY time_attr [asc|desc]`: Specifies the ordering column, it must be a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}). Currently Flink supports [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) and [event time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time). Ordering by ASC means keeping the first row, ordering by DESC means keeping the last row.
- `WHERE rownum = 1`: The `rownum = 1` is required for Flink to recognize this query is deduplication.

**参数说明:**

- `ROW_NUMBER()`: 为每一行分配一个唯一且连续的数字,从1开始.
- `PARTITION BY col1[, col2...]`: 指定分区列,即需要去重的列.
- `ORDER BY time_attr [asc|desc]`:指定排序列,必须是[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}). 目前Flink支持[处理时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) 和 [事件时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time).order by ASC保留第一行,DESC保留最后一行.
- `WHERE rownum = 1`: Flink需要这个条件来识别去重语句.

{{< hint info >}}
Note: the above pattern must be followed exactly, otherwise the optimizer won’t be able to translate the query.

注意: 上面的必须严格遵守,否则优化器不能识别.
{{< /hint >}}

The following examples show how to specify SQL queries with Deduplication on streaming tables.

下面的例子展示了在流表上怎样指定去重SQL查询:

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
-- 同一个order_id只保留第一次出现的行, 
-- 因为订单号不能重复.
SELECT order_id, user, product, num
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num
  FROM Orders)
WHERE row_num = 1
```

{{< top >}}
