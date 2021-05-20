---
title: "Top-N"
weight: 14
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

# Top-N
{{< label Batch >}} {{< label Streaming >}}

Top-N queries ask for the N smallest or largest values ordered by columns. Both smallest and largest values sets are considered Top-N queries. Top-N queries are useful in cases where the need is to display only the N bottom-most or the N top-
most records from batch/streaming table on a condition. This result set can be used for further analysis.

Flink uses the combination of a OVER window clause and a filter condition to express a Top-N query. With the power of OVER window `PARTITION BY` clause, Flink also supports per group Top-N. For example, the top five products per category that have the maximum sales in realtime. Top-N queries are supported for SQL on batch and streaming tables.

The following shows the syntax of the Top-N statement:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
   FROM table_name)
WHERE rownum <= N [AND conditions]
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one, according to the ordering of rows within the partition. Currently, we only support `ROW_NUMBER` as the over window function. In the future, we will support `RANK()` and `DENSE_RANK()`.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns. Each partition will have a Top-N result.
- `ORDER BY col1 [asc|desc][, col2 [asc|desc]...]`: Specifies the ordering columns. The ordering directions can be different on different columns.
- `WHERE rownum <= N`: The `rownum <= N` is required for Flink to recognize this query is a Top-N query. The N represents the N smallest or largest records will be retained.
- `[AND conditions]`: It is free to add other conditions in the where clause, but the other conditions can only be combined with `rownum <= N` using `AND` conjunction.

{{< hint info >}}
Note: the above pattern must be followed exactly, otherwise the optimizer won’t be able to translate the query.
{{< /hint >}}

{{< hint info >}}
The TopN query is <span class="label label-info">Result Updating</span>. Flink SQL will sort the input data stream according to the order key, so if the top N records have been changed, the changed ones will be sent as retraction/update records to downstream.
It is recommended to use a storage which supports updating as the sink of Top-N query. In addition, if the top N records need to be stored in external storage, the result table should have the same unique key with the Top-N query.
{{< /hint >}}

The unique keys of Top-N query is the combination of partition columns and rownum column. Top-N query can also derive the unique key of upstream. Take following job as an example, say `product_id` is the unique key of the `ShopSales`, then the unique keys of the Top-N query are [`category`, `rownum`] and [`product_id`].

The following examples show how to specify SQL queries with Top-N on streaming tables. This is an example to get "the top five products per category that have the maximum sales in realtime" we mentioned above.

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```

#### No Ranking Output Optimization

As described above, the `rownum` field will be written into the result table as one field of the unique key, which may lead to a lot of records being written to the result table. For example, when the record (say `product-1001`) of ranking 9 is updated and its rank is upgraded to 1, all the records from ranking 1 ~ 9 will be output to the result table as update messages. If the result table receives too many data, it will become the bottleneck of the SQL job.

The optimization way is omitting rownum field in the outer SELECT clause of the Top-N query. This is reasonable because the number of the top N records is usually not large, thus the consumers can sort the records themselves quickly. Without rownum field, in the example above, only the changed record (`product-1001`) needs to be sent to downstream, which can reduce much IO to the result table.

The following example shows how to optimize the above Top-N example in this way:

```sql
CREATE TABLE ShopSales (
  product_id   STRING,
  category     STRING,
  product_name STRING,
  sales        BIGINT
) WITH (...);

-- omit row_num field from the output
SELECT product_id, category, product_name, sales
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
  FROM ShopSales)
WHERE row_num <= 5
```

<span class="label label-danger">Attention in Streaming Mode</span> In order to output the above query to an external storage and have a correct result, the external storage must have the same unique key with the Top-N query. In the above example query, if the `product_id` is the unique key of the query, then the external table should also has `product_id` as the unique key.


{{< top >}}
