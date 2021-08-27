---
title: "Deduplication"
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

Flink uses `ROW_NUMBER()` to remove duplicates, just like the way of Top-N query. In theory, deduplication is a special case of Top-N in which the N is one and order by the processing time or event time.

The following shows the syntax of the Deduplication statement:

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

{{< hint info >}}
Note: the above pattern must be followed exactly, otherwise the optimizer won’t be able to translate the query.
{{< /hint >}}

The following examples show how to specify SQL queries with Deduplication on streaming tables.

```sql
CREATE TABLE Orders (
  order_time  STRING,
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
