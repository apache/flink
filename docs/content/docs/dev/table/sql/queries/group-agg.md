---
title: "Group Aggregation"
weight: 8
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

# Group Aggregation
{{< label Batch >}} {{< label Streaming >}}

Like most data systems, Apache Flink supports aggregate functions; both built-in and user-defined. [User-defined functions]({{< ref "docs/dev/table/functions/udfs" >}}) must be registered in a catalog before use.

An aggregate function computes a single result from multiple input rows. For example, there are aggregates to compute the `COUNT`, `SUM`, `AVG` (average), `MAX` (maximum) and `MIN` (minimum) over a set of rows.

```sql
SELECT COUNT(*) FROM Orders
```

For streaming queries, it is important to understand that Flink runs continuous queries that never terminate. Instead, they update their result table according to the updates on its input tables. For the above query, Flink will output an updated count each time a new row is inserted into the `Orders` table.

Apache Flink supports the standard `GROUP BY` clause for aggregating data.

```sql
SELECT COUNT(*)
FROM Orders
GROUP BY order_id
```

For streaming queries, the required state for computing the query result might grow infinitely. State size depends on the number of groups and the number and type of aggregation functions. For example `MIN`/`MAX` are heavy on state size while `COUNT` is cheap. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

Apache Flink provides a set of performance tuning ways for Group Aggregation, see more [Performance Tuning]({{< ref "docs/dev/table/tuning" >}}).

## DISTINCT Aggregation

Distinct aggregates remove duplicate values before applying an aggregation function. The following example counts the number of distinct order_ids instead of the total number of rows in the Orders table.

```sql
SELECT COUNT(DISTINCT order_id) FROM Orders
```

For streaming queries, the required state for computing the query result might grow infinitely. State size is mostly depends on the number of distinct rows and the time that a group is maintained, short lived group by windows are not a problem. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

## GROUPING SETS

Grouping sets allow for more complex grouping operations than those describable by a standard `GROUP BY`. Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple `GROUP BY` clauses.

```sql
SELECT supplier_id, rating, COUNT(*) AS total
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())
```

Results:

```
+-------------+--------+-------+
| supplier_id | rating | total |
+-------------+--------+-------+
|   supplier1 |      4 |     1 |
|   supplier1 | (NULL) |     2 |
|      (NULL) | (NULL) |     4 |
|   supplier1 |      3 |     1 |
|   supplier2 |      3 |     1 |
|   supplier2 | (NULL) |     2 |
|   supplier2 |      4 |     1 |
+-------------+--------+-------+
```

Each sublist of `GROUPING SETS` may specify zero or more columns or expressions and is interpreted the same way as though it was used directly in the `GROUP BY` clause. An empty grouping set means that all rows are aggregated down to a single group, which is output even if no input rows were present.

References to the grouping columns or expressions are replaced by null values in result rows for grouping sets in which those columns do not appear.

For streaming queries, the required state for computing the query result might grow infinitely. State size depends on number of group sets and type of aggregation functions. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

### ROLLUP

`ROLLUP` is a shorthand notation for specifying a common type of grouping set. It represents the given list of expressions and all prefixes of the list, including the empty list.

For example, the following query is equivalent to the one above.

```sql
SELECT supplier_id, rating, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY ROLLUP (supplier_id, rating)
```

### CUBE

`CUBE` is a shorthand notation for specifying a common type of grouping set. It represents the given list and all of its possible subsets - the power set.

For example, the following two queries are equivalent.

```sql
SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY CUBE (supplier_id, rating, product_id)

SELECT supplier_id, rating, product_id, COUNT(*)
FROM (VALUES
    ('supplier1', 'product1', 4),
    ('supplier1', 'product2', 3),
    ('supplier2', 'product3', 3),
    ('supplier2', 'product4', 4))
AS Products(supplier_id, product_id, rating)
GROUP BY GROUPING SET (
    ( supplier_id, product_id, rating ),
    ( supplier_id, product_id         ),
    ( supplier_id,             rating ),
    ( supplier_id                     ),
    (              product_id, rating ),
    (              product_id         ),
    (                          rating ),
    (                                 )
)
```

## HAVING

`HAVING` eliminates group rows that do not satisfy the condition. `HAVING` is different from `WHERE`: `WHERE` filters individual rows before the `GROUP BY` while `HAVING` filters group rows created by `GROUP BY`. Each column referenced in condition must unambiguously reference a grouping column unless it appears within an aggregate function.

```sql
SELECT SUM(amount)
FROM Orders
GROUP BY users
HAVING SUM(amount) > 50
```

The presence of `HAVING` turns a query into a grouped query even if there is no `GROUP BY` clause. It is the same as what happens when the query contains aggregate functions but no `GROUP BY` clause. The query considers all selected rows to form a single group, and the `SELECT` list and `HAVING` clause can only reference table columns from within aggregate functions. Such a query will emit a single row if the `HAVING` condition is true, zero rows if it is not true.

{{< top >}}
