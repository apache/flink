---
title: "集合操作"
weight: 11
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

# Set Operations
{{< label Batch >}} {{< label Streaming >}}

## UNION

`UNION` and `UNION ALL` return the rows that are found in either table.
`UNION` takes only distinct rows while `UNION ALL` does not remove duplicates from the result rows.

```sql
Flink SQL> create view t1(s) as values ('c'), ('a'), ('b'), ('b'), ('c');
Flink SQL> create view t2(s) as values ('d'), ('e'), ('a'), ('b'), ('b');

Flink SQL> (SELECT s FROM t1) UNION (SELECT s FROM t2);
+---+
|  s|
+---+
|  c|
|  a|
|  b|
|  d|
|  e|
+---+

Flink SQL> (SELECT s FROM t1) UNION ALL (SELECT s FROM t2);
+---+
|  c|
+---+
|  c|
|  a|
|  b|
|  b|
|  c|
|  d|
|  e|
|  a|
|  b|
|  b|
+---+
```

## INTERSECT

`INTERSECT` and `INTERSECT ALL` return the rows that are found in both tables.
`INTERSECT` takes only distinct rows while `INTERSECT ALL` does not remove duplicates from the result rows.

```sql
Flink SQL> (SELECT s FROM t1) INTERSECT (SELECT s FROM t2);
+---+
|  s|
+---+
|  a|
|  b|
+---+

Flink SQL> (SELECT s FROM t1) INTERSECT ALL (SELECT s FROM t2);
+---+
|  s|
+---+
|  a|
|  b|
|  b|
+---+
```

## EXCEPT

`EXCEPT` and `EXCEPT ALL` return the rows that are found in one table but not the other.
`EXCEPT` takes only distinct rows while `EXCEPT ALL` does not remove duplicates from the result rows.

```sql
Flink SQL> (SELECT s FROM t1) EXCEPT (SELECT s FROM t2);
+---+
| s |
+---+
| c |
+---+

Flink SQL> (SELECT s FROM t1) EXCEPT ALL (SELECT s FROM t2);
+---+
| s |
+---+
| c |
| c |
+---+
```

## IN

Returns true if an expression exists in a given table sub-query. The sub-query table must
consist of one column. This column must have the same data type as the expression.

```sql
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
```

The optimizer rewrites the IN condition into a join and group operation. For streaming queries, the required state for computing the query result might grow infinitely depending on the number of distinct input rows. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

## EXISTS

```sql
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
```

Returns true if the sub-query returns at least one row. Only supported if the operation can be rewritten in a join and group operation.

The optimizer rewrites the `EXISTS` operation into a join and group operation. For streaming queries, the required state for computing the query result might grow infinitely depending on the number of distinct input rows. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

{{< top >}}
