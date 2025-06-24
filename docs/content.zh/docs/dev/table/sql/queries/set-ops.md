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

# 集合操作
{{< label Batch >}} {{< label Streaming >}}

## UNION

`UNION` 和 `UNION ALL` 返回两个表中的数据。
`UNION` 会去重，`UNION ALL` 不会去重。

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

`INTERSECT` 和 `INTERSECT ALL` 返回两个表中共有的数据。
`INTERSECT` 会去重，`INTERSECT ALL` 不会去重。

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

`EXCEPT` 和 `EXCEPT ALL` 返回在一个表中存在，但在另一个表中不存在数据。
`EXCEPT` 会去重，`EXCEPT ALL`不会去重。

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

如果表达式（可以是列，也可以是函数等）存在于子查询的结果中，则返回 true。子查询的表结果必须由一列组成。此列必须与表达式具有相同的数据类型。

```sql
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
```

优化器会把 `IN` 条件重写为 join 和 group 操作。对于流式查询，计算查询结果所需的状态可能会根据输入行数而无限增长。你可以设置一个合适的状态 time-to-live（TTL）来淘汰过期数据以防止状态过大。注意：这可能会影响查询结果的正确性。详情参见：[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)。

## EXISTS

```sql
SELECT user, amount
FROM Orders
WHERE product EXISTS (
    SELECT product FROM NewProducts
)
```

如果子查询返回至少一行，则为 true。只支持能被重写为 join 和 group 的操作。

优化器会把 `EXIST` 重写为 join 和 group 操作.对于流式查询，计算查询结果所需的状态可能会根据输入行数而无限增长。你可以设置一个合适的状态 time-to-live（TTL）来淘汰过期数据以防止状态过大。注意：这可能会影响查询结果的正确性。详情参见：[查询配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)。

{{< top >}}
