---
title: "SELECT & WHERE"
weight: 4
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

# SELECT & WHERE clause

{{< label Batch >}} {{< label Streaming >}}

The general syntax of the `SELECT` statement is:

```sql
SELECT select_list FROM table_expression [ WHERE boolean_expression ]
```

The `table_expression` refers to any source of data. It could be an existing table, view, or `VALUES` clause, the joined results of multiple existing tables, or a subquery. Assuming that the table is available in the catalog, the following would read all rows from `Orders`.

```sql
SELECT * FROM Orders
```

The `select_list` specification `*` means the query will resolve all columns. However, usage of `*` is discouraged in production because it makes queries less robust to catalog changes. Instead, a `select_list` can specify a subset of available columns or make calculations using said columns. For example, if `Orders` has columns named `order_id`, `price`, and `tax` you could write the following query:

```sql
SELECT order_id, price + tax FROM Orders
```

Queries can also consume from inline data using the `VALUES` clause. Each tuple corresponds to one row and an alias may be provided to assign names to each column.

```sql
SELECT order_id, price FROM (VALUES (1, 2.0), (2, 3.1))  AS t (order_id, price)
```

Rows can be filtered based on a `WHERE` clause.

```sql
SELECT price + tax FROM Orders WHERE id = 10
```

Additionally, built-in and [user-defined scalar functions]({{< ref "docs/dev/table/functions/udfs" >}}) can be invoked on the columns of a single row. User-defined functions must be registered in a catalog before use.

```sql
SELECT PRETTY_PRINT(order_id) FROM Orders
```

{{< top >}}
