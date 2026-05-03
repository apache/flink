---
title: "Sub-Queries"
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

# Sub-Queries

## Sub-Queries in the FROM Clause

### Description

Hive dialect supports sub-queries in the `FROM` clause. The sub-query has to be given a name because every table in a `FROM` clause must have a name.
Columns in the sub-query select list must have unique names.
The columns in the sub-query select list are available in the outer query just like columns of a table.
The sub-query can also be a query expression with `UNION`. Hive dialect supports arbitrary levels of sub-queries.

### Syntax

```sql
select_statement FROM ( select_statement ) [ AS ] name
```

### Example

```sql
SELECT col
FROM (
  SELECT a+b AS col
  FROM t1
) t2
```

## Sub-Queries in the WHERE Clause

### Description

Hive dialect also supports some types of sub-queries in the `WHERE` clause.

### Syntax

```sql
select_statement FROM table WHERE { colName { IN | NOT IN } 
                                  | NOT EXISTS | EXISTS } ( subquery_select_statement )
```

### Examples

```sql
SELECT * FROM t1 WHERE t1.x IN (SELECT y FROM t2);
 
SELECT * FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE t1.x = t2.x);
```
