---
title: "Set Operations"
weight: 5
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

Set Operations are used to combine multiple `SELECT` statements into a single result set.
Hive dialect supports the following operations:
- UNION
- INTERSECT
- EXCEPT/MINUS

## UNION

### Description

`UNION`/`UNION DISTINCT`/`UNION ALL` returns the rows that are found in either side.

`UNION` and `UNION DISTINCT` only returns the distinct rows, while `UNION ALL` does not duplicate.

### Syntax

```sql
<query> { UNION [ ALL | DISTINCT ] } <query> [ .. ]
```

### Examples
```sql
SELECT x, y FROM t1 UNION DISTINCT SELECT x, y FROM t2;
SELECT x, y FROM t1 UNION SELECT x, y FROM t2;
SELECT x, y FROM t1 UNION ALL SELECT x, y FROM t2;
```

## INTERSECT

### Description

`INTERSECT`/`INTERSECT DISTINCT`/`INTERSECT ALL` returns the rows that are found in both side.

`INTERSECT`/`INTERSECT DISTINCT` only returns the distinct rows, while `INTERSECT ALL` does not duplicate.

### Syntax

```sql
<query> { INTERSECT [ ALL | DISTINCT ] } <query> [ .. ]
```

### Examples
```sql
SELECT x, y FROM t1 INTERSECT DISTINCT SELECT x, y FROM t2;
SELECT x, y FROM t1 INTERSECT SELECT x, y FROM t2;
SELECT x, y FROM t1 INTERSECT ALL SELECT x, y FROM t2;
```

## EXCEPT/MINUS

### Description

`EXCEPT`/`EXCEPT DISTINCT`/`EXCEPT ALL` returns the rows that are found in left side but not in right side.

`EXCEPT`/`EXCEPT DISTINCT` only returns the distinct rows, while `EXCEPT ALL` does not duplicate.

`MINUS` is synonym for `EXCEPT`.

### Syntax

```sql
<query> { EXCEPT [ ALL | DISTINCT ] } <query> [ .. ]
```

### Examples

```sql
SELECT x, y FROM t1 EXCEPT DISTINCT SELECT x, y FROM t2;
SELECT x, y FROM t1 EXCEPT SELECT x, y FROM t2;
SELECT x, y FROM t1 EXCEPT ALL SELECT x, y FROM t2;
```
