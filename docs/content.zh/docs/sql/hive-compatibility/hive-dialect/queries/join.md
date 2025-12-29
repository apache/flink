---
title: "Join"
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

# Join

## Description

`JOIN` is used to combine rows from two relations based on join condition.

## Syntax

Hive Dialect supports the following syntax for joining tables:
```sql
join_table:
    table_reference [ INNER ] JOIN table_factor [ join_condition ]
  | table_reference { LEFT | RIGHT | FULL } [ OUTER ] JOIN table_reference join_condition
  | table_reference LEFT SEMI JOIN table_reference [ ON expression ] 
  | table_reference CROSS JOIN table_reference [ join_condition ]
 
table_reference:
    table_factor
  | join_table
 
table_factor:
    tbl_name [ alias ]
  | table_subquery alias
  | ( table_references )
 
join_condition:
    { ON expression | USING ( colName [, ...] ) }
```

## JOIN Type

### INNER JOIN

`INNER JOIN` returns the rows matched in both join sides. `INNER JOIN` is the default join type.

### LEFT JOIN

`LEFT JOIN` returns all the rows from the left join side and the matched values from the right join side. It will concat the values from both sides.  
If there's no match in right join side, it will append `NULL` value. `LEFT JOIN` is equivalent to `LEFT OUTER JOIN`.

### RIGHT JOIN

`RIGHT JOIN` returns all the rows from the right join side and the matched values from the left join side. It will concat the values from both sides.  
If there's no match in left join side, it will append `NULL` value. `RIGHT JOIN` is equivalent to `RIGHT OUTER JOIN`.

### FULL JOIN

`FULL JOIN` returns all the rows from both join sides. It will concat the values from both sides.  
If there's one side does not match the row, it will append `NULL` value. `FULL JOIN` is equivalent to `FULL OUTER JOIN`.

### LEFT SEMI JOIN

`LEFT SMEI JOIN` returns the rows from the left join side that have matching in right join side. It won't concat the values from the right side.

### CROSS JOIN

`CROSS JOIN` returns the Cartesian product of two join sides.

## Examples

```sql
-- INNER JOIN
SELECT t1.x FROM t1 INNER JOIN t2 USING (x);
SELECT t1.x FROM t1 INNER JOIN t2 ON t1.x = t2.x;

-- LEFT JOIN
SELECT t1.x FROM t1 LEFT JOIN t2 USING (x);
SELECT t1.x FROM t1 LEFT OUTER JOIN t2 ON t1.x = t2.x;

-- RIGHT JOIN
SELECT t1.x FROM t1 RIGHT JOIN t2 USING (x);
SELECT t1.x FROM t1 RIGHT OUTER JOIN t2 ON t1.x = t2.x;

-- FULL JOIN
SELECT t1.x FROM t1 FULL JOIN t2 USING (x);
SELECT t1.x FROM t1 FULL OUTER JOIN t2 ON t1.x = t2.x;

-- LEFT SEMI JOIN
SELECT t1.x FROM t1 LEFT SEMI JOIN t2 ON t1.x = t2.x;

-- CROSS JOIN
SELECT t1.x FROM t1 CROSS JOIN t2 USING (x);
```
