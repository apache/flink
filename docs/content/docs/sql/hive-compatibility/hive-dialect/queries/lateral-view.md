---
title: "Lateral View Clause"
weight: 6
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

# Lateral View Clause

## Description

Lateral view clause is used in conjunction with user-defined table generating functions([UDTF](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inTable-GeneratingFunctions(UDTF))) such as `explode()`.
A UDTF generates zero or more output rows for each input row.

A lateral view first applies the UDTF to each row of base and then joins results output rows to the input rows to form a virtual table having the supplied table alias.

## Syntax

```sql
lateralView: LATERAL VIEW [ OUTER ] udtf( expression ) tableAlias AS columnAlias [, ... ]
fromClause: FROM baseTable lateralView [, ... ]
```
The column alias can be omitted. In this case, aliases are inherited from fields name of StructObjectInspector which is returned from UDTF.

## Parameters

- Lateral View Outer

  User can specify the optional `OUTER` keyword to generate rows even when a `LATERAL VIEW` usually would not generate a row.
  This happens when the UDTF used does not generate any rows which happens easily with when the column to explode is empty.
  In this case, the source row would never appear in the results. `OUTER` can be used to prevent that and rows will be generated with `NULL`
  values in the columns coming from UDTF.
- Multiple Lateral Views

  A FROM clause can have multiple LATERAL VIEW clauses.
  Subsequent LATERAL VIEWS can reference columns from any of the tables appearing to the left of the LATERAL VIEW.


## Examples

Assuming you have one table:
```sql
CREATE TABLE pageAds(pageid string, addid_list array<int>);
```
And the table contains two rows:
```sql
front_page, [1, 2, 3];
contact_page, [3, 4, 5];
```
Now, you can use `LATERAL VIEW` to convert the column `addid_list` into separate rows:
```sql
SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid;
-- result
front_page, 1
front_page, 2
front_page, 3
contact_page, 3
contact_page, 4
contact_page, 5
```
Also, if you have one table:
```sql
CREATE TABLE t1(c1 array<int>, c2 array<int>);
```
You can use multiple lateral view clauses to convert the column `c1` and `c2` into separate rows:
```sql
SELECT myc1, myc2 FROM t1
LATERAL VIEW explode(c1) myTable1 AS myc1
LATERAL VIEW explode(c2) myTable2 AS myc2;
```
When the UDTF doesn't produce rows, then `LATERAL VIEW` won't produce rows.
You can use `LATERAL VIEW OUTER` to still produce rows, with `NULL` filling the corresponding column.
```sql
SELECT * FROM t1 LATERAL VIEW OUTER explode(array()) C AS a;
```
