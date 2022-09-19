---
title: "Group By"
weight: 3
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

# Group By Clause

## Description

The `Group by` clause is used to compute a single result from multiple input rows with given aggregation function.
Hive dialect also supports enhanced aggregation features to do multiple aggregations based on the same record by using
`ROLLUP`/`CUBE`/`GROUPING SETS`.

## Syntax

```sql
group_by_clause: 
  group_by_clause_1 | group_by_clause_2

group_by_clause_1: 
  GROUP BY group_expression [ , ... ] [ WITH ROLLUP | WITH CUBE ] 
 
group_by_clause_2: 
  GROUP BY { group_expression | { ROLLUP | CUBE | GROUPING SETS } ( grouping_set [ , ... ] ) } [ , ... ]

grouping_set: 
  { expression | ( [ expression [ , ... ] ] ) }
 
groupByQuery: SELECT expression [ , ... ] FROM src groupByClause?
```
In `group_expression`, columns can be also specified by position number. But please remember:
- For Hive 0.11.0 through 2.1.x, set `hive.groupby.orderby.position.alias` to true (the default is false)
- For Hive 2.2.0 and later, set `hive.groupby.position.alias` to true (the default is false)

## Parameters

### GROUPING SETS

`GROUPING SETS` allow for more complex grouping operations than those describable by a standard `GROUP BY`.
Rows are grouped separately by each specified grouping set and aggregates are computed for each group just as for simple `GROUP BY` clauses.

All `GROUPING SET` clauses can be logically expressed in terms of several `GROUP BY` queries connected by `UNION`.

For example:
```sql
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS ( (a, b), a, b, ( ) )
```
is equivalent to
```sql
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
UNION
SELECT a, null, SUM( c ) FROM tab1 GROUP BY a, null
UNION
SELECT null, b, SUM( c ) FROM tab1 GROUP BY null, b
UNION
SELECT null, null, SUM( c ) FROM tab1
```
When aggregates are displayed for a column its value is null. This may conflict in case the column itself has some null values.
There needs to be some way to identify NULL in column, which means aggregate and NULL in column, which means `GROUPING__ID` function is the solution to that.

This function returns a bitvector corresponding to whether each column is present or not.
For each column, a value of "1" is produced for a row in the result set if that column has been aggregated in that row, otherwise the value is "0".
This can be used to differentiate when there are nulls in the data.
For more details, please refer to Hive's docs [Grouping__ID function](https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup#EnhancedAggregation,Cube,GroupingandRollup-Grouping__IDfunction).

Also, there's [Grouping function](https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup#EnhancedAggregation,Cube,GroupingandRollup-Groupingfunction) indicates whether an expression in a `GROUP BY` clause is aggregated or not for a given row.
The value 0 represents a column that is part of the grouping set, while the value 1 represents a column that is not part of the grouping set.

### ROLLUP

`ROLLUP` is a shorthand notation for specifying a common type of grouping set.
It represents the given list of expressions and all prefixes of the list, including the empty list.
For example:
```sql
GROUP BY a, b, c WITH ROLLUP
```
is equivalent to
```sql
GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( )).
```

### CUBE

`CUBE` is a shorthand notation for specifying a common type of grouping set.
It represents the given list and all of its possible subsets - the power set.

For example:
```sql
GROUP BY a, b, c WITH CUBE
```
is equivalent to
```sql
GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ))
```

## Examples

```sql
-- use group by expression
SELECT abs(x), sum(y) FROM t GROUP BY abs(x);

-- use group by column
SELECT x, sum(y) FROM t GROUP BY x;

-- use group by position
SELECT x, sum(y) FROM t GROUP BY 1; -- group by first column in the table;

-- use grouping sets
SELECT x, SUM(y) FROM t GROUP BY x GROUPING SETS ( x, ( ) );

-- use rollup
SELECT x, SUM(y) FROM t GROUP BY x WITH ROLLUP;
SELECT x, SUM(y) FROM t GROUP BY ROLLUP (x);

-- use cube
SELECT x, SUM(y) FROM t GROUP BY x WITH CUBE;
SELECT x, SUM(y) FROM t GROUP BY CUBE (x);
```
