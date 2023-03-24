---
title: "Window Functions"
weight: 7
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

# Window Functions

## Description

Window functions are a kind of aggregation for a group of rows, referred as a window.
It will return the aggregation value for each row based on the group of rows.

## Syntax

```sql
window_function OVER ( [ { PARTITION | DISTRIBUTE }  BY colName ( [, ... ] ) ] 
{ ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
[ window_frame ] )
```

## Parameters

### window_function

Hive dialect supports the following window functions:
- Windowing functions
    - LEAD
    - LAG
    - FIRST_VALUE
    - LAST_VALUE

  {{< hint warning >}}
  **Note:** For FIRST_VALUE/LAST_VALUE, use parameter to control skip null values or respect null values isn't supported yet. And they will always skip null values
  {{< /hint >}}
- Analytic functions
    - RANK
    - ROW_NUMBER
    - DENSE_RANK
    - CUME_DIST
    - PERCENT_RANK
    - NTILE
- Aggregate Functions
    - COUNT
    - SUM
    - MIN
    - MAX
    - AVG

### window_frame

It's used to specified which row to start on and where to end it. Window frame supports the following formats:
```sql
(ROWS | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECEDING | CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN CURRENT ROW AND (CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN [num] FOLLOWING AND (UNBOUNDED | [num]) FOLLOWING
```

When `ORDER BY` is specified, but missing `window_frame`, the window frame defaults to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

When both `ORDER BY` and `window_frame` are missing, the window frame defaults to `ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

{{< hint warning >}}
**Note:**
Distinct is not supported in window function yet.
{{< /hint >}}

## Examples

```sql
-- PARTITION BY with one partitioning column, no ORDER BY or window specification
SELECT a, COUNT(b) OVER (PARTITION BY c) FROM t;

-- PARTITION BY with two partitioning columns, no ORDER BY or window specification
SELECT a, COUNT(b) OVER (PARTITION BY c, d) FROM t;

-- PARTITION BY with two partitioning columns, no ORDER BY or window specification
SELECT a, SUM(b) OVER (PARTITION BY c, d ORDER BY e, f) FROM t;

-- PARTITION BY with partitioning, ORDER BY, and window specification
SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM t;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
FROM t;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
FROM t;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM t;
```
