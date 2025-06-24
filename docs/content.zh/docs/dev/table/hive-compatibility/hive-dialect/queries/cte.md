---
title: "CTE"
weight: 9
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

# Common Table Expression (CTE)

## Description

A Common Table Expression (CTE) is a temporary result set derived from a query specified in a `WITH` clause, which immediately precedes a `SELECT`
or `INSERT` keyword. The CTE is defined only with the execution scope of a single statement, and can be referred in the scope.

## Syntax

```sql
withClause: WITH cteClause [ , ... ]
cteClause: cte_name AS (select statement)
```


{{< hint warning >}}
**Note:**
- The `WITH` clause is not supported within Sub-Query block
- CTEs are supported in Views, `CTAS` and `INSERT` statement
- [Recursive Queries](https://wiki.postgresql.org/wiki/CTEReadme#Parsing_recursive_queries) are not supported
  {{< /hint >}}

## Examples

```sql
WITH q1 AS ( SELECT key FROM src WHERE key = '5')
SELECT *
FROM q1;

-- chaining CTEs
WITH q1 AS ( SELECT key FROM q2 WHERE key = '5'),
q2 AS ( SELECT key FROM src WHERE key = '5')
SELECT * FROM (SELECT key FROM q1) a;

-- insert example
WITH q1 AS ( SELECT key, value FROM src WHERE key = '5')
FROM q1
INSERT OVERWRITE TABLE t1
SELECT *;

-- ctas example
CREATE TABLE t2 AS
WITH q1 AS ( SELECT key FROM src WHERE key = '4')
SELECT * FROM q1;
```
