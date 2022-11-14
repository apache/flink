---
title: "Overview"
weight: 1
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

# Queries

## Description

Hive dialect supports a commonly-used subset of Hive’s [DQL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select).
The following lists some parts of HiveQL supported by the Hive dialect.

- [Sort/Cluster/Distributed BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by" >}})
- [Group By]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/group-by" >}})
- [Join]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/join" >}})
- [Set Operation]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/set-op" >}})
- [Lateral View]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/lateral-view" >}})
- [Window Functions]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/window-functions" >}})
- [Sub-Queries]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sub-queries" >}})
- [CTE]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/cte" >}})
- [Transform]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/transform" >}})
- [Table Sample]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/table-sample" >}})

## Syntax

The following section describes the overall query syntax.
The SELECT clause can be part of a query which also includes [common table expressions (CTE)]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/cte" >}}), set operations, and various other clauses.

```sql
[WITH CommonTableExpression [ , ... ]]
SELECT [ALL | DISTINCT] select_expr [ , ... ]
  FROM table_reference
  [WHERE where_condition]
  [GROUP BY col_list]
  [ORDER BY col_list]
  [CLUSTER BY col_list
    | [DISTRIBUTE BY col_list] [SORT BY col_list]
  ]
 [LIMIT [offset,] rows]
```
- The `SELECT` statement can be part of a [set]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/set-op" >}}) query or a [sub-query]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sub-queries" >}}) of another query
- `CommonTableExpression` is a temporary result set derived from a query specified in a `WITH` clause
- `table_reference` indicates the input to the query. It can be a regular table, a view, a [join]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/join" >}}) or a [sub-query]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sub-queries" >}}).
- Table names and column names are case-insensitive

### WHERE Clause

The `WHERE` condition is a boolean expression. Hive dialect supports a number of [operators and UDFs](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF)
in the `WHERE` clause. Some types of [sub queries]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sub-queries" >}}) are supported in `WHERE` clause.

### GROUP BY Clause

Please refer to [GROUP BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/group-by" >}}) for more details.

### ORDER BY Clause

The `ORDER BY` clause is used to return the result rows in a sorted manner in the user specified order.
Different from [SORT BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by" >}}#sort-by), `ORDER BY` clause guarantees
a global order in the output.

{{< hint warning >}}
**Note:**
To guarantee global order, there has to be single one task to sort the final output.
So if the number of rows in the output is too large, it could take a very long time to finish.
{{< /hint >}}

## CLUSTER/DISTRIBUTE/SORT BY

Please refer to [Sort/Cluster/Distributed BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/sort-cluster-distribute-by" >}}) for more details.

### ALL and DISTINCT Clauses

The `ALL` and `DISTINCT` options specify whether duplicate rows should be returned or not.
If none of these two options are given, the default is `ALL` (all matching rows are returned).
`DISTINCT` specifies removal of duplicate rows from the result set.

### LIMIT Clause

The `LIMIT` clause can be used to constrain the number of rows returned by the `SELECT` statement.

`LIMIT` takes one or two numeric arguments, which must both be non-negative integer constants.
The first argument specifies the offset of the first row to return and the second specifies the maximum number of rows to return.
When a single argument is given, it stands for the maximum number of rows and the offset defaults to 0.

## Examples

Following is an example of using hive dialect to run some queries.

{{< hint warning >}}
**Note:** Hive dialect no longer supports [Flink SQL queries]({{< ref "docs/dev/table/sql/queries/overview" >}}). Please switch to default dialect if you’d like to write in Flink syntax.
{{< /hint >}}

```bash
Flink SQL> create catalog myhive with ('type' = 'hive', 'hive-conf-dir' = '/opt/hive-conf');
[INFO] Execute statement succeed.

Flink SQL> use catalog myhive;
[INFO] Execute statement succeed.

Flink SQL> load module hive;
[INFO] Execute statement succeed.

Flink SQL> use modules hive,core;
[INFO] Execute statement succeed.

Flink SQL> set table.sql-dialect=hive;
[INFO] Session property has been set.

FLINK SQL> set sql-client.execution.result-mode=tableau;

Flink SQL> select explode(array(1,2,3)); -- call hive udtf
+----+-------------+
| op |         col |
+----+-------------+
| +I |           1 |
| +I |           2 |
| +I |           3 |
+----+-------------+
Received a total of 3 rows

Flink SQL> create table tbl (key int,value string);
[INFO] Execute statement succeed.

Flink SQL> insert into table tbl values (5,'e'),(1,'a'),(1,'a'),(3,'c'),(2,'b'),(3,'c'),(3,'c'),(4,'d');
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:

FLINK SQL> set execution.runtime-mode=batch; -- change to batch mode

Flink SQL> select * from tbl cluster by key; -- run cluster by
2021-04-22 16:13:57,005 INFO  org.apache.hadoop.mapred.FileInputFormat                     [] - Total input paths to process : 1
+-----+-------+
| key | value |
+-----+-------+
|   1 |     a |
|   1 |     a |
|   5 |     e |
|   2 |     b |
|   3 |     c |
|   3 |     c |
|   3 |     c |
|   4 |     d |
+-----+-------+
Received a total of 8 rows
```
