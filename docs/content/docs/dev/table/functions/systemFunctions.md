---
title: "System (Built-in) Functions"
weight: 32
type: docs
aliases:
  - /dev/table/functions/systemFunctions.html
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

# System (Built-in) Functions

Flink Table API & SQL provides users with a set of built-in functions for data transformations. This page gives a brief overview of them.
If a function that you need is not supported yet, you can implement a [user-defined function]({{< ref "docs/dev/table/functions/udfs" >}}).
If you think that the function is general enough, please <a href="https://issues.apache.org/jira/secure/CreateIssue!default.jspa">open a Jira issue</a> for it with a detailed description.

Scalar Functions
----------------

The scalar functions take zero, one or more values as the input and return a single value as the result.

### Comparison Functions

{{< sql_functions "comparison" >}}

### Logical Functions

{{< sql_functions "logical" >}}

### Arithmetic Functions

{{< sql_functions "arithmetic" >}}

### String Functions

{{< sql_functions "string" >}}

### Temporal Functions

{{< sql_functions "temporal" >}}

### Conditional Functions

{{< sql_functions "conditional" >}}

### Type Conversion Functions

{{< sql_functions "conversion" >}}

### Collection Functions

{{< sql_functions "collection" >}}

### JSON Functions

JSON functions make use of JSON path expressions as described in ISO/IEC TR 19075-6 of the SQL
standard. Their syntax is inspired by and adopts many features of ECMAScript, but is neither a
subset nor superset thereof.

Path expressions come in two flavors, lax and strict. When omitted, it defaults to the strict mode.
Strict mode is intended to examine data from a schema perspective and will throw errors whenever
data does not adhere to the path expression. However, functions like `JSON_VALUE` allow defining
fallback behavior if an error is encountered. Lax mode, on the other hand, is more forgiving and
converts errors to empty sequences.

The special character `$` denotes the root node in a JSON path. Paths can access properties (`$.a`),
array elements (`$.a[0].b`), or branch over all elements in an array (`$.a[*].b`).

Known Limitations:
* Not all features of Lax mode are currently supported correctly. This is an upstream bug
  (CALCITE-4717). Non-standard behavior is not guaranteed.

{{< sql_functions "json" >}}

### Value Construction Functions

{{< sql_functions "valueconstruction" >}}

### Value Access Functions

{{< sql_functions "valueaccess" >}}

### Grouping Functions

{{< sql_functions "grouping" >}}

### Hash Functions

{{< sql_functions "hashfunctions" >}}

### Auxiliary Functions

{{< sql_functions "auxiliary" >}}

Aggregate Functions
-------------------

The aggregate functions take an expression across all the rows as the input and return a single aggregated value as the result. 

{{< sql_functions "aggregate" >}}

Time Interval and Point Unit Specifiers
---------------------------------------

The following table lists specifiers for time interval and time point units. 

For Table API, please use `_` for spaces (e.g., `DAY_TO_HOUR`).

| Time Interval Unit       | Time Point Unit                |
|:-------------------------|:-------------------------------|
| `MILLENNIUM`             |                                |
| `CENTURY`                |                                |
| `DECADE`                 |                                |
| `YEAR`                   | `YEAR`                         |
| `YEAR TO MONTH`          |                                |
| `QUARTER`                | `QUARTER`                      |
| `MONTH`                  | `MONTH`                        |
| `WEEK`                   | `WEEK`                         |
| `DAY`                    | `DAY`                          |
| `DAY TO HOUR`            |                                |
| `DAY TO MINUTE`          |                                |
| `DAY TO SECOND`          |                                |
| `HOUR`                   | `HOUR`                         |
| `HOUR TO MINUTE`         |                                |
| `HOUR TO SECOND`         |                                |
| `MINUTE`                 | `MINUTE`                       |
| `MINUTE TO SECOND`       |                                |
| `SECOND`                 | `SECOND`                       |
| `MILLISECOND`            | `MILLISECOND`                  |
| `MICROSECOND`            | `MICROSECOND`                  |
| `NANOSECOND`             |                                |
| `EPOCH`                  |                                |
| `DOY` _(SQL-only)_       |                                |
| `DOW` _(SQL-only)_       |                                |
| `EPOCH` _(SQL-only)_     |                                |
| `ISODOW` _(SQL-only)_    |                                |
| `ISOYEAR` _(SQL-only)_   |                                |
|                          | `SQL_TSI_YEAR` _(SQL-only)_    |
|                          | `SQL_TSI_QUARTER` _(SQL-only)_ |
|                          | `SQL_TSI_MONTH` _(SQL-only)_   |
|                          | `SQL_TSI_WEEK` _(SQL-only)_    |
|                          | `SQL_TSI_DAY` _(SQL-only)_     |
|                          | `SQL_TSI_HOUR` _(SQL-only)_    |
|                          | `SQL_TSI_MINUTE` _(SQL-only)_  |
|                          | `SQL_TSI_SECOND ` _(SQL-only)_ |

{{< top >}}

Column Functions
---------------------------------------

The column functions are used to select or deselect table columns.

{{< hint info >}}
Column functions are only used in Table API.
{{< /hint >}}

| SYNTAX              | DESC                         |
| :--------------------- | :-------------------------- |
| withColumns(...)         | select the specified columns                  |
| withoutColumns(...)        | deselect the columns specified                  |
| withAllColumns()    | select all columns (like `SELECT *` in SQL) |

The detailed syntax is as follows:

```text
columnFunction:
    withColumns(columnExprs)
    withoutColumns(columnExprs)
    withAllColumns()

columnExprs:
    columnExpr [, columnExpr]*

columnExpr:
    columnRef | columnIndex to columnIndex | columnName to columnName

columnRef:
    columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)
```

The usage of the column function is illustrated in the following table. (Suppose we have a table with 5 columns: `(a: Int, b: Long, c: String, d:String, e: String)`):

| API | Usage | Description |
|-|-|-|
| withColumns($(*)) | select(withColumns($("*")))  = select($("a"), $("b"), $("c"), $("d"), $("e")) | all the columns |
| withColumns(m to n) | select(withColumns(range(2, 4))) = select($("b"), $("c"), $("d")) | columns from m to n |
| withColumns(m, n, k)  | select(withColumns(lit(1), lit(3), $("e"))) = select($("a"), $("c"), $("e")) |  columns m, n, k |
| withColumns(m, n to k)  | select(withColumns(lit(1), range(3, 5))) = select($("a"), $("c"), $("d"), $("e")) |  mixing of the above two representation |
| withoutColumns(m to n) | select(withoutColumns(range(2, 4))) = select($("a"), $("e")) |  deselect columns from m to n |
| withoutColumns(m, n, k) | select(withoutColumns(lit(1), lit(3), lit(5))) = select($("b"), $("d")) |  deselect columns m, n, k |
| withoutColumns(m, n to k) | select(withoutColumns(lit(1), range(3, 5))) = select($("b")) |  mixing of the above two representation |

The column functions can be used in all places where column fields are expected, such as `select, groupBy, orderBy, UDFs etc.` e.g.:

{{< tabs "402fe551-5fb9-4b17-bd64-e05cbd56b4cc" >}}
{{< tab "Java" >}}
```java
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns(range("a", "b")), myUDAgg(myUDF(withColumns(range(5, 20)))));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns('a to 'b), myUDAgg(myUDF(withColumns(5 to 20))))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table \
    .group_by(with_columns(range_(1, 3))) \
    .select(with_columns(range_('a', 'b')), myUDAgg(myUDF(with_columns(range_(5, 20)))))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
