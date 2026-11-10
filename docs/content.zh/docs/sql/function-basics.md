---
title: "Function Basics"
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

# Function Basics

Flink SQL comes with built-in functions, and you can write your own. This page explains the kinds of functions and how to pass arguments to a function. [Built-in Functions]({{< ref "docs/sql/built-in-functions" >}}) lists the built-in functions by category.

## Kinds of Functions

### Scalar

A scalar function returns one value per call, usually one value per row, such as `UPPER` or `DATE_FORMAT`. Most built-in categories hold scalar functions, such as [String and Binary]({{< ref "docs/sql/built-in-functions/string-and-binary" >}}) and [Date and Time]({{< ref "docs/sql/built-in-functions/date-and-time" >}}).

### Aggregate

An aggregate function returns one value for a group of rows, such as `SUM` or `COUNT`. See [Aggregate]({{< ref "docs/sql/built-in-functions/aggregate" >}}) and the [bitmap aggregate functions]({{< ref "docs/sql/built-in-functions/bitmap" >}}#位图聚合函数). Aggregate functions also work over an [OVER window]({{< ref "docs/sql/reference/queries/over-agg" >}}).

### Window

A window function returns one value per row, computed from the window of rows that an `OVER` clause defines, such as `ROW_NUMBER` or `LAG`. See [Window]({{< ref "docs/sql/built-in-functions/window" >}}).

### Table

A table function takes scalar arguments and returns a table, with zero, one or more rows for each call. For example, the built-in [`UNNEST`]({{< ref "docs/sql/reference/queries/joins" >}}#数组展开) returns a row for each element of an array or multiset, or for each entry of a map. To call a user-defined table function, see [Table Function]({{< ref "docs/sql/reference/queries/joins" >}}#table-function) in Joins.

### Process Table

A process table function (PTF) takes one or more tables as arguments, besides scalar arguments, and returns a table. It processes a table argument either row by row, or per set of rows that `PARTITION BY` groups. The built-in PTFs, such as `FROM_CHANGELOG`, are listed in [Process Table Functions]({{< ref "docs/sql/built-in-functions/process-table-functions" >}}). The [window table functions]({{< ref "docs/sql/built-in-functions/window" >}}#window-table-functions), such as `TUMBLE`, also take a table and assign its rows to windows.

### User-Defined

Besides the built-in functions, you can write your own. See [User-Defined Functions]({{< ref "docs/sql/user-defined-functions" >}}). Built-in functions are system functions without a namespace. For catalog and temporary functions and how Flink resolves a function name, see [Functions]({{< ref "docs/dev/table/functions/overview" >}}) in the Table API docs.

## Named Arguments

By default, values and expressions are mapped to a function's arguments based on the position in the function call,
for example `f(42, true)`. All functions in both SQL and Table API support position-based arguments.

If the function declares a static signature, named arguments are available as a convenient alternative.
The framework is able to reorder named arguments and consider optional arguments accordingly, before passing them
into the function call. Thus, the order of arguments doesn't matter when calling a function and optional arguments
don't have to be provided.

In `DESCRIBE FUNCTION` and documentation a static signature is indicated by the `=>` assignment operator,
for example `f(left => INT, right => BOOLEAN)`. Note that not every function supports named arguments. Named
arguments are not available for signatures that are overloaded, use varargs, or any other kind of input type strategy.
User-defined functions with a single `eval()` method usually qualify for named arguments.

Named arguments can be used as shown below:

{{< tabs "902fe991-5fb9-4b17-ae99-f05cbd48b4dd" >}}
{{< tab "SQL" >}}
```text
SELECT MyUdf(input => my_column, threshold => 42)
```
{{< /tab >}}
{{< tab "Table API" >}}
```java
table.select(
  call(
    MyUdf.class,
    $("my_column").asArgument("input"),
    lit(42).asArgument("threshold")
  )
);
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
