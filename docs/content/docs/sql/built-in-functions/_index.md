---
title: "Built-in Functions"
weight: 4
type: docs
bookCollapseSection: true
aliases:
  - /dev/table/functions/systemFunctions.html
  - /docs/sql/functions/
  - /docs/sql/functions/built-in-functions/
  - /dev/python/table-api-users-guide/built_in_functions.html
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

[All Functions]({{< ref "docs/sql/built-in-functions/all-functions" >}}) lists every built-in function from A to Z.

## Kinds of Functions

### Scalar

A scalar function returns one value per call, usually one value per row, such as `UPPER` or `DATE_FORMAT`. Most categories hold scalar functions, such as [String and Binary]({{< ref "docs/sql/built-in-functions/string-and-binary" >}}) and [Date and Time]({{< ref "docs/sql/built-in-functions/date-and-time" >}}).

### Aggregate

An aggregate function returns one value for a group of rows, such as `SUM` or `COUNT`. See [Aggregate]({{< ref "docs/sql/built-in-functions/aggregate" >}}) and the [bitmap aggregate functions]({{< ref "docs/sql/built-in-functions/bitmap" >}}#bitmap-aggregate-functions). Aggregate functions also work over an [OVER window]({{< ref "docs/sql/reference/queries/over-agg" >}}).

### Window

A window function returns one value per row, computed from the window of rows that an `OVER` clause defines, such as `ROW_NUMBER` or `LAG`. See [Window]({{< ref "docs/sql/built-in-functions/window" >}}).

### Table

A table function returns a table. The built-in ones are [process table functions]({{< ref "docs/sql/built-in-functions/process-table-functions" >}}), such as `FROM_CHANGELOG`, and [window table functions]({{< ref "docs/sql/built-in-functions/window" >}}#window-table-functions), such as `TUMBLE`.

### User-Defined

Besides the built-in functions, you can write your own. See [User-Defined Functions]({{< ref "docs/sql/user-defined-functions" >}}) and [Named Arguments]({{< ref "docs/sql/named-arguments" >}}). Built-in functions are system functions without a namespace. For catalog and temporary functions and how Flink resolves a function name, see [Functions]({{< ref "docs/dev/table/functions/overview" >}}) in the Table API docs.


{{< built_in_functions_legacy_anchors >}}
