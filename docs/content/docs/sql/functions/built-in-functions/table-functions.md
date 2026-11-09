---
title: "Table Functions"
weight: 21
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

# Table Functions

Table functions take zero, one, or more values as input and return multiple rows (a table) as the result. Most built-in table functions take a table as an input argument.
Table functions can be used in two ways: as stand-alone inputs, where they are invoked just once, or in a `LATERAL` context, where they are invoked for each row of an outer table.

| Function                                   | Description                                                                                                                                                                                                                                                                                                     |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TUMBLE(data => TABLE t, ...)`             | Assigns each row of the `data` table to a tumbling window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#tumble) for the full list of arguments, semantics, and usage.                              |
| `HOP(data => TABLE t, ...)`                | Assigns each row of the `data` table to a hopping window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#hop) for the full list of arguments, semantics, and usage.                                  |
| `CUMULATE(data => TABLE t, ...)`           | Assigns each row of the `data` table to a cumulating window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#cumulate) for the full list of arguments, semantics, and usage.                          |
| `SESSION(data => TABLE t, ...)`            | Assigns each row of the `data` table to a session window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#session) for the full list of arguments, semantics, and usage.                              |
| `FROM_CHANGELOG(input => TABLE t [, ...])` | Converts an append-only table with an explicit operation column into a dynamic table. See [Changelog Conversion]({{< ref "docs/sql/reference/queries/changelog" >}}#from_changelog) for the full list of arguments, semantics, and usage.                                                                       |
| `TO_CHANGELOG(input => TABLE t [, ...])`   | Converts a dynamic table into an append-only table with an explicit operation column. See [Changelog Conversion]({{< ref "docs/sql/reference/queries/changelog" >}}#to_changelog) for the full list of arguments, semantics, and usage.                                                                         |
| `SNAPSHOT(input => TABLE t [, ...])`       | Returns the current state of a dynamic table `t`. `SNAPSHOT` can only be used in a `LATERAL` context and not as a stand-alone table function. See [LATERAL SNAPSHOT join]({{< ref "docs/sql/reference/queries/joins" >}}#lateral-snapshot-join) for the full list of arguments, the join semantics, and usage. |
| `DEDUPLICATE_KEEP_FIRST(input => TABLE t [, ...])` | Keeps the first record per key as an append-only table (first arrival, or earliest event time with `on_time`). See [Deduplicate Keep First]({{< ref "docs/sql/reference/queries/deduplicate-keep-first" >}}#deduplicate_keep_first) for the full list of arguments, semantics, and usage. |

To implement your own table functions, see [user-defined table functions]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions).
