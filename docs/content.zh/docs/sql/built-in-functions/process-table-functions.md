---
title: "Process Table"
weight: 12
type: docs
markdownFunctions: true
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

# Process Table Functions (PTF)

The built-in process table functions (PTFs) take a table as input and return a table. `SNAPSHOT` can only be used in a `LATERAL` context. The others are called in the `FROM` clause.

| Function                                                | Description                                                                                                                                                                                                                                                                                                    |
|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FROM_CHANGELOG(input => TABLE t [, ...])`              | Converts an append-only table with an explicit operation column into a dynamic table. See Changelog Conversion for the full list of arguments, semantics, and usage.                                                                                                                                           |
| `TO_CHANGELOG(input => TABLE t [, ...])`                | Converts a dynamic table into an append-only table with an explicit operation column. See Changelog Conversion for the full list of arguments, semantics, and usage.                                                                                                                                           |
| `SNAPSHOT(input => TABLE t [, ...])`                    | Returns the current state of a dynamic table `t`. `SNAPSHOT` can only be used in a `LATERAL` context and not as a stand-alone table function. See [LATERAL SNAPSHOT join]({{< ref "docs/sql/reference/queries/joins" >}}#lateral-snapshot-join) for the full list of arguments, the join semantics, and usage. |
| `DEDUPLICATE_KEEP_FIRST(input => TABLE t [, ...])`      | Keeps the first record per key as an append-only table (first arrival, or earliest event time with `on_time`). See [Deduplicate Keep First]({{< ref "docs/sql/reference/queries/deduplicate-keep-first" >}}#deduplicate_keep_first) for the full list of arguments, semantics, and usage.                      |
| `ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(...) [, ...])` | Applies the model `m` to the columns of table `t` that the `DESCRIPTOR` names, and returns the columns of `t` plus the prediction columns of the model. See [Model Inference]({{< ref "docs/sql/reference/queries/model-inference" >}}#ml_predict-函数) for the full list of arguments, semantics, and usage.  |

Window table functions such as `TUMBLE` are listed under [Window]({{< ref "docs/sql/built-in-functions/window" >}}#window-table-functions). To implement your own PTF, see [Process Table Functions]({{< ref "docs/dev/table/functions/ptfs" >}}).
