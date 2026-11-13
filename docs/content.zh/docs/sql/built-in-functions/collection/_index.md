---
title: "集合"
weight: 5
type: docs
bookCollapseSection: true
sqlFunctions: collection
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

# 集合函数

Collection functions build, read and change `ARRAY` and `MAP` values, such as `ARRAY_CONTAINS` and `MAP_KEYS`. Array positions start at 1, so `ARRAY[10, 20][1]` returns 10.

{{< sql_functions_zh "collection" >}}

## Table Functions

A table function returns a table for scalar values of the currently processed row, such as `FROM orders CROSS JOIN UNNEST(tags) AS t(tag)`. See [Function Basics]({{< ref "docs/sql/function-basics" >}}#table) for the kinds of functions.

| Function                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                  |
|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `UNNEST(collection) [WITH ORDINALITY]` | Returns a row for each element of an `ARRAY` or `MULTISET`, and a row with the key and the value for each entry of a `MAP`. An element of type `ROW` becomes one column per field. `WITH ORDINALITY` adds a column with the position of each element, starting at 1. The order is guaranteed for arrays only. See [Array, Multiset and Map Expansion]({{< ref "docs/sql/reference/queries/joins" >}}#数组展开) for examples. |
