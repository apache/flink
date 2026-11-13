---
title: "比较"
weight: 6
type: docs
bookCollapseSection: true
sqlFunctions: comparison
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

# 比较函数

Comparison functions compare values and return a `BOOLEAN`, such as `=`, `BETWEEN ... AND`, `LIKE` and `IN`. The operators `=`, `<>`, `<`, `<=`, `>` and `>=` return UNKNOWN when either side is NULL. `IS NULL`, `IS NOT NULL` and `IS [NOT ]DISTINCT FROM` never return UNKNOWN, and `IS [NOT ]DISTINCT FROM` treats two NULLs as equal.

{{< sql_functions_zh "comparison" >}}
