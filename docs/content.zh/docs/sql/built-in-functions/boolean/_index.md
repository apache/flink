---
title: "Boolean"
weight: 3
type: docs
bookCollapseSection: true
sqlFunctions: logical
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

# Boolean Functions

Boolean functions combine and test `BOOLEAN` values. They follow three-valued logic, where NULL stands for UNKNOWN: `TRUE OR NULL` is TRUE, while `FALSE OR NULL` is UNKNOWN. The `IS` tests, such as `IS TRUE`, always return TRUE or FALSE.

{{< sql_functions_zh "logical" >}}
