---
title: "Collection"
weight: 5
type: docs
sqlFunctions: collection
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

# Collection Functions

Collection functions build, read and change `ARRAY` and `MAP` values, such as `ARRAY_CONTAINS` and `MAP_KEYS`. Array positions start at 1, so `ARRAY[10, 20][1]` returns 10.

{{< sql_functions "collection" >}}
