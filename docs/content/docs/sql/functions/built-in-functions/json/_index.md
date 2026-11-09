---
title: "JSON"
weight: 8
type: docs
sqlFunctions: json
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

# JSON Functions

JSON functions make use of JSON path expressions as described in ISO/IEC TR 19075-6 of the SQL
standard. Their syntax is inspired by and adopts many features of ECMAScript, but is neither a
subset nor superset thereof.

A JSON string is read up to the end of its first JSON value and content after it is ignored, so
`'{"a":1} xyz' IS JSON` returns `TRUE`.

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
