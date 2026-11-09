---
title: "Window"
weight: 18
type: docs
sqlFunctions: window
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

# Window Functions

{{< sql_functions "window" >}}

## Window Table Functions

Window table functions assign each row of the `data` table to windows and add the window columns `window_start`, `window_end` and `window_time`. See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}) for how to use them.

| Function                         | Description                                                                                                                                                                                                                                                                            |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TUMBLE(data => TABLE t, ...)`   | Assigns each row of the `data` table to a tumbling window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#tumble) for the full list of arguments, semantics, and usage.     |
| `HOP(data => TABLE t, ...)`      | Assigns each row of the `data` table to a hopping window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#hop) for the full list of arguments, semantics, and usage.         |
| `CUMULATE(data => TABLE t, ...)` | Assigns each row of the `data` table to a cumulating window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#cumulate) for the full list of arguments, semantics, and usage. |
| `SESSION(data => TABLE t, ...)`  | Assigns each row of the `data` table to a session window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#session) for the full list of arguments, semantics, and usage.     |
