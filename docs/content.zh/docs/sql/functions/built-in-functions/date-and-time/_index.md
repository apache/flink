---
title: "Date and Time"
weight: 6
type: docs
sqlFunctions: temporal
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

# Date & Time Functions

{{< sql_functions_zh "temporal" >}}

## 时间间隔单位和时间点单位标识符

下表列出了时间间隔单位和时间点单位标识符。

对于 Table API，请使用 `_` 代替空格（例如 `DAY_TO_HOUR`）。
Plural works for SQL only.

| 时间间隔单位                   | 时间点单位                        |
|:-------------------------|:-----------------------------|
| `MILLENNIUM`             |                              |
| `CENTURY`                |                              |
| `DECADE`                 |                              |
| `YEAR(S)`                | `YEAR`                       |
| `YEAR(S) TO MONTH(S)`    |                              |
| `QUARTER(S)`             | `QUARTER`                    |
| `MONTH(S)`               | `MONTH`                      |
| `WEEK(S)`                | `WEEK`                       |
| `DAY(S)`                 | `DAY`                        |
| `DAY(S) TO HOUR(S)`      |                              |
| `DAY(S) TO MINUTE(S)`    |                              |
| `DAY(S) TO SECOND(S)`    |                              |
| `HOUR(S)`                | `HOUR`                       |
| `HOUR(S) TO MINUTE(S)`   |                              |
| `HOUR(S) TO SECOND(S)`   |                              |
| `MINUTE(S)`              | `MINUTE`                     |
| `MINUTE(S) TO SECOND(S)` |                              |
| `SECOND(S)`              | `SECOND`                     |
| `MILLISECOND`            | `MILLISECOND`                |
| `MICROSECOND`            | `MICROSECOND`                |
| `NANOSECOND`             |                              |
| `EPOCH`                  |                              |
| `DOY` _（仅适用SQL）_         |                              |
| `DOW` _（仅适用SQL）_         |                              |
| `ISODOW` _（仅适用SQL）_      |                              |
| `ISOYEAR` _（仅适用SQL）_     |                              |
|                          | `SQL_TSI_YEAR` _（仅适用SQL）_    |
|                          | `SQL_TSI_QUARTER` _（仅适用SQL）_ |
|                          | `SQL_TSI_MONTH` _（仅适用SQL）_   |
|                          | `SQL_TSI_WEEK` _（仅适用SQL）_    |
|                          | `SQL_TSI_DAY` _（仅适用SQL）_     |
|                          | `SQL_TSI_HOUR` _（仅适用SQL）_    |
|                          | `SQL_TSI_MINUTE` _（仅适用SQL）_  |
|                          | `SQL_TSI_SECOND ` _（仅适用SQL）_ |

{{< top >}}
