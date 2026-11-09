---
title: "Date & Time"
weight: 8
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

{{< sql_functions "temporal" >}}

## Time Interval and Point Unit Specifiers

The following table lists specifiers for time interval and time point units. 

For Table API, please use `_` for spaces (e.g., `DAY_TO_HOUR`).
Plural works for SQL only. 

| Time Interval Unit       | Time Point Unit                |
|:-------------------------|:-------------------------------|
| `MILLENNIUM`             |                                |
| `CENTURY`                |                                |
| `DECADE`                 |                                |
| `YEAR(S)`                | `YEAR`                         |
| `YEAR(S) TO MONTH(S)`    |                                |
| `QUARTER(S)`             | `QUARTER`                      |
| `MONTH(S)`               | `MONTH`                        |
| `WEEK(S)`                | `WEEK`                         |
| `DAY(S)`                 | `DAY`                          |
| `DAY(S) TO HOUR(S)`      |                                |
| `DAY(S) TO MINUTE(S)`    |                                |
| `DAY(S) TO SECOND(S)`    |                                |
| `HOUR(S)`                | `HOUR`                         |
| `HOUR(S) TO MINUTE(S)`   |                                |
| `HOUR(S) TO SECOND(S)`   |                                |
| `MINUTE(S)`              | `MINUTE`                       |
| `MINUTE(S) TO SECOND(S)` |                                |
| `SECOND(S)`              | `SECOND`                       |
| `MILLISECOND`            | `MILLISECOND`                  |
| `MICROSECOND`            | `MICROSECOND`                  |
| `NANOSECOND`             |                                |
| `EPOCH`                  |                                |
| `DOY` _(SQL-only)_       |                                |
| `DOW` _(SQL-only)_       |                                |
| `EPOCH` _(SQL-only)_     |                                |
| `ISODOW` _(SQL-only)_    |                                |
| `ISOYEAR` _(SQL-only)_   |                                |
|                          | `SQL_TSI_YEAR` _(SQL-only)_    |
|                          | `SQL_TSI_QUARTER` _(SQL-only)_ |
|                          | `SQL_TSI_MONTH` _(SQL-only)_   |
|                          | `SQL_TSI_WEEK` _(SQL-only)_    |
|                          | `SQL_TSI_DAY` _(SQL-only)_     |
|                          | `SQL_TSI_HOUR` _(SQL-only)_    |
|                          | `SQL_TSI_MINUTE` _(SQL-only)_  |
|                          | `SQL_TSI_SECOND ` _(SQL-only)_ |

{{< top >}}
