---
title: "Table Sample"
weight: 11
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

# Table Sample

## Description

The `TABLESAMPLE` statement is used to sample rows from the table.

### Syntax

```sql
TABLESAMPLE ( num_rows ROWS )
```
{{< hint warning >}}
**Note:**
Currently, only sample specific number of rows is supported.
{{< /hint >}}

### Parameters

- num_rows `ROWS`

  num_rows is a constant positive to specify how many rows to sample.

### Examples

```sql
SELECT * FROM src TABLESAMPLE (5 ROWS)
```
