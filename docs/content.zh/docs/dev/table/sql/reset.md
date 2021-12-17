---
title: "RESET 语句"
weight: 15
type: docs
aliases:
  - /dev/table/sql/reset.html
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

<a name="reset-statements"></a>

# RESET 语句

`RESET` 语句用于将配置重置为默认值。

<a name="run-a-reset-statement"></a>

## 执行 RESET 语句

{{< tabs "reset statement" >}}
{{< tab "SQL CLI" >}}

`RESET` 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 `RESET` 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "reset" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> RESET 'table.planner';
[INFO] Session property has been reset.

Flink SQL> RESET;
[INFO] All session properties have been set to their default values.
```
{{< /tab >}}
{{< /tabs >}}

<a name="syntax"></a>

## Syntax

```sql
RESET ('key')?
```

如果未指定 key，则将所有属性重置为默认值。否则，将指定的 key 重置为默认值。

{{< top >}}
