---
title: "SET 语句"
weight: 14
type: docs
aliases:
  - /dev/table/sql/set.html
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

<a name="set-statements"></a>

# SET 语句

`SET` 语句用于修改配置或展示配置。

<a name="run-a-set-statement"></a>

## 执行 SET 语句

{{< tabs "set statement" >}}
{{< tab "SQL CLI" >}}

`SET` 语句可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行。

以下示例展示了如何在 SQL CLI 中执行一条 `SET` 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "set" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> SET 'table.local-time-zone' = 'Europe/Berlin';
[INFO] Session property has been set.

Flink SQL> SET;
'table.local-time-zone' = 'Europe/Berlin'
```
{{< /tab >}}
{{< /tabs >}}

<a name="syntax"></a>

## Syntax

```sql
SET ('key' = 'value')?
```

如果没有指定 key 和 value，它仅仅打印所有属性。否则，它会为 key 设置指定的 value 值。

{{< top >}}
