---
title: "RESET Statements"
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

# RESET Statements

`RESET` statements are used to reset the configuration to the default.

## Run a RESET statement

{{< tabs "reset statement" >}}
{{< tab "SQL CLI" >}}

`RESET` statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a `RESET` statement in SQL CLI.

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

## Syntax

```sql
RESET ('key')?
```

If no key is specified, it reset all the properties to the default. Otherwise, reset the specified key to the default.

{{< top >}}
