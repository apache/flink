---
title: "SET Statements"
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

# SET Statements

`SET` statements are used to modify the configuration or list the configuration.

## Run a SET statement

{{< tabs "set statement" >}}
{{< tab "SQL CLI" >}}

`SET` statements can be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a `SET` statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "set" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> SET table.planner = blink;
[INFO] Session property has been set.

Flink SQL> SET;
table.planner=blink;
```
{{< /tab >}}
{{< /tabs >}}

## Syntax

```sql
SET (key = value)?
```

If no key and value are specified, it just print all the properties. Otherwise, set the key with specified value.

{{< top >}}
