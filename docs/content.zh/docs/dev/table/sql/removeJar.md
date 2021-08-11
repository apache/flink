---
title: "REMOVE JAR Statements"
weight: 17
type: docs
aliases:
  - /dev/table/sql/removeJar.html
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

# REMOVE JAR Statements

`REMOVE JAR` statements are used to REMOVE the specified jar from the classloader.

## Run a REMOVE JAR statement

{{< tabs "remove jar statement" >}}
{{< tab "SQL CLI" >}}

`REMOVE JAR` statements can only be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run a `REMOVE JAR` statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "add jar" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> REMOVE JAR 'file://path/hello.jar';
[INFO] The specified jar is removed from session classloader.
```
{{< /tab >}}
{{< /tabs >}}

## Syntax

```sql
REMOVE JAR '<path_to_filename>.jar'
```

Currently it only supports to remove the jar that is added by the [ADD JAR]({{< ref "docs/dev/table/sql/addJar" >}}) command.

{{< top >}}
