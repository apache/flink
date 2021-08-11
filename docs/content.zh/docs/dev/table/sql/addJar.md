---
title: "ADD JAR Statements"
weight: 16
type: docs
aliases:
  - /dev/table/sql/addJar.html
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

# ADD JAR Statements

`ADD JAR` statements are used to add user jars into the classpath in the runtime.

## Run an ADD JAR statement

{{< tabs "add jar statement" >}}
{{< tab "SQL CLI" >}}

`ADD JAR` statements can only be executed in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

The following examples show how to run an `ADD JAR` statement in SQL CLI.

{{< /tab >}}
{{< /tabs >}}

{{< tabs "add jar" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> ADD JAR 'file://path/hello.jar';
[INFO] The specified jar is added into session classloader.
```
{{< /tab >}}
{{< /tabs >}}

## Syntax

```sql
ADD JAR '<path_to_filename>.jar'
```

Currently it only supports to add the local jar into the classloader.

{{< top >}}
