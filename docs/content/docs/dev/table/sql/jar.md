---
title: "JAR Statements"
weight: 16
type: docs
aliases:
  - /dev/table/sql/jar.html
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

# JAR Statements

JAR statements are used to add user jars into the classpath or remove user jars from the classpath
or show added jars in the classpath in the runtime.

Flink SQL supports the following JAR statements for now:
- ADD JAR
- REMOVE JAR
- SHOW JARS

<span class="label label-danger">Attention</span> JAR statements only work in the [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).


## Run a JAR statement

{{< tabs "add jar statement" >}}
{{< tab "SQL CLI" >}}

The following examples show how to run `JAR` statements in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

{{< /tab >}}
{{< /tabs >}}

{{< tabs "add jar" >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> ADD JAR '/path/hello.jar';
[INFO] The specified jar is added into session classloader.

Flink SQL> SHOW JARS;
/path/hello.jar

Flink SQL> REMOVE JAR '/path/hello.jar';
[INFO] The specified jar is removed from session classloader.
```
{{< /tab >}}
{{< /tabs >}}

## ADD JAR

```sql
ADD JAR '<path_to_filename>.jar'
```

Currently it only supports to add the local jar into the session classloader.

## REMOVE JAR

```sql
REMOVE JAR '<path_to_filename>.jar'
```

Currently it only supports to remove the jar that is added by the [`ADD JAR`](#add-jar) statements.

## SHOW JARS

```sql
SHOW JARS
```

Show all added jars in the session classloader which are added by [`ADD JAR`](#add-jar) statements.

{{< top >}}
