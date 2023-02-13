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
- SHOW JARS
- REMOVE JAR

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
[INFO] Execute statement succeed.

Flink SQL> ADD JAR 'hdfs:///udf/common-udf.jar';
[INFO] Execute statement succeed.

Flink SQL> SHOW JARS;
+----------------------------+
|                       jars |
+----------------------------+
|            /path/hello.jar |
| hdfs:///udf/common-udf.jar |
+----------------------------+

Flink SQL> REMOVE JAR '/path/hello.jar';
[INFO] The specified jar is removed from session classloader.
```
{{< /tab >}}
{{< /tabs >}}

## ADD JAR

```sql
ADD JAR '<path_to_filename>.jar'
```

Add a JAR file to the list of resources, it supports adding the jar locates in a local or remote [file system]({{< ref "docs/deployment/filesystems/overview" >}}). The added JAR file can be listed using [`SHOW JARS`](#show-jars) statements.

### Limitation
Please don't use `ADD JAR` statements to load Hive source/sink/function/catalog. This is a known limitation of Hive connector and will be fixed in the future version. Currently, it's recommended to follow this [instruction]({{< ref "docs/connectors/table/hive/overview" >}}#dependencies) to setup Hive integration.

## SHOW JARS

```sql
SHOW JARS
```

Show all added jars which are added by [`ADD JAR`](#add-jar) statements.

## REMOVE JAR

```sql
REMOVE JAR '<path_to_filename>.jar'
```

Remove the specified jar that is added by the [`ADD JAR`](#add-jar) statements.

<span class="label label-danger">Attention</span> REMOVE JAR statements only work in [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}).

{{< top >}}
