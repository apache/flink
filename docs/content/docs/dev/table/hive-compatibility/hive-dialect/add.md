---
title: "ADD Statements"
weight: 7
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

# ADD Statements

With Hive dialect, the following `ADD` statements are supported for now:
- ADD JAR

## ADD JAR

### Description

`ADD JAR` statement is used to add user jars into the classpath.
Add multiple jars file in single `ADD JAR` statement is not supported.


### Syntax

```sql
ADD JAR <jar_path>;
```

### Parameters

- jar_path

  The path of the JAR file to be added. It could be either on a local file or distributed file system.

### Examples

```sql
-- add a local jar
ADD JAR t.jar;

-- add a remote jar
ADD JAR hdfs://namenode-host:port/path/t.jar
```
