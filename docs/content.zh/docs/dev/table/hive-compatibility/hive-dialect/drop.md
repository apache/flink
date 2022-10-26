---
title: "DROP Statements"
weight: 2
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

# CREATE Statements

With Hive dialect, the following DROP statements are supported for now:

- DROP DATABASE
- DROP TABLE
- DROP VIEW
- DROP MARCO
- DROP FUNCTION

## DROP DATABASE

### Description

`DROP DATABASE` statement is used to drop a database as well as the tables/directories associated with the database.

### Syntax

```sql
DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
```
The use of `SCHEMA` and `DATABASE` are interchangeable - they mean the same thing.
The default behavior is `RESTRICT`, where `DROP DATABASE` will fail if the database is not empty.
To drop the tables in the database as well, use `DROP DATABASE ... CASCADE`.

`DROP` returns an error if the database doesn't exist, unless `IF EXISTS` is specified
or the configuration variable [hive.exec.drop.ignorenonexistent](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.exec.drop.ignorenonexistent)
is set to true.

### Examples

```sql
DROP DATABASE db1 CASCADE;
```

## DROP TABLE

### Description

`DROP TABLE` statement removes metadata and data for this table.
The data is actually moved to the `.Trash/Current` directory if Trash is configured.
The metadata is completely lost.

When drop an `EXTERNAL` table, data in the table will not be deleted from the filesystem.

### Syntax

```sql
DROP TABLE [IF EXISTS] table_name;
```

`DROP` returns an error if the table doesn't exist, unless `IF EXISTS` is specified
or the configuration variable [hive.exec.drop.ignorenonexistent](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.exec.drop.ignorenonexistent)
is set to true.

### Examples

```sql
DROP TABLE IF EXISTS t1;
```

## DROP VIEW

### Description

`DROP VIEW` statement is used to removed metadata for the specified view.

### Syntax

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```
`DROP` returns an error if the view doesn't exist, unless `IF EXISTS` is specified
or the configuration variable [hive.exec.drop.ignorenonexistent](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.exec.drop.ignorenonexistent)
is set to true.

### Examples

```sql
DROP VIEW IF EXISTS v1;
```

## DROP MARCO

`DROP MARCO` statement is used to drop the existing `MARCO`.
Please refer to [CREATE MARCO]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/create" >}}#create-marco) for how to create `MARCO`.

### Syntax

```sql
DROP TEMPORARY MACRO [IF EXISTS] macro_name;
```
`DROP` returns an error if the macro doesn't exist, unless `IF EXISTS` is specified.

### Examples

```sql
DROP TEMPORARY MACRO IF EXISTS m1;
```

## DROP FUNCTION

`DROP FUNCTION` statement is used to drop the existing `FUNCTION`.

### Syntax

```sql
--- Drop temporary function
DROP TEMPORARY FUNCTION [IF EXISTS] function_name;

--- Drop permanent function
DROP FUNCTION [IF EXISTS] function_name;
```
`DROP` returns an error if the function doesn't exist, unless `IF EXISTS` is specified
or the configuration variable [hive.exec.drop.ignorenonexistent](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.exec.drop.ignorenonexistent)
is set to true.

### Examples

```sql
DROP FUNCTION IF EXISTS f1;
```
