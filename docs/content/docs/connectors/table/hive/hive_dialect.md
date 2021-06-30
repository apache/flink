---
title: "Hive Dialect"
weight: 3
type: docs
aliases:
  - /dev/table/connectors/hive/hive_dialect.html
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

# Hive Dialect

Flink allows users to write SQL statements in Hive syntax when Hive dialect
is used. By providing compatibility with Hive syntax, we aim to improve the interoperability with
Hive and reduce the scenarios when users need to switch between Flink and Hive in order to execute
different statements.

## Use Hive Dialect

Flink currently supports two SQL dialects: `default` and `hive`. You need to switch to Hive dialect
before you can write in Hive syntax. The following describes how to set dialect with
SQL Client and Table API. Also notice that you can dynamically switch dialect for each
statement you execute. There's no need to restart a session to use a different dialect.

### SQL Client

SQL dialect can be specified via the `table.sql-dialect` property. Therefore you can set the initial dialect to use in
the `configuration` section of the yaml file for your SQL Client.

```yaml

execution:
  planner: blink
  type: batch
  result-mode: table

configuration:
  table.sql-dialect: hive

```

You can also set the dialect after the SQL Client has launched.

```bash

Flink SQL> set table.sql-dialect=hive; -- to use hive dialect
[INFO] Session property has been set.

Flink SQL> set table.sql-dialect=default; -- to use default dialect
[INFO] Session property has been set.

```

### Table API

You can set dialect for your TableEnvironment with Table API.

{{< tabs "f19e5e09-c58d-424d-999d-275106d1d5b3" >}}
{{< tab "Java" >}}
```java

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()...build();
TableEnvironment tableEnv = TableEnvironment.create(settings);
// to use hive dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
// to use default dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

# to use hive dialect
t_env.get_config().set_sql_dialect(SqlDialect.HIVE)
# to use default dialect
t_env.get_config().set_sql_dialect(SqlDialect.DEFAULT)

```
{{< /tab >}}
{{< /tabs >}}

## DDL

This section lists the supported DDLs with the Hive dialect. We'll mainly focus on the syntax
here. You can refer to [Hive doc](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
for the semantics of each DDL statement.

### CATALOG

#### Show

```sql
SHOW CURRENT CATALOG;
```

### DATABASE

#### Show

```sql
SHOW DATABASES;
SHOW CURRENT DATABASE;
```

#### Create

```sql
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  [COMMENT database_comment]
  [LOCATION fs_path]
  [WITH DBPROPERTIES (property_name=property_value, ...)];
```

#### Alter

##### Update Properties

```sql
ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);
```

##### Update Owner

```sql
ALTER (DATABASE|SCHEMA) database_name SET OWNER [USER|ROLE] user_or_role;
```

##### Update Location

```sql
ALTER (DATABASE|SCHEMA) database_name SET LOCATION fs_path;
```

#### Drop

```sql
DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
```

#### Use

```sql
USE database_name;
```

### TABLE

#### Show

```sql
SHOW TABLES;
```

#### Create

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name
  [(col_name data_type [column_constraint] [COMMENT col_comment], ... [table_constraint])]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [
    [ROW FORMAT row_format]
    [STORED AS file_format]
  ]
  [LOCATION fs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]

row_format:
  : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]] [COLLECTION ITEMS TERMINATED BY char]
      [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]
      [NULL DEFINED AS char]
  | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, ...)]

file_format:
  : SEQUENCEFILE
  | TEXTFILE
  | RCFILE
  | ORC
  | PARQUET
  | AVRO
  | INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname

column_constraint:
  : NOT NULL [[ENABLE|DISABLE] [VALIDATE|NOVALIDATE] [RELY|NORELY]]

table_constraint:
  : [CONSTRAINT constraint_name] PRIMARY KEY (col_name, ...) [[ENABLE|DISABLE] [VALIDATE|NOVALIDATE] [RELY|NORELY]]
```

#### Alter

##### Rename

```sql
ALTER TABLE table_name RENAME TO new_table_name;
```

##### Update Properties

```sql
ALTER TABLE table_name SET TBLPROPERTIES (property_name = property_value, property_name = property_value, ... );
```

##### Update Location

```sql
ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION fs_path;
```

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Update File Format

```sql
ALTER TABLE table_name [PARTITION partition_spec] SET FILEFORMAT file_format;
```

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Update SerDe Properties

```sql
ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_class_name [WITH SERDEPROPERTIES serde_properties];

ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;

serde_properties:
  : (property_name = property_value, property_name = property_value, ... )
```

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Add Partitions

```sql
ALTER TABLE table_name ADD [IF NOT EXISTS] (PARTITION partition_spec [LOCATION fs_path])+;
```

##### Drop Partitions

```sql
ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec[, PARTITION partition_spec, ...];
```

##### Add/Replace Columns

```sql
ALTER TABLE table_name
  ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...)
  [CASCADE|RESTRICT]
```

##### Change Column

```sql
ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type
  [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT];
```

#### Drop

```sql
DROP TABLE [IF EXISTS] table_name;
```

### VIEW

#### Create

```sql
CREATE VIEW [IF NOT EXISTS] view_name [(column_name, ...) ]
  [COMMENT view_comment]
  [TBLPROPERTIES (property_name = property_value, ...)]
  AS SELECT ...;
```

#### Alter

##### Rename

```sql
ALTER VIEW view_name RENAME TO new_view_name;
```

##### Update Properties

```sql
ALTER VIEW view_name SET TBLPROPERTIES (property_name = property_value, ... );
```

##### Update As Select

```sql
ALTER VIEW view_name AS select_statement;
```

#### Drop

```sql
DROP VIEW [IF EXISTS] view_name;
```

### FUNCTION

#### Show

```sql
SHOW FUNCTIONS;
```

#### Create

```sql
CREATE FUNCTION function_name AS class_name;
```

#### Drop

```sql
DROP FUNCTION [IF EXISTS] function_name;
```

## DML & DQL _`Beta`_

Hive dialect supports a commonly-used subset of Hive's [DML](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML)
and [DQL](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select). The following lists some examples of
HiveQL supported by the Hive dialect.

- [SORT/CLUSTER/DISTRIBUTE BY](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy)
- [Group By](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+GroupBy)
- [Join](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Joins)
- [Union](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union)
- [LATERAL VIEW](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView)
- [Window Functions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics)
- [SubQueries](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SubQueries)
- [CTE](https://cwiki.apache.org/confluence/display/Hive/Common+Table+Expression)
- [INSERT INTO dest schema](https://issues.apache.org/jira/browse/HIVE-9481)
- [Implicit type conversions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-AllowedImplicitConversions)

In order to have better syntax and semantic compatibility, it's highly recommended to use [HiveModule]({{< ref "docs/connectors/table/hive/hive_functions" >}}#use-hive-built-in-functions-via-hivemodule)
and place it first in the module list, so that Hive built-in functions can be picked up during function resolution.

Hive dialect no longer supports [Flink SQL queries]({{< ref "docs/dev/table/sql/queries/overview" >}}). Please switch to `default`
dialect if you'd like to write in Flink syntax.

Following is an example of using hive dialect to run some queries.

```bash
Flink SQL> create catalog myhive with ('type' = 'hive', 'hive-conf-dir' = '/opt/hive-conf');
[INFO] Execute statement succeed.

Flink SQL> use catalog myhive;
[INFO] Execute statement succeed.

Flink SQL> load module hive;
[INFO] Execute statement succeed.

Flink SQL> use modules hive,core;
[INFO] Execute statement succeed.

Flink SQL> set table.sql-dialect=hive;
[INFO] Session property has been set.

Flink SQL> select explode(array(1,2,3)); -- call hive udtf
+-----+
| col |
+-----+
|   1 |
|   2 |
|   3 |
+-----+
3 rows in set

Flink SQL> create table tbl (key int,value string);
[INFO] Execute statement succeed.

Flink SQL> insert overwrite table tbl values (5,'e'),(1,'a'),(1,'a'),(3,'c'),(2,'b'),(3,'c'),(3,'c'),(4,'d');
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:

Flink SQL> select * from tbl cluster by key; -- run cluster by
2021-04-22 16:13:57,005 INFO  org.apache.hadoop.mapred.FileInputFormat                     [] - Total input paths to process : 1
+-----+-------+
| key | value |
+-----+-------+
|   1 |     a |
|   1 |     a |
|   5 |     e |
|   2 |     b |
|   3 |     c |
|   3 |     c |
|   3 |     c |
|   4 |     d |
+-----+-------+
8 rows in set
```

## Notice

The following are some precautions for using the Hive dialect.

- Hive dialect should only be used to process Hive meta objects, and requires the current catalog to be a
[HiveCatalog]({{< ref "docs/connectors/table/hive/hive_catalog" >}}).
- Hive dialect only supports 2-part identifiers, so you can't specify catalog for an identifier.
- While all Hive versions support the same syntax, whether a specific feature is available still depends on the
[Hive version]({{< ref "docs/connectors/table/hive/overview" >}}#supported-hive-versions) you use. For example, updating database
location is only supported in Hive-2.4.0 or later.
- Use [HiveModule]({{< ref "docs/connectors/table/hive/hive_functions" >}}#use-hive-built-in-functions-via-hivemodule)
to run DML and DQL.
