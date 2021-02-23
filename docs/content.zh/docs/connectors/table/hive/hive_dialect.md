---
title: "Hive 方言"
weight: 3
type: docs
aliases:
  - /zh/dev/table/connectors/hive/hive_dialect.html
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

# Hive 方言

从 1.11.0 开始，在使用 Hive 方言时，Flink 允许用户用 Hive 语法来编写 SQL 语句。通过提供与 Hive 语法的兼容性，我们旨在改善与 Hive 的互操作性，并减少用户需要在 Flink 和 Hive 之间切换来执行不同语句的情况。

## 使用 Hive 方言

Flink 目前支持两种 SQL 方言: `default` 和 `hive`。你需要先切换到 Hive 方言，然后才能使用 Hive 语法编写。下面介绍如何使用 SQL 客户端和 Table API 设置方言。
还要注意，你可以为执行的每个语句动态切换方言。无需重新启动会话即可使用其他方言。

### SQL 客户端

SQL 方言可以通过 `table.sql-dialect` 属性指定。因此你可以通过 SQL 客户端 yaml 文件中的 `configuration` 部分来设置初始方言。

```yaml

execution:
  planner: blink
  type: batch
  result-mode: table

configuration:
  table.sql-dialect: hive

```

你同样可以在 SQL 客户端启动后设置方言。

```bash

Flink SQL> set table.sql-dialect=hive; -- to use hive dialect
[INFO] Session property has been set.

Flink SQL> set table.sql-dialect=default; -- to use default dialect
[INFO] Session property has been set.

```

### Table API

你可以使用 Table API 为 TableEnvironment 设置方言。

{{< tabs "82a7968d-df12-4db2-83ab-16f09b263935" >}}
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

本章节列出了 Hive 方言支持的 DDL 语句。我们主要关注语法。你可以参考 [Hive 文档](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
了解每个 DDL 语句的语义。

### CATALOG

#### Show

```sql
SHOW CURRENT CATALOG;
```

### DATABASE

#### Show

```sql
SHOW DATABASES;
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

如果指定了 `partition_spec`，那么必须完整，即具有所有分区列的值。如果指定了，该操作将作用在对应分区上而不是表上。

##### Update File Format

```sql
ALTER TABLE table_name [PARTITION partition_spec] SET FILEFORMAT file_format;
```

如果指定了 `partition_spec`，那么必须完整，即具有所有分区列的值。如果指定了，该操作将作用在对应分区上而不是表上。

##### Update SerDe Properties

```sql
ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_class_name [WITH SERDEPROPERTIES serde_properties];

ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;

serde_properties:
  : (property_name = property_value, property_name = property_value, ... )
```

如果指定了 `partition_spec`，那么必须完整，即具有所有分区列的值。如果指定了，该操作将作用在对应分区上而不是表上。

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

**注意**: 变更视图只在 Table API 中有效，SQL 客户端不支持。

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

## DML

### INSERT

```sql
INSERT (INTO|OVERWRITE) [TABLE] table_name [PARTITION partition_spec] SELECT ...;
```

如果指定了 `partition_spec`，可以是完整或者部分分区列。如果是部分指定，则可以省略动态分区的列名。

## DQL

目前，对于DQL语句 Hive 方言和 Flink SQL 支持的语法相同。有关更多详细信息，请参考[Flink SQL 查询]({{< ref "docs/dev/table/sql/queries" >}})。并且建议切换到 `default` 方言来执行 DQL 语句。

## 注意

以下是使用 Hive 方言的一些注意事项。

- Hive 方言只能用于操作 Hive 表，不能用于一般表。Hive 方言应与[HiveCatalog]({{< ref "docs/connectors/table/hive/hive_catalog" >}})一起使用。
- 虽然所有 Hive 版本支持相同的语法，但是一些特定的功能是否可用仍取决于你使用的[Hive 版本]({{< ref "docs/connectors/table/hive/overview" >}}#支持的hive版本)。例如，更新数据库位置
 只在 Hive-2.4.0 或更高版本支持。
- Hive 和 Calcite 有不同的保留关键字集合。例如，`default` 是 Calcite 的保留关键字，却不是 Hive 的保留关键字。即使使用 Hive 方言, 也必须使用反引号 ( ` ) 引用此类关键字才能将其用作标识符。
- 由于扩展的查询语句的不兼容性，在 Flink 中创建的视图是不能在 Hive 中查询的。
