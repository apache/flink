---
title: "Hive Dialect"
nav-parent_id: hive_tableapi
nav-pos: 1
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

Starting from 1.11.0, Flink allows users to write SQL statements in Hive syntax when Hive dialect
is used. By providing compatibility with Hive syntax, we aim to improve the interoperability with
Hive and reduce the scenarios when users need to switch between Flink and Hive in order to execute
different statements.

* This will be replaced by the TOC
{:toc}

## Use Hive Dialect

Flink currently supports two SQL dialects: `default` and `hive`. You need to switch to Hive dialect
before you can write in Hive syntax. The following describes how to set dialect with
SQL Client and Table API. Also notice that you can dynamically switch dialect for each
statement you execute. There's no need to restart a session to use a different dialect.

### SQL Client

SQL dialect can be specified via the `table.sql-dialect` property. Therefore you can set the initial dialect to use in
the `configuration` section of the yaml file for your SQL Client.

{% highlight yaml %}

execution:
  planner: blink
  type: batch
  result-mode: table

configuration:
  table.sql-dialect: hive
  
{% endhighlight %}

You can also set the dialect after the SQL Client has launched.

{% highlight bash %}

Flink SQL> set table.sql-dialect=hive; -- to use hive dialect
[INFO] Session property has been set.

Flink SQL> set table.sql-dialect=default; -- to use default dialect
[INFO] Session property has been set.

{% endhighlight %}

### Table API

You can set dialect for your TableEnvironment with Table API.

{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()...build();
TableEnvironment tableEnv = TableEnvironment.create(settings);
// to use hive dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
// to use default dialect
tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

{% endhighlight %}

## DDL

This section lists the supported DDLs with the Hive dialect. We'll mainly focus on the syntax
here. You can refer to [Hive doc](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL)
for the semantics of each DDL statement.

### DATABASE

#### Show

{% highlight sql %}
SHOW DATABASES;
{% endhighlight %}

#### Create

{% highlight sql %}
CREATE (DATABASE|SCHEMA) [IF NOT EXISTS] database_name
  [COMMENT database_comment]
  [LOCATION fs_path]
  [WITH DBPROPERTIES (property_name=property_value, ...)];
{% endhighlight %}

#### Alter

##### Update Properties

{% highlight sql %}
ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...);
{% endhighlight %}

##### Update Owner

{% highlight sql %}
ALTER (DATABASE|SCHEMA) database_name SET OWNER [USER|ROLE] user_or_role;
{% endhighlight %}

##### Update Location

{% highlight sql %}
ALTER (DATABASE|SCHEMA) database_name SET LOCATION fs_path;
{% endhighlight %}

#### Drop

{% highlight sql %}
DROP (DATABASE|SCHEMA) [IF EXISTS] database_name [RESTRICT|CASCADE];
{% endhighlight %}

#### Use

{% highlight sql %}
USE database_name;
{% endhighlight %}

### TABLE

#### Show

{% highlight sql %}
SHOW TABLES;
{% endhighlight %}

#### Create

{% highlight sql %}
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
{% endhighlight %}

#### Alter

##### Rename

{% highlight sql %}
ALTER TABLE table_name RENAME TO new_table_name;
{% endhighlight %}

##### Update Properties

{% highlight sql %}
ALTER TABLE table_name SET TBLPROPERTIES (property_name = property_value, property_name = property_value, ... );
{% endhighlight %}

##### Update Location

{% highlight sql %}
ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION fs_path;
{% endhighlight %}

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Update File Format

{% highlight sql %}
ALTER TABLE table_name [PARTITION partition_spec] SET FILEFORMAT file_format;
{% endhighlight %}

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Update SerDe Properties

{% highlight sql %}
ALTER TABLE table_name [PARTITION partition_spec] SET SERDE serde_class_name [WITH SERDEPROPERTIES serde_properties];
 
ALTER TABLE table_name [PARTITION partition_spec] SET SERDEPROPERTIES serde_properties;
 
serde_properties:
  : (property_name = property_value, property_name = property_value, ... )
{% endhighlight %}

The `partition_spec`, if present, needs to be a full spec, i.e. has values for all partition columns. And when it's
present, the operation will be applied to the corresponding partition instead of the table.

##### Add Partitions

{% highlight sql %}
ALTER TABLE table_name ADD [IF NOT EXISTS] (PARTITION partition_spec [LOCATION fs_path])+;
{% endhighlight %}

##### Drop Partitions

{% highlight sql %}
ALTER TABLE table_name DROP [IF EXISTS] PARTITION partition_spec[, PARTITION partition_spec, ...];
{% endhighlight %}

##### Add/Replace Columns

{% highlight sql %}
ALTER TABLE table_name
  ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...)
  [CASCADE|RESTRICT]
{% endhighlight %}

##### Change Column

{% highlight sql %}
ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type
  [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT];
{% endhighlight %}

#### Drop

{% highlight sql %}
DROP TABLE [IF EXISTS] table_name;
{% endhighlight %}

### VIEW

#### Create

{% highlight sql %}
CREATE VIEW [IF NOT EXISTS] view_name [(column_name, ...) ]
  [COMMENT view_comment]
  [TBLPROPERTIES (property_name = property_value, ...)]
  AS SELECT ...;
{% endhighlight %}

#### Alter

**NOTE**: Altering view only works in Table API, but not supported via SQL client.

##### Rename

{% highlight sql %}
ALTER VIEW view_name RENAME TO new_view_name;
{% endhighlight %}

##### Update Properties

{% highlight sql %}
ALTER VIEW view_name SET TBLPROPERTIES (property_name = property_value, ... );
{% endhighlight %}

##### Update As Select

{% highlight sql %}
ALTER VIEW view_name AS select_statement;
{% endhighlight %}

#### Drop

{% highlight sql %}
DROP VIEW [IF EXISTS] view_name;
{% endhighlight %}

### FUNCTION

#### Show

{% highlight sql %}
SHOW FUNCTIONS;
{% endhighlight %}

#### Create

{% highlight sql %}
CREATE FUNCTION function_name AS class_name;
{% endhighlight %}

#### Drop

{% highlight sql %}
DROP FUNCTION [IF EXISTS] function_name;
{% endhighlight %}

## DML

### INSERT

{% highlight sql %}
INSERT (INTO|OVERWRITE) [TABLE] table_name [PARTITION partition_spec] SELECT ...;
{% endhighlight %}

The `partition_spec`, if present, can be either a full spec or partial spec. If the `partition_spec` is a partial
spec, the dynamic partition column names can be omitted.

## DQL

At the moment, Hive dialect supports the same syntax as Flink SQL for DQLs. Refer to
[Flink SQL queries]({{ site.baseurl }}/zh/dev/table/sql/queries.html) for more details. And it's recommended to switch to
`default` dialect to execute DQLs.

## Notice

The following are some precautions for using the Hive dialect.

- Hive dialect should only be used to manipulate Hive tables, not generic tables. And Hive dialect should be used together
with a [HiveCatalog]({{ site.baseurl }}/zh/dev/table/hive/hive_catalog.html).
- While all Hive versions support the same syntax, whether a specific feature is available still depends on the
[Hive version]({{ site.baseurl }}/zh/dev/table/hive/#支持的hive版本) you use. For example, updating database
location is only supported in Hive-2.4.0 or later.
- Hive and Calcite have different sets of reserved keywords. For example, `default` is a reserved keyword in Calcite and
a non-reserved keyword in Hive. Even with Hive dialect, you have to quote such keywords with backtick ( ` ) in order to
use them as identifiers.
- Due to expanded query incompatibility, views created in Flink cannot be queried in Hive.
