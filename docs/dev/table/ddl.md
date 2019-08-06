---
title: "DDL"
nav-parent_id: tableapi
nav-pos: 0
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

The Table API and SQL are integrated in a joint API. The central concept of this API is a `Table` which serves as input and output of queries. This document shows all the DDL grammar Flink support, how to register a `Table`(or view) through DDL, how to drop a `Table`(or view) through DDL.

* This will be replaced by the TOC
{:toc}

Create Table
---------------------------------------
{% highlight sql %}
CREATE [OR REPLACE] TABLE [catalog_name.][db_name.]table_name
  [(col_name1 col_type1 [COMMENT col_comment1], ...)]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name1, col_name2, ...)]
  [WITH (key1=val1, key2=val2, ...)]
{% endhighlight %}

Create a table with the given table properties. If a table with the same name already exists in the database, an exception is thrown except that *IF NOT EXIST* is declared.

**OR REPLACE**

If a table with the same name already exists in the database, replace it if this is declared. **Notes:** The OR REPLACE option is always false now.

**PARTITIONED BY**

Partition the created table by the specified columns. A directory is created for each partition.

**WITH OPTIONS**

Table properties used to create a table source/sink. The properties are usually used to find and create the underlying connector. **Notes:** the key and value of expression `key1=val1` should both be string literal.

**Examples**:
{% highlight sql %}
-- CREATE a partitioned CSV table using the CREATE TABLE syntax.
create table csv_table (
  f0 int,
  f1 bigint,
  f2 string
) 
COMMENT 'This is a csv table.' 
PARTITIONED BY(f0)
WITH (
  'connector.type' = 'filesystem',
  'format.type' = 'csv',
  'connector.path' = 'path1'
  'format.fields.0.name' = 'f0',
  'format.fields.0.type' = 'INT',
  'format.fields.1.name' = 'f1',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.2.name' = 'f2',
  'format.fields.2.type' = 'STRING',
);

-- CREATE a Kafka table start from the earliest offset(as table source) and append mode(as table sink).
create table kafka_table (
  f0 int,
  f1 bigint,
  f2 string
) with (
  'connector.type' = 'kafka',
  'update-mode' = 'append',
  'connector.topic' = 'topic_name',
  'connector.startup-mode' = 'earliest-offset',
  'connector.properties.0.key' = 'props-key0',
  'connector.properties.0.value' = 'props-val0',
  'format.fields.0.name' = 'f0',
  'format.fields.0.type' = 'INT',
  'format.fields.1.name' = 'f1',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.2.name' = 'f2',
  'format.fields.2.type' = 'STRING'
);

-- CREATE a Elasticsearch table.
create table kafka_table (
  f0 int,
  f1 bigint,
  f2 string
) with (
  'connector.type' = 'elasticsearch',
  'update-mode' = 'append',
  'connector.hosts.0.hostname' = 'host_name',
  'connector.hosts.0.port' = '9092',
  'connector.hosts.0.protocal' = 'http',
  'connector.index' = 'index_name',
  'connector.document-type' = 'type_name',
  'format.fields.0.name' = 'f0',
  'format.fields.0.type' = 'INT',
  'format.fields.1.name' = 'f1',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.2.name' = 'f2',
  'format.fields.2.type' = 'STRING'
);
{% endhighlight %}

{% top %}

Drop Table
---------------------------------------
{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

{% top %}

Create View
---------------------------------------
{% highlight sql %}
CREATE [OR REPLACE] VIEW [catalog_name.][db_name.]view_name
[COMMENT view_comment]
AS
select_statement
{% endhighlight %}

Define a logical view on a sql query which may be from multiple tables or views.

**OR REPLACE**

If the view does not exist, CREATE OR REPLACE VIEW is equivalent to CREATE VIEW. If the view does exist, CREATE OR REPLACE VIEW is equivalent to ALTER VIEW. **Notes:** The OR REPLACE option is always false now.

**AS select_statement**

A SELECT statement that defines the view. The statement can select from base tables or the other views.

**Examples**:
{% highlight sql %}
-- Create a view view_deptDetails in database1. The view definition is recorded in the specified catalog and database.
CREATE VIEW catalog1.database1.view1
  AS SELECT * FROM company JOIN dept ON company.dept_id = dept.id;

-- Create or replace a view from a persistent view with an extra filter
CREATE OR REPLACE VIEW view2
  AS SELECT * FROM catalog1.database1.view1 WHERE loc = 'Shanghai';

-- Access the base tables through the view
SELECT * FROM view2;

-- Drop the view1, view2.
DROP VIEW catalog1.database1.view1;
DROP VIEW IF EXISTS view2;
{% endhighlight %}

{% top %}

Execute DDLs from Table Environment
---------------------------------------
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// create a TableEnvironment
// for batch programs use BatchTableEnvironment instead of StreamTableEnvironment
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// register a Table
tableEnv.sqlUpdate("create table table_name ...")

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment

// create a TableEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// register a Table
tableEnv.sqlUpdate(""create table table_name ..."")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
# for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# create a TableEnvironment
table_env = StreamTableEnvironment.create(env)

# register a Table
table_env.sql_update("create table table_name ...")

{% endhighlight %}
</div>
</div>

**Notes:** The table name can be two format: 1. `catalog_name.db_name.table_name` 2. `table_name`; For `catalog_name.db_name.table_name`, the table would be registered into metastore with catalog named "catalog_name" and database named "db_name", for `table_name`, the table would be registered into the current catalog and database of the execution table environment.

{% top %}

DDL Data Types
---------------------------------------
For DDLs, we support full data types defined in page [Data Types]({{ site.baseurl }}/dev/table/types.html).

**Notes:** Some of the data types are not supported in the sql query(the cast expression or literals). E.G. `STRING`, `BYTES`, `TIME(p) WITHOUT TIME ZONE`, `TIME(p) WITH LOCAL TIME ZONE`, `TIMESTAMP(p) WITHOUT TIME ZONE`, `TIMESTAMP(p) WITH LOCAL TIME ZONE`.

{% top %}
