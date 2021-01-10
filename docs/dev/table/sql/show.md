---
title: "SHOW Statements"
nav-parent_id: sql
nav-pos: 10
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

* This will be replaced by the TOC
{:toc}

SHOW statements are used to list all catalogs, or list all databases in the current catalog, or list all tables/views in the current catalog and the current database, or show current catalog and database, or list all functions including temp system functions, system functions, temp catalog functions and catalog functions in the current catalog and the current database.

Flink SQL supports the following SHOW statements for now:
- SHOW CATALOGS
- SHOW CURRENT CATALOG
- SHOW DATABASES
- SHOW CURRENT DATABASE
- SHOW TABLES
- SHOW VIEWS
- SHOW FUNCTIONS


## Run a SHOW statement

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

SHOW statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns objects for a successful SHOW operation, otherwise will throw an exception.

The following examples show how to run a SHOW statement in `TableEnvironment`.

</div>

<div data-lang="python" markdown="1">

SHOW statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns objects for a successful SHOW operation, otherwise will throw an exception.

The following examples show how to run a SHOW statement in `TableEnvironment`.

</div>

<div data-lang="SQL CLI" markdown="1">

SHOW statements can be executed in [SQL CLI]({% link dev/table/sqlClient.md %}).

The following examples show how to run a SHOW statement in SQL CLI.

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// show catalogs
tEnv.executeSql("SHOW CATALOGS").print();
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// +-----------------+

// show current catalog
tEnv.executeSql("SHOW CURRENT CATALOG").print();
// +----------------------+
// | current catalog name |
// +----------------------+
// |      default_catalog |
// +----------------------+

// show databases
tEnv.executeSql("SHOW DATABASES").print();
// +------------------+
// |    database name |
// +------------------+
// | default_database |
// +------------------+

// show current database
tEnv.executeSql("SHOW CURRENT DATABASE").print();
// +-----------------------+
// | current database name |
// +-----------------------+
// |      default_database |
// +-----------------------+

// create a table
tEnv.executeSql("CREATE TABLE my_table (...) WITH (...)");
// show tables
tEnv.executeSql("SHOW TABLES").print();
// +------------+
// | table name |
// +------------+
// |   my_table |
// +------------+

// create a view
tEnv.executeSql("CREATE VIEW my_view AS ...");
// show views
tEnv.executeSql("SHOW VIEWS").print();
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show functions
tEnv.executeSql("SHOW FUNCTIONS").print();
// +---------------+
// | function name |
// +---------------+
// |           mod |
// |        sha256 |
// |           ... |
// +---------------+

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// show catalogs
tEnv.executeSql("SHOW CATALOGS").print()
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// +-----------------+

// show databases
tEnv.executeSql("SHOW DATABASES").print()
// +------------------+
// |    database name |
// +------------------+
// | default_database |
// +------------------+

// create a table
tEnv.executeSql("CREATE TABLE my_table (...) WITH (...)")
// show tables
tEnv.executeSql("SHOW TABLES").print()
// +------------+
// | table name |
// +------------+
// |   my_table |
// +------------+

// create a view
tEnv.executeSql("CREATE VIEW my_view AS ...")
// show views
tEnv.executeSql("SHOW VIEWS").print()
// +-----------+
// | view name |
// +-----------+
// |   my_view |
// +-----------+

// show functions
tEnv.executeSql("SHOW FUNCTIONS").print()
// +---------------+
// | function name |
// +---------------+
// |           mod |
// |        sha256 |
// |           ... |
// +---------------+

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

# show catalogs
table_env.execute_sql("SHOW CATALOGS").print()
# +-----------------+
# |    catalog name |
# +-----------------+
# | default_catalog |
# +-----------------+

# show databases
table_env.execute_sql("SHOW DATABASES").print()
# +------------------+
# |    database name |
# +------------------+
# | default_database |
# +------------------+

# create a table
table_env.execute_sql("CREATE TABLE my_table (...) WITH (...)")
# show tables
table_env.execute_sql("SHOW TABLES").print()
# +------------+
# | table name |
# +------------+
# |   my_table |
# +------------+

# create a view
table_env.execute_sql("CREATE VIEW my_view AS ...")
# show views
table_env.execute_sql("SHOW VIEWS").print()
# +-----------+
# | view name |
# +-----------+
# |   my_view |
# +-----------+

# show functions
table_env.execute_sql("SHOW FUNCTIONS").print()
# +---------------+
# | function name |
# +---------------+
# |           mod |
# |        sha256 |
# |           ... |
# +---------------+

{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}

Flink SQL> SHOW CATALOGS;
default_catalog

Flink SQL> SHOW DATABASES;
default_database

Flink SQL> CREATE TABLE my_table (...) WITH (...);
[INFO] Table has been created.

Flink SQL> SHOW TABLES;
my_table

Flink SQL> CREATE VIEW my_view AS ...;
[INFO] View has been created.

Flink SQL> SHOW VIEWS;
my_view

Flink SQL> SHOW FUNCTIONS;
mod
sha256
...

{% endhighlight %}
</div>
</div>

{% top %}

## SHOW CATALOGS

{% highlight sql %}
SHOW CATALOGS
{% endhighlight %}

Show all catalogs.

## SHOW CURRENT CATALOG

{% highlight sql %}
SHOW CURRENT CATALOG
{% endhighlight %}

Show current catalog.

## SHOW DATABASES

{% highlight sql %}
SHOW DATABASES
{% endhighlight %}

Show all databases in the current catalog.

## SHOW CURRENT DATABASE

{% highlight sql %}
SHOW CURRENT DATABASE
{% endhighlight %}

Show current database.

## SHOW TABLES

{% highlight sql %}
SHOW TABLES
{% endhighlight %}

Show all tables in the current catalog and the current database.

## SHOW VIEWS

{% highlight sql %}
SHOW VIEWS
{% endhighlight %}

Show all views in the current catalog and the current database.

## SHOW FUNCTIONS

{% highlight sql %}
SHOW FUNCTIONS
{% endhighlight %}

Show all functions including temp system functions, system functions, temp catalog functions and catalog functions in the current catalog and current database.
