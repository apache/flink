---
title: "SHOW 语句"
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

SHOW 语句用于列出所有的 catalog，或者列出当前 catalog 中所有的 database，或者列出当前 catalog 和当前 database 的所有表或视图，或者列出当前正在使用的 catalog 和 database, 或者列出所有的 function，包括：临时系统 function，系统 function，临时 catalog function，当前 catalog 和 database 中的 catalog function。

目前 Flink SQL 支持下列 SHOW 语句：
- SHOW CATALOGS
- SHOW CURRENT CATALOG
- SHOW DATABASES
- SHOW CURRENT DATABASE
- SHOW TABLES
- SHOW VIEWS
- SHOW FUNCTIONS


## 执行 SHOW 语句

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 SHOW 语句。 若 SHOW 操作执行成功，`executeSql()` 方法返回所有对象，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 SHOW 语句。

</div>

<div data-lang="python" markdown="1">

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 SHOW 语句。 若 SHOW 操作执行成功，`execute_sql()` 方法返回所有对象，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 SHOW 语句。

</div>

<div data-lang="SQL CLI" markdown="1">

可以在 [SQL CLI]({{ site.baseurl }}/zh/dev/table/sqlClient.html) 中执行 SHOW 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 SHOW 语句。

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

展示所有的 catalog。

## SHOW CURRENT CATALOG

{% highlight sql %}
SHOW CURRENT CATALOG
{% endhighlight %}

显示当前正在使用的 catalog。

## SHOW DATABASES

{% highlight sql %}
SHOW DATABASES
{% endhighlight %}

展示当前 catalog 中所有的 database。

## SHOW CURRENT DATABASE

{% highlight sql %}
SHOW CURRENT DATABASE
{% endhighlight %}

显示当前正在使用的 database。

## SHOW TABLES

{% highlight sql %}
SHOW TABLES
{% endhighlight %}

展示当前 catalog 和当前 database 中所有的表。

## SHOW VIEWS

{% highlight sql %}
SHOW VIEWS
{% endhighlight %}

展示当前 catalog 和当前 database 中所有的视图。

## SHOW FUNCTIONS

{% highlight sql %}
SHOW FUNCTIONS
{% endhighlight %}

展示所有的 function，包括：临时系统 function, 系统 function, 临时 catalog function，当前 catalog 和 database 中的 catalog function。
