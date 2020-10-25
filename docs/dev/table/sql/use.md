---
title: "USE Statements"
nav-parent_id: sql
nav-pos: 9
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

USE statements are used to set the current database or catalog.


## Run a USE statement

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

USE statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful USE operation, otherwise will throw an exception.

The following examples show how to run a USE statement in `TableEnvironment`.

</div>

<div data-lang="python" markdown="1">

USE statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns 'OK' for a successful USE operation, otherwise will throw an exception.

The following examples show how to run a USE statement in `TableEnvironment`.

</div>

<div data-lang="SQL CLI" markdown="1">

USE statements can be executed in [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html).

The following examples show how to run a USE statement in SQL CLI.

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// create a catalog
tEnv.executeSql("CREATE CATALOG cat1 WITH (...)");
tEnv.executeSql("SHOW CATALOGS").print();
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// | cat1            |
// +-----------------+

// change default catalog
tEnv.executeSql("USE CATALOG cat1");

tEnv.executeSql("SHOW DATABASES").print();
// databases are empty
// +---------------+
// | database name |
// +---------------+
// +---------------+

// create a database
tEnv.executeSql("CREATE DATABASE db1 WITH (...)");
tEnv.executeSql("SHOW DATABASES").print();
// +---------------+
// | database name |
// +---------------+
// |        db1    |
// +---------------+

// change default database
tEnv.executeSql("USE db1");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
val tEnv = StreamTableEnvironment.create(env)

// create a catalog
tEnv.executeSql("CREATE CATALOG cat1 WITH (...)")
tEnv.executeSql("SHOW CATALOGS").print()
// +-----------------+
// |    catalog name |
// +-----------------+
// | default_catalog |
// | cat1            |
// +-----------------+

// change default catalog
tEnv.executeSql("USE CATALOG cat1")

tEnv.executeSql("SHOW DATABASES").print()
// databases are empty
// +---------------+
// | database name |
// +---------------+
// +---------------+

// create a database
tEnv.executeSql("CREATE DATABASE db1 WITH (...)")
tEnv.executeSql("SHOW DATABASES").print()
// +---------------+
// | database name |
// +---------------+
// |        db1    |
// +---------------+

// change default database
tEnv.executeSql("USE db1")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

# create a catalog
table_env.execute_sql("CREATE CATALOG cat1 WITH (...)")
table_env.execute_sql("SHOW CATALOGS").print()
# +-----------------+
# |    catalog name |
# +-----------------+
# | default_catalog |
# | cat1            |
# +-----------------+

# change default catalog
table_env.execute_sql("USE CATALOG cat1")

table_env.execute_sql("SHOW DATABASES").print()
# databases are empty
# +---------------+
# | database name |
# +---------------+
# +---------------+

# create a database
table_env.execute_sql("CREATE DATABASE db1 WITH (...)")
table_env.execute_sql("SHOW DATABASES").print()
# +---------------+
# | database name |
# +---------------+
# |           db1 |
# +---------------+

# change default database
table_env.execute_sql("USE db1")

{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE CATALOG cat1 WITH (...);
[INFO] Catalog has been created.

Flink SQL> SHOW CATALOGS;
default_catalog
cat1

Flink SQL> USE CATALOG cat1;

Flink SQL> SHOW DATABASES;

Flink SQL> CREATE DATABASE db1 WITH (...);
[INFO] Database has been created.

Flink SQL> SHOW DATABASES;
db1

Flink SQL> USE db1;

{% endhighlight %}
</div>
</div>

{% top %}

## USE CATLOAG

{% highlight sql %}
USE CATALOG catalog_name
{% endhighlight %}

Set the current catalog. All subsequent commands that do not explicitly specify a catalog will use this one. If the provided catalog does not exist, an exception is thrown. The default current catalog is `default_catalog`.


## USE

{% highlight sql %}
USE [catalog_name.]database_name
{% endhighlight %}

Set the current database. All subsequent commands that do not explicitly specify a database will use this one. If the provided database does not exist, an exception is thrown. The default current database is `default_database`.
