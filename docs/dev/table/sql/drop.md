---
title: "DROP Statements"
nav-parent_id: sql
nav-pos: 3
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

DROP statements are used to remove a registered table/view/function from current or specified [Catalog]({{ site.baseurl }}/dev/table/catalogs.html).

Flink SQL supports the following DROP statements for now:

- DROP TABLE
- DROP DATABASE
- DROP VIEW
- DROP FUNCTION

## Run a DROP statement

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

DROP statements can be executed with the `executeSql()` method of the `TableEnvironment`. The `executeSql()` method returns 'OK' for a successful DROP operation, otherwise will throw an exception.

The following examples show how to run a DROP statement in `TableEnvironment`.

</div>

<div data-lang="python" markdown="1">

DROP statements can be executed with the `execute_sql()` method of the `TableEnvironment`. The `execute_sql()` method returns 'OK' for a successful DROP operation, otherwise will throw an exception.

The following examples show how to run a DROP statement in `TableEnvironment`.

</div>

<div data-lang="SQL CLI" markdown="1">

DROP statements can be in [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html).

The following examples show how to run a DROP statement in SQL CLI.

</div>
</div>
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// a string array: ["Orders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();

// drop "Orders" table from catalog
tableEnv.executeSql("DROP TABLE Orders");

// an empty string array
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// register a table named "Orders"
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// a string array: ["Orders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()

// drop "Orders" table from catalog
tableEnv.executeSql("DROP TABLE Orders")

// an empty string array
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

# a string array: ["Orders"]
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()

# drop "Orders" table from catalog
table_env.execute_sql("DROP TABLE Orders")

# an empty string array
tables = table_env.list_tables()
# or table_env.execute_sql("SHOW TABLES").print()
{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> SHOW TABLES;
Orders

Flink SQL> DROP TABLE Orders;
[INFO] Table has been removed.

Flink SQL> SHOW TABLES;
[INFO] Result was empty.
{% endhighlight %}
</div>
</div>

## DROP TABLE

{% highlight sql %}
DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
{% endhighlight %}

Drop a table with the given table name. If the table to drop does not exist, an exception is thrown.

**IF EXISTS**

If the table does not exist, nothing happens.

## DROP DATABASE

{% highlight sql %}
DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
{% endhighlight %}

Drop a database with the given database name. If the database to drop does not exist, an exception is thrown.

**IF EXISTS**

If the database does not exist, nothing happens.

**RESTRICT**

Dropping a non-empty database triggers an exception. Enabled by default.

**CASCADE**

Dropping a non-empty database also drops all associated tables and functions.

## DROP VIEW

{% highlight sql %}
DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name
{% endhighlight %}

Drop a view that has catalog and database namespaces. If the view to drop does not exist, an exception is thrown.

**TEMPORARY**

Drop temporary view that has catalog and database namespaces.

**IF EXISTS**

If the view does not exist, nothing happens.

**MAINTAIN DEPENDENCIES**
Flink does not maintain dependencies of view by CASCADE/RESTRICT keywords, the current way is producing postpone error message when user tries to use the view under the scenarios like the underlying table of view has been dropped.

## DROP FUNCTION

{% highlight sql%}
DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name;
{% endhighlight %}

Drop a catalog function that has catalog and database namespaces. If the function to drop does not exist, an exception is thrown.

**TEMPORARY**

Drop temporary catalog function that has catalog and database namespaces.

**TEMPORARY SYSTEM**

Drop temporary system function that has no namespace.

**IF EXISTS**

If the function doesn't exists, nothing happens.
