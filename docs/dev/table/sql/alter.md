---
title: "ALTER Statements"
nav-parent_id: sql
nav-pos: 4
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

ALTER statements are used to modified a registered table/view/function definition in the [Catalog]({{ site.baseurl }}/dev/table/catalogs.html).

Flink SQL supports the following ALTER statements for now:

- ALTER TABLE
- ALTER DATABASE
- ALTER FUNCTION

## Run an ALTER statement

ALTER statements can be executed with the `sqlUpdate()` method of the `TableEnvironment`, or executed in [SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html). The `sqlUpdate()` method returns nothing for a successful ALTER operation, otherwise will throw an exception.

The following examples show how to run an ALTER statement in `TableEnvironment` and in SQL CLI.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// a string array: ["Orders"]
String[] tables = tableEnv.listTable();

// rename "Orders" to "NewOrders"
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;");

// a string array: ["NewOrders"]
String[] tables = tableEnv.listTable();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// register a table named "Orders"
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// a string array: ["Orders"]
val tables = tableEnv.listTable()

// rename "Orders" to "NewOrders"
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;")

// a string array: ["NewOrders"]
val tables = tableEnv.listTable()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# a string array: ["Orders"]
tables = tableEnv.listTable()

# rename "Orders" to "NewOrders"
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;")

# a string array: ["NewOrders"]
tables = tableEnv.listTable()
{% endhighlight %}
</div>

<div data-lang="SQL CLI" markdown="1">
{% highlight sql %}
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> SHOW TABLES;
Orders

Flink SQL> ALTER TABLE Orders RENAME TO NewOrders;
[INFO] Table has been removed.

Flink SQL> SHOW TABLES;
NewOrders
{% endhighlight %}
</div>
</div>

## ALTER TABLE

* Rename Table

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
{% endhighlight %}

Rename the given table name to another new table name.

* Set or Alter Table Properties

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

Set one or more properties in the specified table. If a particular property is already set in the table, override the old value with the new one.

## ALTER DATABASE

{% highlight sql %}
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

Set one or more properties in the specified database. If a particular property is already set in the database, override the old value with the new one.

## ALTER FUNCTION

{% highlight sql%}
ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION 
  [IF EXISTS] [catalog_name.][db_name.]function_name 
  AS identifier [LANGUAGE JAVA|SCALA|
{% endhighlight %}

Alter a catalog function with the new identifier which is full classpath for JAVA/SCALA and optional language tag. If a function doesn't exist in the catalog, an exception is thrown.

**TEMPORARY**

Alter temporary catalog function that has catalog and database namespaces and overrides catalog functions.

**TEMPORARY SYSTEM**

Alter temporary system function that has no namespace and overrides built-in functions

**IF EXISTS**

If the function doesn't exist, nothing happens.

**LANGUAGE JAVA\|SCALA**

Language tag to instruct flink runtime how to execute the function. Currently only JAVA and SCALA are supported, the default language for a function is JAVA.

