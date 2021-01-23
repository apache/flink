---
title: "DROP 语句"
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

DROP 语句用于从当前或指定的 [Catalog]({% link dev/table/catalogs.zh.md %}) 中删除一个已经注册的表、视图或函数。

Flink SQL 目前支持以下 DROP 语句：

- DROP TABLE
- DROP DATABASE
- DROP VIEW
- DROP FUNCTION

## 执行 DROP 语句

<div class="codetabs" data-hide-tabs="1" markdown="1">

<div data-lang="java/scala" markdown="1">

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 DROP 语句。 若 DROP 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 DROP 语句。

</div>

<div data-lang="python" markdown="1">

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 DROP 语句。 若 DROP 操作执行成功，`execute_sql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 DROP 语句。

</div>

<div data-lang="SQL CLI" markdown="1">

可以在 [SQL CLI]({% link dev/table/sqlClient.zh.md %}) 中执行 DROP 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 DROP 语句。

</div>
</div>

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// 字符串数组： ["Orders"]
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();

// 从 catalog 删除 “Orders” 表
tableEnv.executeSql("DROP TABLE Orders");

// 空字符串数组
String[] tables = tableEnv.listTables();
// or tableEnv.executeSql("SHOW TABLES").print();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")

// 字符串数组： ["Orders"]
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()

// 从 catalog 删除 “Orders” 表
tableEnv.executeSql("DROP TABLE Orders")

// 空字符串数组
val tables = tableEnv.listTables()
// or tableEnv.executeSql("SHOW TABLES").print()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.new_instance()...
table_env = StreamTableEnvironment.create(env, settings)

# 字符串数组： ["Orders"]
tables = table_env.listTables()
# or table_env.executeSql("SHOW TABLES").print()

# 从 catalog 删除 “Orders” 表
table_env.execute_sql("DROP TABLE Orders")

# 空字符串数组
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

根据给定的表名删除某个表。若需要删除的表不存在，则抛出异常。

**IF EXISTS**

表不存在时不会进行任何操作。

## DROP DATABASE

{% highlight sql %}
DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
{% endhighlight %}

根据给定的表名删除数据库。若需要删除的数据库不存在会抛出异常 。

**IF EXISTS**

若数据库不存在，不执行任何操作。

**RESTRICT**

当删除一个非空数据库时，会触发异常。（默认为开）

**CASCADE**

删除一个非空数据库时，把相关联的表与函数一并删除。

## DROP VIEW

{% highlight sql %}
DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name
{% endhighlight %}

删除一个有 catalog 和数据库命名空间的视图。若需要删除的视图不存在，则会产生异常。

**TEMPORARY**

删除一个有 catalog 和数据库命名空间的临时视图。

**IF EXISTS**

若视图不存在，则不会进行任何操作。

**依赖管理**
Flink 没有使用 CASCADE / RESTRICT 关键字来维护视图的依赖关系，当前的方案是在用户使用视图时再提示错误信息，比如在视图的底层表已经被删除等场景。

## DROP FUNCTION

{% highlight sql%}
DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name;
{% endhighlight %}

删除一个有 catalog 和数据库命名空间的 catalog function。若需要删除的函数不存在，则会产生异常。

**TEMPORARY**

删除一个有 catalog 和数据库命名空间的临时 catalog function。

**TEMPORARY SYSTEM**

删除一个没有数据库命名空间的临时系统函数。

**IF EXISTS**

若函数不存在，则不会进行任何操作。
