---
title: "ALTER 语句"
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

ALTER 语句用于修改一个已经在 [Catalog]({{ site.baseurl }}/zh/dev/table/catalogs.html) 中注册的表、视图或函数定义。

Flink SQL 目前支持以下 ALTER 语句：

- ALTER TABLE
- ALTER DATABASE
- ALTER FUNCTION

## 执行 ALTER 语句

可以使用 `TableEnvironment` 中的 `sqlUpdate()` 方法执行 ALTER 语句，也可以在 [SQL CLI]({{ site.baseurl }}/zh/dev/table/sqlClient.html) 中执行 ALTER 语句。 若 ALTER 操作执行成功，`sqlUpdate()` 方法不返回任何内容，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 和  SQL CLI 中执行一个 ALTER 语句。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// 注册名为 “Orders” 的表
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// 字符串数组： ["Orders"]
String[] tables = tableEnv.listTable();

// 把 “Orders” 的表名改为 “NewOrders”
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;");

// 字符串数组：["NewOrders"]
String[] tables = tableEnv.listTable();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val settings = EnvironmentSettings.newInstance()...
val tableEnv = TableEnvironment.create(settings)

// 注册名为 “Orders” 的表
tableEnv.sqlUpdate("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");

// 字符串数组： ["Orders"]
val tables = tableEnv.listTable()

// 把 “Orders” 的表名改为 “NewOrders”
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;")

// 字符串数组：["NewOrders"]
val tables = tableEnv.listTable()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
settings = EnvironmentSettings.newInstance()...
table_env = TableEnvironment.create(settings)

# 字符串数组： ["Orders"]
tables = tableEnv.listTable()

# 把 “Orders” 的表名改为 “NewOrders”
tableEnv.sqlUpdate("ALTER TABLE Orders RENAME TO NewOrders;")

# 字符串数组：["NewOrders"]
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

* 重命名表

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
{% endhighlight %}

把原有的表名更改为新的表名。

* 设置或修改表属性

{% highlight sql %}
ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

为指定的表设置一个或多个属性。若个别属性已经存在于表中，则使用新的值覆盖旧的值。

## ALTER DATABASE

{% highlight sql %}
ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
{% endhighlight %}

在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。

## ALTER FUNCTION

{% highlight sql%}
ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION
  [IF EXISTS] [catalog_name.][db_name.]function_name
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
{% endhighlight %}

修改一个有 catalog 和数据库命名空间的 catalog function ，需要指定一个新的 identifier ，可指定 language tag 。若函数不存在，删除会抛出异常。

如果 language tag 是 JAVA 或者 SCALA ，则 identifier 是 UDF 实现类的全限定名。关于 JAVA/SCALA UDF 的实现，请参考 [自定义函数]({{ site.baseurl }}/zh/dev/table/functions/udfs.html)。

如果 language tag 是 PYTHON ， 则 identifier 是 UDF 对象的全限定名，例如 `pyflink.table.tests.test_udf.add`。关于 PYTHON UDF 的实现，请参考 [Python UDFs]({{ site.baseurl }}/zh/dev/table/python/python_udfs.html)。

**TEMPORARY**

修改一个有 catalog 和数据库命名空间的临时 catalog function ，并覆盖原有的 catalog function 。

**TEMPORARY SYSTEM**

修改一个没有数据库命名空间的临时系统 catalog function ，并覆盖系统内置的函数。

**IF EXISTS**

若函数不存在，则不进行任何操作。

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag 用于指定 Flink runtime 如何执行这个函数。目前，只支持 JAVA，SCALA 和 PYTHON，且函数的默认语言为 JAVA。
