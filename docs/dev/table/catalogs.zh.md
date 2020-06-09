---
title: "Catalogs"
nav-parent_id: tableapi
nav-pos: 80
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

Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。

数据处理最关键的方面之一是管理元数据。
元数据可以是临时的，例如临时表、或者通过 TableEnvironment 注册的 UDF。
元数据也可以是持久化的，例如 Hive Metastore 中的元数据。Catalog 提供了一个统一的API，用于管理元数据，并使其可以从 Table API 和 SQL 查询语句中来访问。

* This will be replaced by the TOC
{:toc}

## Catalog 类型

### GenericInMemoryCatalog

`GenericInMemoryCatalog` 是基于内存实现的 Catalog，所有元数据只在 session 的生命周期内可用。

### JdbcCatalog

The `JdbcCatalog` enables users to connect Flink to relational databases over JDBC protocol.

#### PostgresCatalog

`PostgresCatalog` is the only implementation of JDBC Catalog at the moment.

#### Usage of JdbcCatalog

Set a `Jdbcatalog` with the following parameters:

- name: required, name of the catalog
- default database: required, default database to connect to
- username: required, username of Postgres account
- password: required, password of the account
- base url: required, should be of format "jdbc:postgresql://<ip>:<port>", and should not contain database name here

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name            = "mypg";
String defaultDatabase = "mydb";
String username        = "...";
String password        = "...";
String baseUrl         = "..."

JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
tableEnv.registerCatalog("mypg", catalog);

// set the JdbcCatalog as the current catalog of the session
tableEnv.useCatalog("mypg");
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">
{% highlight scala %}

val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
val tableEnv = TableEnvironment.create(settings)

val name            = "mypg"
val defaultDatabase = "mydb"
val username        = "..."
val password        = "..."
val baseUrl         = "..."

val catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl)
tableEnv.registerCatalog("mypg", catalog)

// set the JdbcCatalog as the current catalog of the session
tableEnv.useCatalog("mypg")
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE CATALOG mypg WITH(
    'type'='jdbc',
    'default-database'='...',
    'username'='...',
    'password'='...',
    'base-url'='...'
);

USE CATALOG mypg;
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
{% highlight yaml %}

execution:
    planner: blink
    ...
    current-catalog: mypg  # set the JdbcCatalog as the current catalog of the session
    current-database: mydb
    
catalogs:
   - name: mypg
     type: jdbc
     default-database: mydb
     username: ...
     password: ...
     base-url: ...
{% endhighlight %}
</div>
</div>


### HiveCatalog

`HiveCatalog` 有两个用途：作为原生 Flink 元数据的持久化存储，以及作为读写现有 Hive 元数据的接口。 
Flink 的 [Hive 文档]({{ site.baseurl }}/zh/dev/table/hive/index.html) 提供了有关设置 `HiveCatalog` 以及访问现有 Hive 元数据的详细信息。


<span class="label label-danger">警告</span> Hive Metastore 以小写形式存储所有元数据对象名称。而 `GenericInMemoryCatalog` 区分大小写。

### 用户自定义 Catalog

Catalog 是可扩展的，用户可以通过实现 `Catalog` 接口来开发自定义 Catalog。
想要在 SQL CLI 中使用自定义 Catalog，用户除了需要实现自定义的 Catalog 之外，还需要为这个 Catalog 实现对应的 `CatalogFactory` 接口。

`CatalogFactory` 定义了一组属性，用于 SQL CLI 启动时配置 Catalog。
这组属性集将传递给发现服务，在该服务中，服务会尝试将属性关联到 `CatalogFactory` 并初始化相应的 Catalog 实例。

## 如何创建 Flink 表并将其注册到 Catalog

### 使用 SQL DDL

用户可以使用 DDL 通过 Table API 或者 SQL Client 在 Catalog 中创建表。

使用 Table API：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableEnvironment tableEnv = ...

// Create a HiveCatalog 
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>", "<hive_version>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
tableEnv.sqlUpdate("CREATE DATABASE mydb WITH (...)");

// Create a catalog table
tableEnv.sqlUpdate("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

tableEnv.listTables(); // should return the tables in current catalog and database.

{% endhighlight %}
</div>
</div>

使用 SQL Client：

{% highlight sql %}
// the catalog should have been registered via yaml file
Flink SQL> CREATE DATABASE mydb WITH (...);

Flink SQL> CREATE TABLE mytable (name STRING, age INT) WITH (...);

Flink SQL> SHOW TABLES;
mytable
{% endhighlight %}

更多详细信息，请参考[Flink SQL CREATE DDL]({{ site.baseurl }}/zh/dev/table/sql/create.html)。

### 使用 Java/Scala/Python API

用户可以用编程的方式使用Java、Scala 或者 Python API 来创建 Catalog 表。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableEnvironment tableEnv = ...

// Create a HiveCatalog
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>", "<hive_version>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...))

// Create a catalog table
TableSchema schema = TableSchema.builder()
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .build();

catalog.createTable(
        new ObjectPath("mydb", "mytable"),
        new CatalogTableImpl(
            schema,
            new Kafka()
                .version("0.11")
                ....
                .startFromEarlist(),
            "my comment"
        )
    );

List<String> tables = catalog.listTables("mydb"); // tables should contain "mytable"
{% endhighlight %}

</div>
</div>

## Catalog API

注意：这里只列出了编程方式的 Catalog API，用户可以使用 SQL DDL 实现许多相同的功能。
关于 DDL 的详细信息请参考 [SQL CREATE DDL]({{ site.baseurl }}/zh/dev/table/sql/create.html)。


### 数据库操作

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}
// create database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);

// drop database
catalog.dropDatabase("mydb", false);

// alter database
catalog.alterDatabase("mydb", new CatalogDatabaseImpl(...), false);

// get databse
catalog.getDatabase("mydb");

// check if a database exist
catalog.databaseExists("mydb");

// list databases in a catalog
catalog.listDatabases("mycatalog");
{% endhighlight %}
</div>
</div>

### 表操作

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}
// create table
catalog.createTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);

// drop table
catalog.dropTable(new ObjectPath("mydb", "mytable"), false);

// alter table
catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogTableImpl(...), false);

// rename table
catalog.renameTable(new ObjectPath("mydb", "mytable"), "my_new_table");

// get table
catalog.getTable("mytable");

// check if a table exist or not
catalog.tableExists("mytable");

// list tables in a database
catalog.listTables("mydb");
{% endhighlight %}
</div>
</div>

### 视图操作

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}
// create view
catalog.createTable(new ObjectPath("mydb", "myview"), new CatalogViewImpl(...), false);

// drop view
catalog.dropTable(new ObjectPath("mydb", "myview"), false);

// alter view
catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogViewImpl(...), false);

// rename view
catalog.renameTable(new ObjectPath("mydb", "myview"), "my_new_view");

// get view
catalog.getTable("myview");

// check if a view exist or not
catalog.tableExists("mytable");

// list views in a database
catalog.listViews("mydb");
{% endhighlight %}
</div>
</div>


### 分区操作

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}
// create view
catalog.createPartition(
    new ObjectPath("mydb", "mytable"),
    new CatalogPartitionSpec(...),
    new CatalogPartitionImpl(...),
    false);

// drop partition
catalog.dropPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...), false);

// alter partition
catalog.alterPartition(
    new ObjectPath("mydb", "mytable"),
    new CatalogPartitionSpec(...),
    new CatalogPartitionImpl(...),
    false);

// get partition
catalog.getPartition(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// check if a partition exist or not
catalog.partitionExists(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// list partitions of a table
catalog.listPartitions(new ObjectPath("mydb", "mytable"));

// list partitions of a table under a give partition spec
catalog.listPartitions(new ObjectPath("mydb", "mytable"), new CatalogPartitionSpec(...));

// list partitions of a table by expression filter
catalog.listPartitions(new ObjectPath("mydb", "mytable"), Arrays.asList(epr1, ...));
{% endhighlight %}
</div>
</div>


### 函数操作

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}
// create function
catalog.createFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);

// drop function
catalog.dropFunction(new ObjectPath("mydb", "myfunc"), false);

// alter function
catalog.alterFunction(new ObjectPath("mydb", "myfunc"), new CatalogFunctionImpl(...), false);

// get function
catalog.getFunction("myfunc");

// check if a function exist or not
catalog.functionExists("myfunc");

// list functions in a database
catalog.listFunctions("mydb");
{% endhighlight %}
</div>
</div>


## 通过 Table API 和 SQL Client 操作 Catalog

### 注册 Catalog

用户可以访问默认创建的内存 Catalog `default_catalog`，这个 Catalog 默认拥有一个默认数据库 `default_database`。
用户也可以注册其他的 Catalog 到现有的 Flink 会话中。

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.registerCatalog(new CustomCatalog("myCatalog"));
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">

使用 YAML 定义的 Catalog 必须提供 `type` 属性，以表示指定的 Catalog 类型。
以下几种类型可以直接使用。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">Catalog</th>
      <th class="text-center">Type Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td class="text-center">GenericInMemory</td>
        <td class="text-center">generic_in_memory</td>
    </tr>
    <tr>
        <td class="text-center">Hive</td>
        <td class="text-center">hive</td>
    </tr>
  </tbody>
</table>

{% highlight yaml %}
catalogs:
   - name: myCatalog
     type: custom_catalog
     hive-conf-dir: ...
{% endhighlight %}
</div>
</div>

### 修改当前的 Catalog 和数据库

Flink 始终在当前的 Catalog 和数据库中寻找表、视图和 UDF。 

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.useCatalog("myCatalog");
tableEnv.useDatabase("myDb");
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> USE CATALOG myCatalog;
Flink SQL> USE myDB;
{% endhighlight %}
</div>
</div>

通过提供全限定名 `catalog.database.object` 来访问不在当前 Catalog 中的元数据信息。

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.from("not_the_current_catalog.not_the_current_db.my_table");
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> SELECT * FROM not_the_current_catalog.not_the_current_db.my_table;
{% endhighlight %}
</div>
</div>

### 列出可用的 Catalog

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.listCatalogs();
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> show catalogs;
{% endhighlight %}
</div>
</div>


### 列出可用的数据库

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.listDatabases();
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> show databases;
{% endhighlight %}
</div>
</div>

### 列出可用的表

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.listTables();
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}
Flink SQL> show tables;
{% endhighlight %}
</div>
</div>
