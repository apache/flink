---
title: "Catalog"
is_beta: true
nav-parent_id: tableapi
nav-pos: 100
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

<<<<<<< HEAD
Catalog提供元数据，例如名称，模式，表的统计信息以及有关如何访问存储在数据库或其他外部系统中的数据。 一旦目录在`TableEnvironment`中注册，就可以从Table API和SQL查询中访问其所有元对象。
=======
Catalogs provide metadata, such as names, schemas, statistics of tables, and information about how to access data stored in a database or other external systems. Once a catalog is registered within a `TableEnvironment`, all its meta-objects are accessible from the Table API and SQL queries.

>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

* This will be replaced by the TOC
{:toc}


<<<<<<< HEAD
Catalog接口
-----------------

API在`Catalog`接口中被定义。 该接口定义了一组API，用于读取和写入目录元对象，如数据库，表，分区，视图和函数。


Catalog元对象命名结构
-------------------------------------

Flink的catalog使用严格的两级结构，即catalog包含数据库，数据库包含元对象。 因此，元对象的全名总是被构造为`catalogName`.`databaseName`.`objectName`。


每个`TableEnvironment`都有一个`CatalogManager`来管理所有已注册的catalog。 为了便于访问元对象，`CatalogManager`具有当前目录和当前数据库的概念。 通过设置当前目录和当前数据库，用户可以在查询中仅使用元对象的名称。 这极大地简化了用户体验。

例如，以前的查询为
=======
Catalog Interface
-----------------

APIs are defined in `Catalog` interface. The interface defines a set of APIs to read and write catalog meta-objects such as database, tables, partitions, views, and functions.


Catalog Meta-Objects Naming Structure
-------------------------------------

Flink's catalogs use a strict two-level structure, that is, catalogs contain databases, and databases contain meta-objects. Thus, the full name of a meta-object is always structured as `catalogName`.`databaseName`.`objectName`.

Each `TableEnvironment` has a `CatalogManager` to manager all registered catalogs. To ease access to meta-objects, `CatalogManager` has a concept of current catalog and current database. By setting current catalog and current database, users can use just the meta-object's name in their queries. This greatly simplifies user experience.

For example, a previous query as
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```sql
select * from mycatalog.mydb.myTable;
```

<<<<<<< HEAD
可以缩短为
=======
can be shortened to
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```sql
select * from myTable;
```

<<<<<<< HEAD
要查询当前catalog下的其他数据库中的表，用户无需指定catalog名称。 在我们的例子中，它将是
=======
To querying tables in a different database under the current catalog, users don't need to specify the catalog name. In our example, it would be
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```
select * from mydb2.myTable2
```

<<<<<<< HEAD
`CatalogManager`总是有一个名为`default_catalog`的内置`GenericInMemoryCatalog`，它有一个名为`default_database`的内置默认数据库。 如果没有显式设置其他目录和数据库，则默认情况下它们将会使用当前目录和当前数据库。 所有临时元对象（例如由`TableEnvironment#registerTable`定义的对象）都将注册到此目录中。


在FLINK SQL中，用户可以通过Table API中的`TableEnvironment.useCatalog（...）`和`TableEnvironment.useDatabase（...）`，或`USE CATALOG ...`和`USE DATABASE ...`来设置当前目录和数据库。


Catalog的类型
=======
`CatalogManager` always has a built-in `GenericInMemoryCatalog` named `default_catalog`, which has a built-in default database named `default_database`. If no other catalog and database are explicitly set, they will be the current catalog and current database by default. All temp meta-objects, such as those defined by `TableEnvironment#registerTable`  are registered to this catalog. 

Users can set current catalog and database via `TableEnvironment.useCatalog(...)` and `TableEnvironment.useDatabase(...)` in Table API, or `USE CATALOG ...` and `USE DATABASE ...` in Flink SQL.


Catalog Types
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6
-------------

## GenericInMemoryCatalog

<<<<<<< HEAD
默认目录; 此目录中的所有元对象都存储在内存中，并且会话关闭后将丢失。

SQL CLI yaml文件中的配置条目为“generic_in_memory”。

## HiveCatalog

Flink的`HiveCatalog`可以使用Hive Metastore作为持久存储来读写Flink和Hive元对象。

它在SQL CLI yaml文件中的配置条目是"hive"。

### 持久化Flink的元对象

从历史上看，Flink元对象是基于会话的，信息仅存储在内存中。 这意味着用户每次开始新会话时都必须重新创建所有元对象。

为了跨会话维护元对象，用户可以选择使用`HiveCatalog`来持久保存所有用户的Flink流（无界流）和批量（有界流）元对象。 由于Hive Metastore仅用于存储，因此Hive本身可能无法理解存储在Metastore中的Flink的元对象。

### Flink与Hive元数据集成

将Flink与Hive元数据集成的最终目标是：

1. Flink可以使用由Hive或其他与Hive兼容的应用程序创建的现有元对象，如表，视图和函数

2. 由HiveCatalog创建的元对象可以写回Hive Metastore，以便Hive和其他Hive兼容的应用程序可以使用。

## 用户配置的Catalog

目录是可插拔的。 用户可以通过实现`Catalog`接口来开发自定义目录，该接口定义了一组用于读取和编写目录元对象（如数据库，表，分区，视图和函数）的API。
=======
The default catalog; all meta-objects in this catalog are stored in memory, and be will be lost once the session shuts down.

Its config entry value in SQL CLI yaml file is "generic_in_memory".

## HiveCatalog

Flink's `HiveCatalog` can read and write both Flink and Hive meta-objects using Hive Metastore as persistent storage.

Its config entry value in SQL CLI yaml file is "hive".

### Persist Flink meta-objects

Historically, Flink meta-objects are only stored in memory and are per session based. That means users have to recreate all the meta-objects every time they start a new session.

To maintain meta-objects across sessions, users can choose to use `HiveCatalog` to persist all of users' Flink streaming (unbounded-stream) and batch (bounded-stream) meta-objects. Because Hive Metastore is only used for storage, Hive itself may not understand Flink's meta-objects stored in the metastore.

### Integrate Flink with Hive metadata

The ultimate goal for integrating Flink with Hive metadata is that:

1. Existing meta-objects, like tables, views, and functions, created by Hive or other Hive-compatible applications can be used by Flink

2. Meta-objects created by `HiveCatalog` can be written back to Hive metastore such that Hive and other Hive-compatible applications can consume.

## User-configured Catalog

Catalogs are pluggable. Users can develop custom catalogs by implementing the `Catalog` interface, which defines a set of APIs for reading and writing catalog meta-objects such as database, tables, partitions, views, and functions.
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6


HiveCatalog
-----------

<<<<<<< HEAD
## 支持的Hive版本

Flink的`HiveCatalog`正式支持Hive 2.3.4和1.2.1。

Hive版本显式指定为String，可以通过在Table API中直接创建`HiveCatalog`实例或在SQL CLI中的yaml配置文件中指定它来将其传递给构造函数。 Hive版本字符串是`2.3.4`和`1.2.1`。

## 不区分元对象名称的大小写

请注意，Hive Metastore在较小的情况下存储元对象名称。 因此，与`GenericInMemoryCatalog`不同，`HiveCatalog`对元对象名称不区分大小写，用户需要对此保持谨慎。

## 依赖

要使用`HiveCatalog`，用户需要包含以下依赖jar。

对于Hive 2.3.4，用户需要：
=======
## Supported Hive Versions

Flink's `HiveCatalog` officially supports Hive 2.3.4 and 1.2.1.

The Hive version is explicitly specified as a String, either by passing it to the constructor when creating `HiveCatalog` instances directly in Table API or specifying it in yaml config file in SQL CLI. The Hive version string are `2.3.4` and `1.2.1`.

## Case Insensitive to Meta-Object Names

Note that Hive Metastore stores meta-object names in lower cases. Thus, unlike `GenericInMemoryCatalog`, `HiveCatalog` is case-insensitive to meta-object names, and users need to be cautious on that.

## Dependencies

To use `HiveCatalog`, users need to include the following dependency jars.

For Hive 2.3.4, users need:
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```
// Hive dependencies

- hive-exec-2.3.4.jar // contains hive-metastore-2.3.4


// Hadoop dependencies
- flink-shaded-hadoop-2-uber-2.7.5-1.8.0.jar
- flink-hadoop-compatibility-{{site.version}}.jar

```

<<<<<<< HEAD
对于Hive 1.2.1，用户需要：
=======
For Hive 1.2.1, users need:
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```
// Hive dependencies

- hive-metastore-1.2.1.jar
- hive-exec-1.2.1.jar


// Hadoop dependencies
- flink-shaded-hadoop-2-uber-2.6.5-1.8.0.jar
- flink-hadoop-compatibility-{{site.version}}.jar

```

<<<<<<< HEAD
如果您手头没有Hive依赖，可以在以下位置找到它们 [mvnrepostory.com](https://mvnrepository.com):
=======
If you don't have Hive dependencies at hand, they can be found at [mvnrepostory.com](https://mvnrepository.com):
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

- [hive-exec](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)
- [hive-metastore](https://mvnrepository.com/artifact/org.apache.hive/hive-metastore)

<<<<<<< HEAD
请注意，用户需要确保其Hive版本与Hadoop版本之间的兼容性。 否则，可能存在潜在问题，例如，Hive 2.3.4是针对Hadoop 2.7.2编译的，当使用Hive 2.3.4和Hadoop 2.4时可能会遇到问题。


## 数据类型映射

对于Flink和Hive表，`HiveCatalog`通过将表映射到具有Hive数据类型的Hive表模式来存储表模式。 类型在读取时动态映射回来。

目前，`HiveCatalog`支持大多数Flink数据类型，具有以下映射：
=======
Note that users need to make sure the compatibility between their Hive versions and Hadoop versions. Otherwise, there may be potential problem, for example, Hive 2.3.4 is compiled against Hadoop 2.7.2, you may run into problems when using Hive 2.3.4 with Hadoop 2.4.


## Data Type Mapping

For both Flink and Hive tables, `HiveCatalog` stores table schemas by mapping them to Hive table schemas with Hive data types. Types are dynamically mapped back on read.

Currently `HiveCatalog` supports most Flink data types with the following mapping:
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

|  Flink Data Type  |  Hive Data Type  |
|---|---|
| CHAR(p)       |  char(p)* |
| VARCHAR(p)    |  varchar(p)** |
| STRING        |  string |
| BOOLEAN       |  boolean |
| BYTE          |  tinyint |
| SHORT         |  smallint |
| INT           |  int |
| BIGINT        |  long |
| FLOAT         |  float |
| DOUBLE        |  double |
| DECIMAL(p, s) |  decimal(p, s) |
| DATE          |  date |
| TIMESTAMP_WITHOUT_TIME_ZONE |  Timestamp |
| TIMESTAMP_WITH_TIME_ZONE |  N/A |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE |  N/A |
| INTERVAL |  N/A |
| BINARY        |  binary |
| VARBINARY(p)  |  binary |
| ARRAY\<E>     |  list\<E> |
| MAP<K, V>     |  map<K, V> |
| ROW           |  struct |
| MULTISET      |  N/A |


<<<<<<< HEAD
请注意，我们现在只涵盖最常用的数据类型。

Hive的数据类型中的以下限制会影响Flink和Hive之间的映射：

\* 最小长度 255

\* 最大长度 65535

## Hive兼容性

有关Hive兼容性和版本，请参阅 [Hive Compatibility]({{ site.baseurl }}/dev/batch/hive_compatibility.html)


目录注册
=======
Note that we only cover most commonly used data types for now.

The following limitations in Hive's data types impact the mapping between Flink and Hive:

\* maximum length is 255

\** maximum length is 65535


Catalog Registration
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6
--------------------

## Register Catalog in Table API

<<<<<<< HEAD
要在Table API中注册目录，用户可以创建目录实例并通过`TableEnvironment.registerCatalog（name，catalog）`注册它。

## Register Catalog in SQL CLI

要在SQL CLI中使用预定义的目录（`GenericInMemoryCatalog`和`HiveCatalog`），请参阅 [SQL Clinet]({{ site.baseurl }}/dev/table/sqlClient.html)

要在SQL CLI中使用自定义目录，用户应分别通过实现`Catalog`和`CatalogFactory`接口来开发目录及其相应的目录工厂。

目录工厂定义了一组属性用于在SQL CLI引导时配置目录。 该组属性将传递给发现服务，这些服务会尝试将属性与`CatalogFactory`匹配并启动相应的目录实例。
=======
To register a catalog in Table API, users can create a catalog instance and register it through `TableEnvironment.registerCatalog(name, catalog)`.

## Register Catalog in SQL CLI

To use pre-defined catalogs (`GenericInMemoryCatalog` and `HiveCatalog`) in SQL CLI, please refer to [SQL Clinet]({{ site.baseurl }}/dev/table/sqlClient.html)

To use custom catalogs in SQL CLI, users should develop both a catalog and its corresponding catalog factory by implementing `Catalog` and `CatalogFactory` interfaces respectively.

The catalog factory defines a set of properties for configuring the catalog when SQL CLI bootstraps. The set of properties will be passed to a discovery service where the service tries to match the properties to a `CatalogFactory` and initiate an corresponding catalog instance.
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6


{% top %}

<<<<<<< HEAD
目录模块
---------------

`GenericInMemoryCatalog`内置于Flink的Table API中。

要在Flink Table API和SQL中使用`HiveCatalog`，用户需要在他们的项目中包含`flink-connector-hive` jar。

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-hive_{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version}}</version>
=======
Catalog Modules
---------------

`GenericInMemoryCatalog` is built into Flink's Table API.

To use `HiveCatalog` in Flink Table API and SQL, users need to include `flink-connector-hive` jar in their projects.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-hive_{{ site.scala_version_suffix }}</artifactId>
	<version>{{site.version}}</version>
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6
</dependency>
{% endhighlight %}


<<<<<<< HEAD
使用目录
-----------

## 在Table API中使用HiveCatalog
=======
Use Catalog
-----------

## Use HiveCatalog in Table API
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
//  ------ Register HiveCatalog ------
TableEnvironment tEnv = ...

// Register with Hive conf dir
tEnv.registerCatalog(new HiveCatalog("myHive1", hiveConfDir));

tEnv.listCatalogs();

// ------ Set default catalog and database ------

tEnv.useCatalog("myHive1")
tEnv.useDatabase("myDb");

// ------ Access Hive meta-objects ------

// First get the catalog
Catalog myHive1 = tEnv.getCatalog("myHive1");

// Then read Hive meta-objects
myHive1.listDatabases();
myHive1.listTables("myDb");
myHive1.listViews("myDb");

ObjectPath myTablePath = new ObjectPath("myDb", "myTable");
myHive1.getTable(myTablePath);
myHive1.listPartitions(myTablePath);

......

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
//  ------ Register HiveCatalog ------
val tEnv = ...

// Register with Hive conf dir
tEnv.registerCatalog(new HiveCatalog("myHive1", hiveConfDir));

tEnv.listCatalogs();

// ------ Set default catalog and database ------

tEnv.useCatalog("myHive1")
tEnv.useDatabase("myDb");

// ------ Access Hive meta-objects ------

// First get the catalog
val myHive1 = tEnv.getCatalog("myHive1");

// Then read Hive meta-objects
myHive1.listDatabases();
myHive1.listTables("myDb");
myHive1.listViews("myDb");

val myTablePath = ew ObjectPath("myDb", "myTable");
myHive1.getTable(myTablePath);
myHive1.listPartitions(myTablePath);

......


{% endhighlight %}
</div>
</div>



<<<<<<< HEAD
## 在Flink SQL Client中使用HiveCatalog

用户可以在Flink SQL CLI的yaml配置文件中指定目录。 具体请参阅 [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html) for more details.
=======
## Use HiveCatalog in Flink SQL Client

Users can specify catalogs in the yaml config file of Flink SQL CLI. See [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html) for more details.
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```yaml
catalogs:
   - name: myHive1
     type: hive
     hive-conf-dir: ...
   - name: myHive2
     type: hive
     hive-conf-dir: ...
     default-database: ...
```

<<<<<<< HEAD
以下是一些访问Hive表的示例SQL。
=======
And below are a few example SQL commands accessing a Hive table.
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6

```sql
Flink SQL> show catalogs;
myHive1
myHive2
default_catalog

# ------ Set default catalog and database ------

Flink SQL> use catalog myHive1;
Flink SQL> use database myDb;

# ------ Access Hive metadata ------

Flink SQL> show databases;
myDb

Flink SQL> show tables;
myTable

Flink SQL> describe myTable;
root
 |--    name: name
    |-- type: StringType
    |-- isNullable: true
 |--    name: value
    |-- type: DoubleType
    |-- isNullable: true

Flink SQL> ......


```

<<<<<<< HEAD
有关访问Hive元对象的Flink SQL命令的完整列表，请参阅 [FLINK SQL]({{ site.baseurl }}/dev/table/sql.html)


{% top %}
=======
For a full list of Flink SQL commands to access Hive meta-objects, see [FLINK SQL]({{ site.baseurl }}/dev/table/sql.html)


{% top %}
>>>>>>> 07ce142259a08eaec2e044c31d5beaa7b19ae6b6
