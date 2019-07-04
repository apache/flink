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

A catalog can provide information about metadata, such as names, schemas, statistics of tables, and information about how to access data stored in a database or table. Once a catalog is registered to a `TableEnvironment`, all meta-objects defined in a catalog can be accessed from Table API or SQL queries.


* This will be replaced by the TOC
{:toc}


Catalog Interface
-----------------

APIs are defined in `Catalog` interface. The interface defines a set of APIs to read and write catalog meta-objects such as database, tables, partitions, views, and functions.

Users can develop their own catalogs by implementing the interface.


Naming Structure in Catalog
---------------------------

Flink's catalogs use a strict two-level structure, that is, catalogs contain databases, and databases contain meta-objects. Thus, the full name of a meta-object is always structured as `catalogName`.`databaseName`.`objectName`.

All registered catalogs are managed by a `CatalogManager` instance in `TableEnvironment`. In order to ease access to meta-objects, `CatalogManager` has a concept of current catalog and current database. Usually how users access meta-objects in a catalog is to specify its full name in the format of `catalogName`.`databaseName`.`objectName`. By setting current catalog and current database, users can use just the meta-object's name in their queries. This greatly simplifies user experience. For example, a previous query as

```sql
select * from mycatalog.mydb.myTable;
```

can be shortened as

```sql
select * from myTable;
```

Querying tables in a different databases under the default catalog would be

```
select * from mydb2.myTable
```

`CatalogManager` always has a built-in `GenericInMemoryCatalog` with name of `default_catalog`, which has a built-in default database named `default_database`. They will be the current catalog and current database if no other catalog and database are explicitly set. All temp meta-objects will be registered to this catalog. Users can set current catalog and database via `TableEnvironment.useCatalog(...)` and `TableEnvironment.useDatabase(...)` in Table API, or `USE CATALOG ...` and `USE DATABASE ...` in Flink SQL.


Catalog Types
-------------

## GenericInMemoryCatalog

All meta-objects in this catalog are stored in memory, and be will lost once the session shuts down.

Its config entry value in SQL CLI yaml file is "generic_in_memory".

## HiveCatalog

`HiveCatalog` can read and write both Flink and Hive meta-objects by using Hive Metastore as a persistent storage.

Its config entry value in SQL CLI yaml file is "hive".

### Persist Flink meta-objects

Previously, Flink meta-objects are only stored in memory and are per session based. That means users have to recreate all the meta-objects every time they start a new session.

To solve this user pain point, users can choose the option to use `HiveCatalog` to persist all of users' Flink streaming and batch meta-objects by using Hive Metastore as a pure storage. Because Hive Metastore is only used for storage in this case, Hive itself may not understand Flink's meta-objects stored in the metastore.

### Integrate Flink with Hive metadata

The ultimate goal for integrating Flink with Hive metadata is that:

1. existing meta-objects, like tables, views, and functions, created by Hive or other Hive-compatible applications can be used by Flink

2. meta-objects created by `HiveCatalog` can be written back to Hive metastore such that Hive and other Hive-compatibile applications can consume.

## User-configured Catalog

Catalogs are pluggable, and users can use their own, customized catalog implementations.


HiveCatalog
-----------

## Supported Hive Versions

`HiveCatalog` officially supports Hive 2.3.4 and 1.2.1, and depends on Hive's own compatibility for the other 2.x.x and 1.x.x versions.

Users need to explicitly specify the Hive version as string, by either passing it to the constructor when creating `HiveCatalog` instances directly in Table API or specifying it in yaml config file in SQL CLI. The Hive version string will be either `2.3.4`, `1.2.1`, or your own 2.x.x/1.x.x versions.

## Dependencies

In order to use `HiveCatalog`, users need to either downloading the following dependency jars and adding them to the `/lib` dir in Flink distribution, or adding their existing Hive jars to Flink's classpath in order for Flink to find them at runtime.

Take Hive 2.3.4 for example:

```
// Hive dependencies

- hive-metastore-2.3.4.jar
- hive-exec-2.3.4.jar


// Hadoop dependencies
- flink-shaded-hadoop-2-uber-2.7.5-1.8.0.jar
- flink-hadoop-compatibility-{{site.version}}.jar

```

Note that users need to make sure the compatibility between their Hive versions and hadoop versions. Otherwise, there may be potential problem, for example, Hive 2.3.4 is compiled against Hadoop 2.7.2, you may run into problems when using Hive 2.3.4 with Hadoop 2.4.


## Data Type Mapping

For both Flink and Hive tables, `HiveCatalog` stores column types in schema by mapping them to Hive data types. Upon reading tables, `HiveCatalog` maps them back.

Currently `HiveCatalog` supports most Flink data types with the following mapping:

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


Note that we only covers most commonly used data types for now.

The following limitations in Hive's data types that will impact the mapping between Flink and Hive if you choose to use `HiveCatalog`.

\* maximum length is 255

\** maximum length is 65535

## Hive Compatibility

For Hive compatibility and versions, see [Hive Compatibility]({{ site.baseurl }}/dev/batch/hive_compatibility.html)


Catalog Registration
--------------------

## Register Catalog in Table API

To register catalog in Table API, users can just create catalog instances and register them through `TableEnvironment.registerCatalog(name, catalog)`.

This applies to both pre-defined catalogs and users' customized catalogs.


## Register Catalog in SQL CLI

To use pre-defined catalogs (`GenericInMemoryCatalog` and `HiveCatalog`) in SQL CLI, please refer to [SQL Clinet]({{ site.baseurl }}/dev/table/sqlClient.html)

To use custom catalog in SQL CLI, users should develop both a catalog implementation and its corresponding catalog factory which implements `CatalogFactory` interface, as well as defining a set of properties for the catalog to be configured in SQL CLI yaml file. When SQL CLI bootstraps, it will read the configured properties and pass them into a discovery service where the service tries to match the properties to a `CatalogFactory` and initiate an corresponding catalog instance.


{% top %}

Catalog Modules
---------------

`GenericInMemoryCatalog` is built in Flink's Table API.

In order to use `HiveCatalog` in Flink Table API and SQL, users need to include `flink-connector-hive` jar in their projects.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-hive_{{ site.scala_version_suffix }}</artifactId>
	<version>{{site.version}}</version>
</dependency>
{% endhighlight %}


Use Catalog
-----------

We will use `HiveCatalog` for example in the following content.

## Use HiveCatalog in Table API

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



## Use HiveCatalog in Flink SQL Cli

Users can specify catalogs in the yaml config file of Flink SQL CLI. See [SQL Cli]({{ site.baseurl }}/dev/table/sqlClient.html) for more details.

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

To run a few example SQL commands to access a Hive table.

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

For a full list of Flink SQL commands to access Hive meta-objects, see [FLINK SQL]({{ site.baseurl }}/dev/table/sql.html)


{% top %}
