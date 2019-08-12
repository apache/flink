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

Catalogs provide metadata, such as names, schemas, statistics of tables, and information about how to access data stored in a database or other external systems. Once a catalog is registered within a `TableEnvironment`, all its meta-objects are accessible from the Table API and SQL queries.


* This will be replaced by the TOC
{:toc}


Catalog Interface
-----------------

APIs are defined in `Catalog` interface. The interface defines a set of APIs to read and write catalog meta-objects such as database, tables, partitions, views, and functions.


Catalog Meta-Objects Naming Structure
-------------------------------------

Flink's catalogs use a strict two-level structure, that is, catalogs contain databases, and databases contain meta-objects. Thus, the full name of a meta-object is always structured as `catalogName`.`databaseName`.`objectName`.

Each `TableEnvironment` has a `CatalogManager` to manager all registered catalogs. To ease access to meta-objects, `CatalogManager` has a concept of current catalog and current database. By setting current catalog and current database, users can use just the meta-object's name in their queries. This greatly simplifies user experience.

For example, a previous query as

```sql
select * from mycatalog.mydb.myTable;
```

can be shortened to

```sql
select * from myTable;
```

To querying tables in a different database under the current catalog, users don't need to specify the catalog name. In our example, it would be

```
select * from mydb2.myTable2
```

`CatalogManager` always has a built-in `GenericInMemoryCatalog` named `default_catalog`, which has a built-in default database named `default_database`. If no other catalog and database are explicitly set, they will be the current catalog and current database by default. All temp meta-objects, such as those defined by `TableEnvironment#registerTable`  are registered to this catalog. 

Users can set current catalog and database via `TableEnvironment.useCatalog(...)` and
`TableEnvironment.useDatabase(...)` in Table API, or `USE CATALOG ...` and `USE ...` in Flink SQL
 Client.


Catalog Types
-------------

## GenericInMemoryCatalog

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

### Supported Hive Versions

Flink's `HiveCatalog` officially supports Hive 2.3.4 and 1.2.1.

The Hive version is explicitly specified as a String, either by passing it to the constructor when creating `HiveCatalog` instances directly in Table API or specifying it in yaml config file in SQL CLI. The Hive version string are `2.3.4` and `1.2.1`.

### Case Insensitive to Meta-Object Names

Note that Hive Metastore stores meta-object names in lower cases. Thus, unlike `GenericInMemoryCatalog`, `HiveCatalog` is case-insensitive to meta-object names, and users need to be cautious on that.

### Dependencies

To use `HiveCatalog`, users need to include the following dependency jars.

For Hive 2.3.4, users need:

```
// Hive dependencies

- hive-exec-2.3.4.jar // contains hive-metastore-2.3.4


// Hadoop dependencies
- flink-shaded-hadoop-2-uber-2.7.5-1.8.0.jar
- flink-hadoop-compatibility-{{site.version}}.jar

```

For Hive 1.2.1, users need:

```
// Hive dependencies

- hive-metastore-1.2.1.jar
- hive-exec-1.2.1.jar
- libfb303-0.9.3.jar


// Hadoop dependencies
- flink-shaded-hadoop-2-uber-2.6.5-1.8.0.jar
- flink-hadoop-compatibility-{{site.version}}.jar

```

If you don't have Hive dependencies at hand, they can be found at [mvnrepostory.com](https://mvnrepository.com):

- [hive-exec](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)
- [hive-metastore](https://mvnrepository.com/artifact/org.apache.hive/hive-metastore)

Note that users need to make sure the compatibility between their Hive versions and Hadoop versions. Otherwise, there may be potential problem, for example, Hive 2.3.4 is compiled against Hadoop 2.7.2, you may run into problems when using Hive 2.3.4 with Hadoop 2.4.


### Data Type Mapping

For both Flink and Hive tables, `HiveCatalog` stores table schemas by mapping them to Hive table schemas with Hive data types. Types are dynamically mapped back on read.

Currently `HiveCatalog` supports most Flink data types with the following mapping:

|  Flink Data Type  |  Hive Data Type  |
|---|---|
| CHAR(p)       |  CHAR(p)* |
| VARCHAR(p)    |  VARCHAR(p)** |
| STRING        |  STRING |
| BOOLEAN       |  BOOLEAN |
| TINYINT       |  TINYINT |
| SMALLINT      |  SMALLINT |
| INT           |  INT |
| BIGINT        |  LONG |
| FLOAT         |  FLOAT |
| DOUBLE        |  DOUBLE |
| DECIMAL(p, s) |  DECIMAL(p, s) |
| DATE          |  DATE |
| TIMESTAMP_WITHOUT_TIME_ZONE |  TIMESTAMP |
| TIMESTAMP_WITH_TIME_ZONE |  N/A |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE |  N/A |
| INTERVAL      |   N/A*** |
| BINARY        |   N/A |
| VARBINARY(p)  |   N/A |
| BYTES         |   BINARY |
| ARRAY\<E>     |  ARRAY\<E> |
| MAP<K, V>     |  MAP<K, V> ****|
| ROW           |  STRUCT |
| MULTISET      |  N/A |


Note that we only cover most commonly used data types for now.

The following limitations in Hive's data types impact the mapping between Flink and Hive:

\* maximum length is 255

\** maximum length is 65535

\*** `INTERVAL` type can not be mapped to hive `INTERVAL` for now.

\**** Hive map key type only allows primitive types, while Flink map key can be any data type.

## User-configured Catalog

Catalogs are pluggable. Users can develop custom catalogs by implementing the `Catalog` interface, which defines a set of APIs for reading and writing catalog meta-objects such as database, tables, partitions, views, and functions.

Catalog Registration
--------------------

## Register Catalog in Table API

To register a catalog in Table API, users can create a catalog instance and register it through `TableEnvironment.registerCatalog(name, catalog)`.

## Register Catalog in SQL CLI

To use pre-defined catalogs (`GenericInMemoryCatalog` and `HiveCatalog`) in SQL CLI, please refer to [SQL Clinet]({{ site.baseurl }}/dev/table/sqlClient.html)

To use custom catalogs in SQL CLI, users should develop both a catalog and its corresponding catalog factory by implementing `Catalog` and `CatalogFactory` interfaces respectively.

The catalog factory defines a set of properties for configuring the catalog when SQL CLI bootstraps. The set of properties will be passed to a discovery service where the service tries to match the properties to a `CatalogFactory` and initiate an corresponding catalog instance.


{% top %}

Catalog Modules
---------------

`GenericInMemoryCatalog` is built into Flink's Table API.

To use `HiveCatalog` in Flink Table API and SQL, users need to include `flink-connector-hive` jar in their projects.

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-hive_{{ site.scala_version_suffix }}</artifactId>
	<version>{{site.version}}</version>
</dependency>
{% endhighlight %}


Use Catalog
-----------

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



## Use HiveCatalog in Flink SQL Client

Users can specify catalogs in the yaml config file of Flink SQL CLI. See [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html) for more details.

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

And below are a few example SQL commands accessing a Hive table.

```sql
Flink SQL> show catalogs;
myHive1
myHive2
default_catalog

# ------ Set default catalog and database ------

Flink SQL> use catalog myHive1;
Flink SQL> use myDb;

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
