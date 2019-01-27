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

A catalog can provide information about databases and tables such as their name, schema, statistics, and information for how to access data stored in a database, table, or file. Once a catalog is registered to a `TableEnvironment`, all meta-objects including databases and tables defined in a catalog can be accessed from Table API or SQL queries.


* This will be replaced by the TOC
{:toc}


Catalog Interface
-----------------

Catalogs are categorized into `ReadableCatalog` and `ReadableWritableCatalog`. The former one can only read meta-objects from the catalog system, and the later one can do both read and write.


Naming Structure in Catalog
---------------------------

Flink's catalogs use a strict two-level structure, that is, catalogs contain databases, and databases contain meta-objects. Thus, the full name of a meta-object is always structured as `catalogName`.`databaseName`.`objectName`.

All registered catalogs are managed by a `CatalogManager` instance in a `TableEnvironment`. In order to ease access to meta-objects, `CatalogManager` has a concept of default catalog and default database. Usually how users access meta-objects in a catalog is to specify its full name in the format of `catalogName`.`databaseName`.`objectName`. By setting default catalog and default database, users can use just the meta-object's name in their queries. This greatly simplifies user experience. For example, a previous query as 

```sql
select * from mycatalog.mydb.myTable
```
 
can be shortened as 

```sql
select * from myTable
```

Querying tables in a different databases under the default catalog would be 

```
select * from mydb2.myTable
```

`CatalogManager` always has a built-in `FlinkInMemoryCatalog` with name of `builtin`, which has a built-in default database named `default`. If no catalog is explicitly set as default catalog, they will be the default catalog and default database. All temp meta-objects will be registered to this catalog. Users can set default catalog and database via `TableEnvironment.setDefaultDatabase()` in Table API or `use catalog.db` in Flink SQL Cli.

 
Catalog Types
-------------

## FlinkInMemoryCatalog

This class implements `ReadableWritableCatalog` and will be provided by Flink to host meta-objects defined in Flink using memory as storage.

## HiveCatalog

`HiveCatalog` integrates Flink with Hive at metadata level.

The ultimate goal for `HiveCatalog` is that: 

1. existing meta-objects, like tables, views, and functions, created by Hive or other Hive-compatible applications can be used by Flink

2. meta-objects created by `HiveCatalog` can be written back to Hive metastore such that Hive and other Hive-compatibile applications can consume.

To query Hive data with `HiveCatalog`, users have to use Flink's `batch` mode by either using `BatchTableEnvironment` in Table APIs or setting `execution.type` as `batch` in Flink SQL Cli.

Note that currently `HiveCatalog` only offers capabilities of reading Hive metastore metadata, including databases, tables, table partitions, simple data types, and table and column stats. Other meta-objects read and write capabilities are under either experiment or active development.

Also note that currently only registering `HiveCatalog` through Table APIs allows users to customize their `HiveConf` with additional Hive connection parameters. Users need to make sure Flink can connect to their Hive metastore within their environment.


### Data Type Mapping between Flink and Hive via HiveCatalog

Currently `HiveCatalog` supports most simple data types. Upon reading Hive table, `HiveCatalog` will map Hive data type to Flink data type according to the following mapping:

| Hive Data Type | Flink Data Type  |
|---|---|
| char(p)       | String |
| varchar(p)    | String |
| string        | String |
| decimal(p, s) | Decimal(p, s) |
| boolean       | Boolean |
| tinyint       | Byte |
| smallint      | Short |
| int           | Int |
| bigint        | Long |
| float         | Float |
| double        | Double |
| date          | Date |
| timestamp     | Timestamp |
| binary        | ByteArray |

 
## GenericHiveMetastoreCatalog

`GenericHiveMetastoreCatalog` is created to persist Flink meta-objects in Hive metastore by using Hive metastore purely as storage. Users are not dealing with anything specific to Hive, and Hive may not understand these objects at all.

`GenericHiveMetastoreCatalog` is under active development. Stay tuned.


## Hive Compatibility

For Hive compatibility and versions, see [Hive Compatibility]({{ site.baseurl }}/dev/batch/hive_compatibility.html)

{% top %}

Catalog Module
--------------

`FlinkInMemoryCatalog` is built in `flink-table` module.

In order to use Hive-metastore-backed catalogs in Flink, users need to include `flink-connector-hive` jar in their projects.

{% highlight xml %}
<dependency>
	<groupId>com.alibaba.blink</groupId>
	<artifactId>flink-connector-hive{{ site.scala_version_suffix }}</artifactId>
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
BatchTableEnvironment tEnv = ...

// Option 1: Register with Hive metastore thrift URIs
tEnv.registerCatalog(new HiveCatalog("myHive1", "thrift:<myip1>:<myport1>,thrift:<myip2>:<myport2>"));

// Option 2: Register with HiveConf
HiveConf hiveConf = ...
tEnv.registerCatalog(new HiveCatalog("myHive2", hiveConf));


// ------ Set default catalog and database ------

tEnv.setDefaultDatabase("myHive1", "myDb");


// ------ Access Hive meta-objects ------

// First get the catalog
ReadableCatalog myHive1 = tEnv.getCatalog("myHive1");

// Then read Hive meta-objects
myHive1.listDatabases();
myHive1.listAllTables();
myHive1.listTables("myDb");

ObjectPath myTablePath = ew ObjectPath("myDb", "myTable");
myHive1.getTable(myTablePath);
myHive1.listPartitions(myTablePath);

......

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
//  ------ Register HiveCatalog ------
val tEnv = ...

// Option 1: Register with Hive metastore thrift URIs
tEnv.registerCatalog(new HiveCatalog("myHive1", "thrift:<myip1>:<myport1>,thrift:<myip2>:<myport2>"));

// Option 2: Register with HiveConf
val hiveConf = ...
tEnv.registerCatalog(new HiveCatalog("myHive2", hiveConf));


// ------ Set default catalog and database ------

tEnv.setDefaultDatabase("myHive1", "myDb");


// ------ Access Hive meta-objects ------

// First get the catalog
val myHive1 = tEnv.getCatalog("myHive1");

// Then read Hive meta-objects
myHive1.listDatabases();
myHive1.listAllTables();
myHive1.listTables("myDb");

val myTablePath = ew ObjectPath("myDb", "myTable");
myHive1.getTable(myTablePath);
myHive1.listPartitions(myTablePath);

......


{% endhighlight %}
</div>
</div>



## Use HiveCatalog in Flink SQL Cli

Users can specify catalogs in the yaml config file of Flink SQL Cli. See [SQL Cli]({{ site.baseurl }}/dev/table/sqlClient.html) for more details.

```yaml
catalogs:
   - name: myHive1
     catalog:
      type: hive
      connector:
        # Hive metastore thrift uri
        Hive.metastore.uris: thrift://<ip1>:<port1>,thrift://<ip2>:<port2>
   - name: myHive2
     catalog:
      type: hive
      connector:
        # Hive metastore thrift uri
        Hive.metastore.uris: thrift://<ip>:<port>
```

To run a few example SQL commands to access a Hive table.

```sql
Flink SQL> show catalogs;
myHive1
myHive2

# ------ Set default catalog and database ------

Flink SQL> use myHive1.myDb;

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
