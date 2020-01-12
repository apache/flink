---
title: "Catalogs"
is_beta: true
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

Catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems.

One of the most crucial aspects of data processing is managing metadata.
It may be transient metadata like temporary tables, or UDFs registered against the table environment.
Or permanent metadata, like that in a Hive Metastore. Catalogs provide a unified API for managing metadata and making it accessible from the Table API and SQL Queries. 

* This will be replaced by the TOC
{:toc}

## Catalog Types

### GenericInMemoryCatalog

The `GenericInMemoryCatalog` is an in-memory implementation of a catalog. All objects will be available only for the lifetime of the session.

### HiveCatalog

The `HiveCatalog` serves two purposes; as persistent storage for pure Flink metadata, and as an interface for reading and writing existing Hive metadata. 
Flink's [Hive documentation]({{ site.baseurl }}/dev/table/hive/index.html) provides full details on setting up the catalog and interfacing with an existing Hive installation.


{% warn %} The Hive Metastore stores all meta-object names in lower case. This is unlike `GenericInMemoryCatalog` which is case-sensitive

### User-Defined Catalog

Catalogs are pluggable and users can develop custom catalogs by implementing the `Catalog` interface.
To use custom catalogs in SQL CLI, users should develop both a catalog and its corresponding catalog factory by implementing the `CatalogFactory` interface.

The catalog factory defines a set of properties for configuring the catalog when the SQL CLI bootstraps.
The set of properties will be passed to a discovery service where the service tries to match the properties to a `CatalogFactory` and initiate a corresponding catalog instance.

## How to Create and Register Flink Tables to Catalog

### Using SQL DDL

Users can use SQL DDL to create tables in catalogs in both Table API and SQL.

For Table API:

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

For SQL Client:

{% highlight sql %}
// the catalog should have been registered via yaml file
Flink SQL> CREATE DATABASE mydb WITH (...);

Flink SQL> CREATE TABLE mytable (name STRING, age INT) WITH (...);

Flink SQL> SHOW TABLES;
mytable
{% endhighlight %}

For detailed information, please check out [Flink SQL CREATE DDL]({{ site.baseurl }}/dev/table/sql/create.html).

### Using Java/Scala/Python API

Users can use Java, Scala, or Python API to create catalog tables programmatically.

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
    
List<String> tables = catalog.listTables("mydb); // tables should contain "mytable"
{% endhighlight %}

</div>
</div>

## Catalog API

Note: only catalog program APIs are listed here. Users can achieve many of the same funtionalities with SQL DDL. 
For detailed DDL information, please refer to [SQL CREATE DDL]({{ site.baseurl }}/dev/table/sql/create.html).


### Database operations

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

### Table operations

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

### View operations

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


### Partition operations

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


### Function operations

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


## Table API and SQL for Catalog

### Registering a Catalog

Users have access to a default in-memory catalog named `default_catalog`, that is always created by default. This catalog by default has a single database called `default_database`.
Users can also register additional catalogs into an existing Flink session.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnv.registerCatalog(new CustomCatalog("myCatalog"));
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">

All catalogs defined using YAML must provide a `type` property that specifies the type of catalog. 
The following types are supported out of the box.

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

### Changing the Current Catalog And Database

Flink will always search for tables, views, and UDF's in the current catalog and database. 

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

Metadata from catalogs that are not the current catalog are accessible by providing fully qualified names in the form `catalog.database.object`.

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

### List Available Catalogs

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


### List Available Databases

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

### List Available Tables

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
