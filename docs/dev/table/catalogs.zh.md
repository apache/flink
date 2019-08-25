---
title: "Catalogs"
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

Catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems.

One of the most crucial aspects of data processing is managing metadata.
It may be transient metadata like temporary tables, or UDFs registered against the table environment.
Or permanent metadata, like that in a Hive Metastore. Catalogs provide a unified API for managing metadata and making it accessible from the Table API and SQL Queries. 

* This will be replaced by the TOC
{:toc}

## Catalog Types

### GenericInMemoryCatalog

Flink sessions always have a built-in `GenericInMemoryCatalog` named `default_catalog`, which has a built-in default database named `default_database`.
All temporary metadata, such tables defined using `TableEnvironment#registerTable` is registered to this catalog. 

### HiveCatalog

The `HiveCatalog` serves two purposes; as persistent storage for pure Flink metadata, and as an interface for reading and writing existing Hive metadata. 
Flink's [Hive documentation]({{ site.baseurl }}/dev/table/hive/index.html) provides full details on setting up the catalog and interfacing with an existing Hive installation.


{% warn %} The Hive Metastore stores all meta-object names in lower case. This is unlike `GenericInMemoryCatalog` which is case-sensitive

### User-Defined Catalog

Catalogs are pluggable and users can develop custom catalogs by implementing the `Catalog` interface.
To use custom catalogs in SQL CLI, users should develop both a catalog and its corresponding catalog factory by implementing the `CatalogFactory` interface.

The catalog factory defines a set of properties for configuring the catalog when the SQL CLI bootstraps.
The set of properties will be passed to a discovery service where the service tries to match the properties to a `CatalogFactory` and initiate a corresponding catalog instance.

## Catalog API

### Registering a Catalog

Users can register additional catalogs into an existing Flink session.

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
tableEnv.scan("not_the_current_catalog", "not_the_current_db", "my_table");
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
