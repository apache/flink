---
title: "Catalogs"
weight: 81
type: docs
aliases:
  - /zh/dev/table/catalogs.html
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

# Catalogs

Catalog 提供了元数据信息，例如数据库、表、分区、视图以及数据库或其他外部系统中存储的函数和信息。

数据处理最关键的方面之一是管理元数据。
元数据可以是临时的，例如临时表、或者通过 TableEnvironment 注册的 UDF。
元数据也可以是持久化的，例如 Hive Metastore 中的元数据。Catalog 提供了一个统一的API，用于管理元数据，并使其可以从 Table API 和 SQL 查询语句中来访问。



## Catalog 类型

### GenericInMemoryCatalog

`GenericInMemoryCatalog` 是基于内存实现的 Catalog，所有元数据只在 session 的生命周期内可用。

### JdbcCatalog

`JdbcCatalog` 使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。`PostgresCatalog` 是当前实现的唯一一种 JDBC Catalog。
参考 [JdbcCatalog 文档]({{< ref "docs/connectors/table/jdbc" >}}) 获取关于配置 JDBC catalog 的详细信息。

### HiveCatalog

`HiveCatalog` 有两个用途：作为原生 Flink 元数据的持久化存储，以及作为读写现有 Hive 元数据的接口。 
Flink 的 [Hive 文档]({{< ref "docs/connectors/table/hive/overview" >}}) 提供了有关设置 `HiveCatalog` 以及访问现有 Hive 元数据的详细信息。


<span class="label label-danger">警告</span> Hive Metastore 以小写形式存储所有元数据对象名称。而 `GenericInMemoryCatalog` 区分大小写。

### 用户自定义 Catalog

Catalog 是可扩展的，用户可以通过实现 `Catalog` 接口来开发自定义 Catalog。
想要在 SQL CLI 中使用自定义 Catalog，用户除了需要实现自定义的 Catalog 之外，还需要为这个 Catalog 实现对应的 `CatalogFactory` 接口。

`CatalogFactory` 定义了一组属性，用于 SQL CLI 启动时配置 Catalog。
这组属性集将传递给发现服务，在该服务中，服务会尝试将属性关联到 `CatalogFactory` 并初始化相应的 Catalog 实例。

## 如何创建 Flink 表并将其注册到 Catalog

### 使用 SQL DDL

用户可以使用 DDL 通过 Table API 或者 SQL Client 在 Catalog 中创建表。

{{< tabs "88ed733a-cf54-4676-9685-7d77d3cc9771" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = ...

// Create a HiveCatalog 
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
tableEnv.executeSql("CREATE DATABASE mydb WITH (...)");

// Create a catalog table
tableEnv.executeSql("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

tableEnv.listTables(); // should return the tables in current catalog and database.

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = ...

// Create a HiveCatalog 
val catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
tableEnv.executeSql("CREATE DATABASE mydb WITH (...)");

// Create a catalog table
tableEnv.executeSql("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

tableEnv.listTables(); // should return the tables in current catalog and database.

```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.catalog import HiveCatalog

# Create a HiveCatalog
catalog = HiveCatalog("myhive", None, "<path_of_hive_conf>")

# Register the catalog
t_env.register_catalog("myhive", catalog)

# Create a catalog database
t_env.execute_sql("CREATE DATABASE mydb WITH (...)")

# Create a catalog table
t_env.execute_sql("CREATE TABLE mytable (name STRING, age INT) WITH (...)")

# should return the tables in current catalog and database.
t_env.list_tables()

```
{{< /tab >}}
{{< tab "SQL Client" >}}
```sql
// the catalog should have been registered via yaml file
Flink SQL> CREATE DATABASE mydb WITH (...);

Flink SQL> CREATE TABLE mytable (name STRING, age INT) WITH (...);

Flink SQL> SHOW TABLES;
mytable
```
{{< /tab >}}
{{< /tabs >}}


更多详细信息，请参考[Flink SQL CREATE DDL]({{< ref "docs/dev/table/sql/create" >}})。

### 使用 Java/Scala

用户可以用编程的方式使用Java 或者 Scala 来创建 Catalog 表。

{{< tabs "f491601e-d053-4ccb-9e14-203154f01b82" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Create a HiveCatalog
Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");

// Register the catalog
tableEnv.registerCatalog("myhive", catalog);

// Create a catalog database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...));

// Create a catalog table
final Schema schema = Schema.newBuilder()
    .column("name", DataTypes.STRING())
    .column("age", DataTypes.INT())
    .build();

tableEnv.createTable("myhive.mydb.mytable", TableDescriptor.forConnector("kafka")
    .schema(schema)
    // …
    .build());

List<String> tables = catalog.listTables("mydb"); // tables should contain "mytable"
```

{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.api._
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.hive.HiveCatalog

val tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode())

// Create a HiveCatalog
val catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>")

// Register the catalog
tableEnv.registerCatalog("myhive", catalog)

// Create a catalog database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...))

// Create a catalog table
val schema = Schema.newBuilder()
    .column("name", DataTypes.STRING())
    .column("age", DataTypes.INT())
    .build()

tableEnv.createTable("myhive.mydb.mytable", TableDescriptor.forConnector("kafka")
    .schema(schema)
    // …
    .build())

val tables = catalog.listTables("mydb") // tables should contain "mytable"
```

{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *
from pyflink.table.catalog import HiveCatalog, CatalogDatabase, ObjectPath, CatalogBaseTable
from pyflink.table.descriptors import Kafka

settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

# Create a HiveCatalog
catalog = HiveCatalog("myhive", None, "<path_of_hive_conf>")

# Register the catalog
t_env.register_catalog("myhive", catalog)

# Create a catalog database
database = CatalogDatabase.create_instance({"k1": "v1"}, None)
catalog.create_database("mydb", database)

# Create a catalog table
schema = Schema.new_builder() \
    .column("name", DataTypes.STRING()) \
    .column("age", DataTypes.INT()) \
    .build()
    
catalog_table = t_env.create_table("myhive.mydb.mytable", TableDescriptor.for_connector("kafka")
    .schema(schema)
    // …
    .build())

# tables should contain "mytable"
tables = catalog.list_tables("mydb")
```

{{< /tab >}}
{{< /tabs >}}

## Catalog API

注意：这里只列出了编程方式的 Catalog API，用户可以使用 SQL DDL 实现许多相同的功能。
关于 DDL 的详细信息请参考 [SQL CREATE DDL]({{< ref "docs/dev/table/sql/create" >}})。


### 数据库操作

{{< tabs "3558a545-e79d-4328-8b79-af4bfd90c7d1" >}}
{{< tab "Java/Scala" >}}
```java
// create database
catalog.createDatabase("mydb", new CatalogDatabaseImpl(...), false);

// drop database
catalog.dropDatabase("mydb", false);

// alter database
catalog.alterDatabase("mydb", new CatalogDatabaseImpl(...), false);

// get database
catalog.getDatabase("mydb");

// check if a database exist
catalog.databaseExists("mydb");

// list databases in a catalog
catalog.listDatabases("mycatalog");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.catalog import CatalogDatabase

# create database
catalog_database = CatalogDatabase.create_instance({"k1": "v1"}, None)
catalog.create_database("mydb", catalog_database, False)

# drop database
catalog.drop_database("mydb", False)

# alter database
catalog.alter_database("mydb", catalog_database, False)

# get database
catalog.get_database("mydb")

# check if a database exist
catalog.database_exists("mydb")

# list databases in a catalog
catalog.list_databases()
```
{{< /tab >}}
{{< /tabs >}}

### 表操作

{{< tabs "ba887fb0-81d0-4080-8609-789d548ae56b" >}}
{{< tab "Java/Scala" >}}
```java
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *
from pyflink.table.catalog import CatalogBaseTable, ObjectPath
from pyflink.table.descriptors import Kafka

table_schema = TableSchema.builder() \
    .field("name", DataTypes.STRING()) \
    .field("age", DataTypes.INT()) \
    .build()

table_properties = Kafka() \
    .version("0.11") \
    .start_from_earlist() \
    .to_properties()

catalog_table = CatalogBaseTable.create_table(schema=table_schema, properties=table_properties, comment="my comment")

# create table
catalog.create_table(ObjectPath("mydb", "mytable"), catalog_table, False)

# drop table
catalog.drop_table(ObjectPath("mydb", "mytable"), False)

# alter table
catalog.alter_table(ObjectPath("mydb", "mytable"), catalog_table, False)

# rename table
catalog.rename_table(ObjectPath("mydb", "mytable"), "my_new_table")

# get table
catalog.get_table("mytable")

# check if a table exist or not
catalog.table_exists("mytable")

# list tables in a database
catalog.list_tables("mydb")
```
{{< /tab >}}
{{< /tabs >}}

### 视图操作

{{< tabs "797800ec-dde2-45f9-922e-1431ff43e175" >}}
{{< tab "Java/Scala" >}}
```java
// create view
catalog.createTable(new ObjectPath("mydb", "myview"), new CatalogViewImpl(...), false);

// drop view
catalog.dropTable(new ObjectPath("mydb", "myview"), false);

// alter view
catalog.alterTable(new ObjectPath("mydb", "mytable"), new CatalogViewImpl(...), false);

// rename view
catalog.renameTable(new ObjectPath("mydb", "myview"), "my_new_view", false);

// get view
catalog.getTable("myview");

// check if a view exist or not
catalog.tableExists("mytable");

// list views in a database
catalog.listViews("mydb");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *
from pyflink.table.catalog import CatalogBaseTable, ObjectPath

table_schema = TableSchema.builder() \
    .field("name", DataTypes.STRING()) \
    .field("age", DataTypes.INT()) \
    .build()

catalog_table = CatalogBaseTable.create_view(
    original_query="select * from t1",
    expanded_query="select * from test-catalog.db1.t1",
    schema=table_schema,
    properties={},
    comment="This is a view"
)

catalog.create_table(ObjectPath("mydb", "myview"), catalog_table, False)

# drop view
catalog.drop_table(ObjectPath("mydb", "myview"), False)

# alter view
catalog.alter_table(ObjectPath("mydb", "mytable"), catalog_table, False)

# rename view
catalog.rename_table(ObjectPath("mydb", "myview"), "my_new_view", False)

# get view
catalog.get_table("myview")

# check if a view exist or not
catalog.table_exists("mytable")

# list views in a database
catalog.list_views("mydb")
```
{{< /tab >}}
{{< /tabs >}}


### 分区操作

{{< tabs "ea16c47a-94a6-4fd6-9f6c-c453dde908b2" >}}
{{< tab "Java/Scala" >}}
```java
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.catalog import ObjectPath, CatalogPartitionSpec, CatalogPartition

catalog_partition = CatalogPartition.create_instance({}, "my partition")

catalog_partition_spec = CatalogPartitionSpec({"third": "2010", "second": "bob"})
catalog.create_partition(
    ObjectPath("mydb", "mytable"),
    catalog_partition_spec,
    catalog_partition,
    False)

# drop partition
catalog.drop_partition(ObjectPath("mydb", "mytable"), catalog_partition_spec, False)

# alter partition
catalog.alter_partition(
    ObjectPath("mydb", "mytable"),
    CatalogPartitionSpec(...),
    catalog_partition,
    False)

# get partition
catalog.get_partition(ObjectPath("mydb", "mytable"), catalog_partition_spec)

# check if a partition exist or not
catalog.partition_exists(ObjectPath("mydb", "mytable"), catalog_partition_spec)

# list partitions of a table
catalog.list_partitions(ObjectPath("mydb", "mytable"))

# list partitions of a table under a give partition spec
catalog.list_partitions(ObjectPath("mydb", "mytable"), catalog_partition_spec)
```
{{< /tab >}}
{{< /tabs >}}


### 函数操作

{{< tabs "e02e0c0e-20b2-48a3-8aa1-ff825e6cf391" >}}
{{< tab "Java/Scala" >}}
```java
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.catalog import ObjectPath, CatalogFunction

catalog_function = CatalogFunction.create_instance(class_name="my.python.udf")

# create function
catalog.create_function(ObjectPath("mydb", "myfunc"), catalog_function, False)

# drop function
catalog.drop_function(ObjectPath("mydb", "myfunc"), False)

# alter function
catalog.alter_function(ObjectPath("mydb", "myfunc"), catalog_function, False)

# get function
catalog.get_function("myfunc")

# check if a function exist or not
catalog.function_exists("myfunc")

# list functions in a database
catalog.list_functions("mydb")
```
{{< /tab >}}
{{< /tabs >}}


## 通过 Table API 和 SQL Client 操作 Catalog

### 注册 Catalog

用户可以访问默认创建的内存 Catalog `default_catalog`，这个 Catalog 默认拥有一个默认数据库 `default_database`。
用户也可以注册其他的 Catalog 到现有的 Flink 会话中。

{{< tabs "b9ea0237-4684-49cd-9d02-ff3f2487a512" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.registerCatalog(new CustomCatalog("myCatalog"));
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.register_catalog(catalog)
```
{{< /tab >}}
{{< tab "YAML" >}}

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

```yaml
catalogs:
   - name: myCatalog
     type: custom_catalog
     hive-conf-dir: ...
```
{{< /tab >}}
{{< /tabs >}}

### 修改当前的 Catalog 和数据库

Flink 始终在当前的 Catalog 和数据库中寻找表、视图和 UDF。 

{{< tabs "517f64ba-ce33-42a0-8955-dac7e6760827" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.useCatalog("myCatalog");
tableEnv.useDatabase("myDb");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.use_catalog("myCatalog")
t_env.use_database("myDb")
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> USE CATALOG myCatalog;
Flink SQL> USE myDB;
```
{{< /tab >}}
{{< /tabs >}}

通过提供全限定名 `catalog.database.object` 来访问不在当前 Catalog 中的元数据信息。

{{< tabs "eb7d643a-745f-4b4c-9cf2-cdcd77d49998" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.from("not_the_current_catalog.not_the_current_db.my_table");
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.from_path("not_the_current_catalog.not_the_current_db.my_table")
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> SELECT * FROM not_the_current_catalog.not_the_current_db.my_table;
```
{{< /tab >}}
{{< /tabs >}}

### 列出可用的 Catalog

{{< tabs "f0beae6f-5272-4c3e-8f98-4ce35922a7d4" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.listCatalogs();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.list_catalogs()
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> show catalogs;
```
{{< /tab >}}
{{< /tabs >}}


### 列出可用的数据库

{{< tabs "f8a1a667-e2bf-48ee-bb06-58c774d34f76" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.listDatabases();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.list_databases()
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> show databases;
```
{{< /tab >}}
{{< /tabs >}}

### 列出可用的表

{{< tabs "e6f05027-2eef-44cc-baff-188d15c9c3f2" >}}
{{< tab "Java/Scala" >}}
```java
tableEnv.listTables();
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.list_tables()
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql
Flink SQL> show tables;
```
{{< /tab >}}
{{< /tabs >}}
