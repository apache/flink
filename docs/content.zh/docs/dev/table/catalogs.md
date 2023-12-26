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

`JdbcCatalog` 使得用户可以将 Flink 通过 JDBC 协议连接到关系数据库。Postgres Catalog 和 MySQL Catalog 是目前 JDBC Catalog 仅有的两种实现。
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

{{< hint warning >}}从 Flink v1.16 开始, TableEnvironment 引入了一个用户类加载器，以在 table 程序、SQL Client、SQL Gateway 中保持一致的类加载行为。该类加载器会统一管理所有的用户 jar 包，包括通过 `ADD JAR` 或 `CREATE FUNCTION .. USING JAR ..` 添加的 jar 资源。
 在用户自定义 catalog 中，应该将 `Thread.currentThread().getContextClassLoader()` 替换成该用户类加载器去加载类。否则，可能会发生 `ClassNotFoundException` 的异常。该用户类加载器可以通过 `CatalogFactory.Context#getClassLoader` 获得。
{{< /hint >}}

#### Catalog 中支持时间旅行的接口

从 1.18 开始， Flink 框架开始支持[时间旅行]({{< ref "docs/dev/table/sql/queries/time-travel" >}})查询表的历史数据。如果要查询表的历史数据，需要这张表所属于的 `catalog` 实现 `getTable(ObjectPath tablePath, long timestamp)` 方法，如下所示:

```java
public class MyCatalogSupportTimeTravel implements Catalog {
    
    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
            throws TableNotExistException {
        // Build a schema corresponding to the specific time point.
        Schema schema = buildSchema(timestamp);
        // Set parameters to read data at the corresponding time point.
        Map<String, String> options = buildOptions(timestamp);
        // Build CatalogTable
        CatalogTable catalogTable =
                CatalogTable.of(schema, "", Collections.emptyList(), options, timestamp);
        return catalogTable;
    }
}

public class MyDynamicTableFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final ReadableConfig configuration =
                Configuration.fromMap(context.getCatalogTable().getOptions());

        // Get snapshot from CatalogTable
        final Optional<Long> snapshot = context.getCatalogTable().getSnapshot();
        
        // Build DynamicTableSource using snapshot options.
        final DynamicTableSource dynamicTableSource = buildDynamicSource(configuration, snapshot);

        return dynamicTableSource;
    }
}
```

## 如何创建 Flink 表并将其注册到 Catalog

### 使用 SQL DDL

用户可以使用 DDL 通过 Table API 或者 SQL Client 在 Catalog 中创建表。

{{< tabs "88ed733a-cf54-4676-9685-7d77d3cc9771" >}}
{{< tab "Java" >}}
```java
TableEnvironment tableEnv = ...;

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
    # …
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

## Catalog Modification Listener

Flink supports registering customized listener for catalog modification, such as database and table ddl. Flink will create
a `CatalogModificationEvent` event for ddl and notify `CatalogModificationListener`. You can implement a listener
and do some customized operations when receiving the event, such as report the information to some external meta-data systems.

### Implement Catalog Listener

There are two interfaces for the catalog modification listener: `CatalogModificationListenerFactory` to create the listener and `CatalogModificationListener`
to receive and process the event. You need to implement these interfaces and below is an example.

```java
/** Factory used to create a {@link CatalogModificationListener} instance. */
public class YourCatalogListenerFactory implements CatalogModificationListenerFactory {
    /** The identifier for the customized listener factory, you can named it yourself. */
    private static final String IDENTIFIER = "your_factory";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public CatalogModificationListener createListener(Context context) {
        return new YourCatalogListener(Create http client from context);
    }
}

/** Customized catalog modification listener. */
public class YourCatalogListener implements CatalogModificationListener {
    private final HttpClient client;

    YourCatalogListener(HttpClient client) {
        this.client = client;
    }
    
    @Override
    public void onEvent(CatalogModificationEvent event) {
        // Report the database and table information via http client.
    }
}
```

You need to create a file `org.apache.flink.table.factories.Factory` in `META-INF/services`
with the content of `the full name of YourCatalogListenerFactory` for your
customized catalog listener factory. After that, you can package the codes into a jar file
and add it to `lib` of Flink cluster.

### Register Catalog Listener

After implemented above catalog modification factory and listener, you can register it to the table environment.

```java
Configuration configuration = new Configuration();

// Add the factory identifier, you can set multiple listeners in the configuraiton.
configuration.set(TableConfigOptions.TABLE_CATALOG_MODIFICATION_LISTENERS, Arrays.asList("your_factory"));
TableEnvironment env = TableEnvironment.create(
            EnvironmentSettings.newInstance()
                .withConfiguration(configuration)
                .build());

// Create/Alter/Drop database and table.
env.executeSql("CREATE TABLE ...").wait();
```

For sql-gateway, you can add the option `table.catalog-modification.listeners` in the `flink-conf.yaml` and start
the gateway, or you can also start sql-gateway with dynamic parameter, then you can use sql-client to perform ddl directly.

## Catalog Store

Catalog Store 用于保存 Catalog 的配置信息， 配置 Catalog Store 之后，在 session 中创建的 catalog 信息会持久化至
Catalog Store 对应的外部系统中，即使 session 重建， 之前创建的 Catalog 依旧可以从 Catalog Store 中重新获取。

### Catalog Store 的配置
用户可以以不同的方式配置 Catalog Store，一种是使用Table API，另一种是使用 YAML 配置。

在 Table API 中使用 Catalog Store 实例来注册 Catalog Store 。
```java
// Initialize a catalog Store
CatalogStore catalogStore = new FileCatalogStore("file://path/to/catalog/store/");

// set up the catalog store
final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inBatchMode()
        .withCatalogStore(catalogStore)
        .build();

final TableEnvironment tableEnv = TableEnvironment.create(settings);
```

在 Table API 中使用 configuration 注册 Catalog Store 。
```java
// set up configuration
Configuration configuration = new Configuration();
configuration.set("table.catalog-store.kind", "file");
configuration.set("table.catalog-store.file.path", "file://path/to/catalog/store/");
// set up the configuration.
final EnvironmentSettings settings =
        EnvironmentSettings.newInstance().inBatchMode()
        .withConfiguration(configuration)
        .build();

final TableEnvironment tableEnv = TableEnvironment.create(settings);
```

在 SQL Gateway 中，推荐在 `flink-conf.yaml` 文件中进行配置，所有的 session 可以自动使用已经创建好的 Catalog 。
配置的格式如下，一般情况下需要配置 Catalog Store 的 kind ，以及 Catalog Store 需要的其他参数配置。
```yaml
table.catalog-store.kind: file
table.catalog-store.file.path: /path/to/catalog/store/
```

### Catalog Store 类型
Flink 框架内置了两种 Catalog Store，分别是 GenericInMemoryCatalogStore 和 FileCatalogStore。用户也可以自定义 Catalog Store 。

#### GenericInMemoryCatalogStore
GenericInMemoryCatalogStore 是基于内存实现的 Catalog Store，所有的 Catalog 配置只在 session 的生命周期内可用，
session 重建之后 store 中保存的 Catalog 配置也会自动清理。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>kind</h5></td>
      <td>指定要使用的 Catalog Store 类型，此处应为 'generic_in_memory'</td>
    </tr>
    </tbody>
</table>

#### FileCatalogStore
FileCatalogStore 可以将用户的 Catalog 配置信息保存至文件中，使用 FileCatalogStore 需要指定 Catalog 配置需要
保存的目录，不同的 Catalog 会对应不同的文件并和 Catalog Name 一一对应。

这是一个示例目录结构，用于表示使用 `FileCatalogStore` 保存 `catalog` 配置的情况：
```shell
- /path/to/save/the/catalog/
  - catalog1.yaml
  - catalog2.yaml
  - catalog3.yaml
```

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>kind</h5></td>
      <td>指定要使用的 Catalog Store 类型，此处应为 'file'</td>
    </tr>
    <tr>
      <td><h5>path</h5></td>
      <td>指定要使用的 Catalog Store 保存的路径，必须是一个合法的目录，当前只支持本地目录</td>
    </tr>
    </tbody>
</table>

#### 用户自定义 Catalog Store
Catalog Store 是可拓展的， 用户可以通过实现 Catalog Store 的接口来自定义 Catalog Store。如果需要 SQL CLI 或者 SQL Gateway 中使用
Catalog Store，还需要这个 Catalog Store 实现对应的 CatalogStoreFactory 接口。

```java
public class CustomCatalogStoreFactory implements CatalogStoreFactory {

    public static final String IDENTIFIER = "custom-kind";
    
    // Used to connect external storage systems
    private CustomClient client;
    
    @Override
    public CatalogStore createCatalogStore() {
        return new CustomCatalogStore();
    }

    @Override
    public void open(Context context) throws CatalogException {
        // initialize the resources, such as http client
        client = initClient(context);
    }

    @Override
    public void close() throws CatalogException {
        // release the resources
    }

    @Override
    public String factoryIdentifier() {
        // table store kind identifier
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        // define the required options
        Set<ConfigOption> options = new HashSet();
        options.add(OPTION_1);
        options.add(OPTION_2);
        
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // define the optional options
    }
}

public class CustomCatalogStore extends AbstractCatalogStore {

    private Client client;
    
    public CustomCatalogStore(Client client) {
        this.client = client;
    }

    @Override
    public void storeCatalog(String catalogName, CatalogDescriptor catalog)
            throws CatalogException {
        // store the catalog
    }

    @Override
    public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        // remove the catalog descriptor
    }

    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) {
        // retrieve the catalog configuration and build the catalog descriptor
    }

    @Override
    public Set<String> listCatalogs() {
        // list all catalogs
    }

    @Override
    public boolean contains(String catalogName) {
    }
}
```
