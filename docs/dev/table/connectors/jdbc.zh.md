---
title: "JDBC SQL Connector"
nav-title: JDBC
nav-parent_id: sql-connectors
nav-pos: 9
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

<span class="label label-primary">Scan Source: Bounded</span>
<span class="label label-primary">Lookup Source: Sync Mode</span>
<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Append & Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

The JDBC connector allows for reading data from and writing data into any relational databases with a JDBC driver. This document describes how to setup the JDBC connector to run SQL queries against relational databases.

The connector operate in UPSERT mode for exchange changelog messages with the external system if a primary key is defined on the DDL, otherwise, it operates in Append mode.

Dependencies
------------

In order to setup the JDBC connector, the following table provide dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{% if site.is_stable %}

|  Maven dependency                                |  Download                                                 |  Note                |
| :----------------------------------------------- | :-------------------------------------------------------- | :----------------------|
| `flink-connector-jdbc{{site.scala_version_suffix}}`|[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) | JDBC SQL Client JAR | 
| `mysql-connector-java	`|[Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) | MySQL Driver JAR | 
| `postgresql`|[Download](https://jdbc.postgresql.org/download.html) | PostgreSQL Driver JAR | 
| `derby`|[Download](http://db.apache.org/derby/derby_downloads.html) | Derby Driver JAR | 

{% else %}

The dependency table is only available for stable releases.

{% endif %}

How to create a JDBC table
----------------

The JDBC table can be defined as follows:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/flink-test',
   'table-name' = 'jdbc_table_name'
)

{% endhighlight %}
</div>
</div>

Connector Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be 'jdbc'.</td>
    </tr>
    <tr>
      <td><h5>url</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC database url.</td>
    </tr>
    <tr>
      <td><h5>table-name</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of JDBC table to connect.</td>
    </tr>
    <tr>
      <td><h5>driver</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The class name of the JDBC driver to use to connect to this URL, if not set, it will automatically be derived from the URL.</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC user name, 'username' and 'password' must both be specified if any of them is specified.</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC password.</td>
    </tr> 
    <tr>
      <td><h5>scan.partition.column</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The column name used for partitioning the input.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.num</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The number of partitions.</td>
    </tr> 
    <tr>
      <td><h5>scan.partition.lower-bound</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The smallest value of the first partition.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.upper-bound</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The largest value of the last partition.</td>
    </tr>
    <tr>
      <td><h5>scan.fetch-size</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>The number of rows that should be fetched from the database when reading per round trip. If the value specified is zero, then the hint is ignored.</td>
    </tr>     
    <tr>
      <td><h5>lookup.cache.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max number of rows of lookup cache, over this value, the oldest rows will be expired. 'lookup.cache.max-rows' and 'lookup.cache.ttl' options must all be specified if any of them is specified. Cache is not enabled as default.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.ttl</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max time to live for each rows in lookup cache, over this time, the oldest rows will be expired. Cache is not enabled as default.</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if lookup database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>The max size of buffered records before flush. Can be set to zero to disable it.</td>
    </tr> 
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>The flush interval mills, over this time, asynchronous threads will flush data. Can be set to zero to disable it. Note, 'sink.buffer-flush.max-rows' can be set to zero with the flush interval set allowing for complete async processing of buffered actions.</td>
    </tr>
    <tr>
      <td><h5>sink.max-retries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if writing records to database failed.</td>
    </tr>          
    </tbody>
</table>

Data Type Mapping
----------------
Flink supports connect to several databases which uses dialect like MySQL, PostgresSQL, Derby. The Derby dialect usually used for test purpose. The field data type mappings from current supported databases to Flink data type are as follows mapping table, the mapping table can help define JDBC table in Flink easily.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">MySQL Data Type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">PostgreSQL Data Type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink Data Type<a href="{{ site.baseurl }}/dev/table/types.html"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT</td>
      <td></td>
      <td>TINYINT</td>
    </tr>
    <tr>
      <td>SMALLINT / TINYINT UNSIGNED</td>
      <td>SMALLINT / INT2 / SMALLSERIAL / SERIAL2</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>INT / MEDIUMINT / SMALLINT UNSIGNED</td>
      <td>INTEGER / SERIAL</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>BIGINT / INT UNSIGNED</td>
      <td>BIGINT / BIGSERIAL</td>
      <td>BIGINT</td>
    </tr>
   <tr>
      <td>BIGINT UNSIGNED</td>
      <td></td>
      <td>DECIMAL(20, 0)</td>
    </tr>    
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>REAL / FLOAT4</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE / DOUBLE PRECISION</td>
      <td>DOUBLE PRECISION / FLOAT8</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>NUMERIC(p, s) / DECIMAL(p, s)</td>
      <td>NUMERIC(p, s) / DECIMAL(p, s)</td>
      <td>DECIMAL(p, s)</td>
    </tr>
    <tr>
      <td>BOOLEAN / TINYINT(1)</td>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIME [(p)] [WITHOUT TIMEZONE]</td>
    </tr>
    <tr>
      <td>DATETIME [(p)]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
      <td>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</td>
    </tr>    
    <tr>
      <td>CHAR(n)</td>
      <td>CHAR(n) / CHARACTER(n)</td>
      <td>CHAR(n)</td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VARCHAR(n) / CHARACTER VARYING(n)</td>
      <td>VARCHAR(n)</td>
    </tr>
    <tr>
      <td>TEXT</td>
      <td>TEXT</td>
      <td>STRING</td>
    </tr>    
    <tr>
      <td>BLOB</td>
      <td>BYTEA</td>
      <td>BYTES</td>
    </tr>
    <tr>
      <td>BINARY</td>
      <td></td>
      <td>BINARY(n)</td>
    </tr>
    <tr>
      <td>VARBINARY</td>
      <td></td>
      <td>VARBINARY(n)</td>
    </tr>
    <tr>
      <td></td>
      <td>ARRAY</td>
      <td>ARRAY</td>
    </tr>                                 
    </tbody>
</table>

Features
--------
<h3>Key handling</h3>

Flink uses the primary key that defined in DDL when write data to external databases. The connector operate in UPSERT mode if the primary key was defined, otherwise, the connector operate in Append mode.

In UPSERT mode, Flink will insert a new row or update the existing row according to the primary key, Flink can ensure the idempotence in this way. To guarantee the output result is as expected, It's recommended to define primary key for the table and make sure the primary key is one of the unique key sets or primary key of the underlying database. In Append mode, Flink will interpret all records as insert rows, the insert action may fail if a primary key constraint violations happens in the underlying database.

<h3>Partitioned Scan</h3>

To accelerate reading data in parallel `Source` task instances, Flink provides partitioned scan feature for JDBC table.

These scan partition options in above options table must all be specified if any of them is specified. In addition, `scan.partition.num` must be specified. They describe how to partition the table when reading in parallel from multiple tasks. `scan.partition.column` must be a numeric, date, or timestamp column from the table in question. Notice that `scan.partition.lower-bound` and `scan.partition.upper-bound` are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.

<h3>Lookup Cache</h3>

JDBC connector can be used in temporal join as a lookup source. Currently, only sync lookup mode is supported.

The lookup cache options `lookup.cache.max-rows` and `lookup.cache.ttl` must all be specified if any of them is specified. The lookup cache is used to improve performance of temporal join JDBC connector by querying the cache first instead of send all requests to remote database. But the returned value might not be the latest if it is from the cache. So it's a balance between throughput and correctness.

<h3>Idempotent Writes</h3>

The connector works in Append mode if primary key was not defined in DDL, some redundant re-processed rows will insert to the table if a failover happened which can not not sure idempotence. The connector works in UPSERT mode and offers upsert semantic by inserting new row or updating exiting row according to primary key, the re-processed result will keep same even if a failover happened which provides idempotence.

So setting primary key is highly recommended, currently Flink supported databases offer upsert ability as follows:
<table class="table table-bordered" style="width: 60%">
    <thead>
      <tr>
        <th class="text-left">database</th>
        <th class="text-left">upsert grammar</th>
       </tr>
    </thead>    
    <tbody>
        <tr>
            <td>MySQL</td>
            <td>INSERT .. ON DUPLICATE KEY UPDATE ..</td>
        </tr>
        <tr>
            <td>PostgreSQL</td>
            <td>INSERT .. ON CONFLICT .. DO UPDATE SET ..</td>
        </tr>
    </tbody>
</table>

<h3>JdbcCatalog</h3>

The `JdbcCatalog` enables users to connect Flink to relational databases over JDBC protocol.

**PostgresCatalog**

`PostgresCatalog` is the only implementation of JDBC Catalog at the moment, `PostgresCatalog` only supports limited `Catalog` methods include:

{% highlight java %}
// The supported methods by Postgres Catalog. 
PostgresCatalog.databaseExists(String databaseName)
PostgresCatalog.listDatabases()
PostgresCatalog.getDatabase(String databaseName)
PostgresCatalog.listTables(String databaseName)
PostgresCatalog.getTable(ObjectPath tablePath)
PostgresCatalog.tableExists(ObjectPath tablePath)
{% endhighlight %}

Other `Catalog` methods is unsupported now.



**Usage of PostgresCatalog**

`PostgresCatalog` requires [flink-connector-jdbc{{site.scala_version_suffix}}](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) and [postgresql](https://jdbc.postgresql.org/download.html) dependencies for connecting to PostgreSQL database. After set proper dependencies, set a `PostgresCatalog` with the following parameters:

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

**PostgresSQL schemas**

A PostgresSQL database contains one or more [schemas](https://www.postgresql.org/docs/12/ddl-schemas.html), a schema also contain other kinds of named objects, including data types, functions, and operators. So, PostgresSQL table full path `catalog_name.database_name.schema_name.table_name` is different with common catalog table full path `catalog_name.database_name.table_name`, the default schema name is `public`.
`PostgresCatalog` supports visit both default schema tables and custom schema tables, the visit command as follows:

{% highlight java %}
// scan table 'public.test_table', the default schema can omit.
select * from mypg.mydb.test_table;
select * from mydb.test_table;
select * from test_table;

// scan 'custom_schema.test_table2', the custom schema can not omit and need escaped with table.
select * from mypg.mydb.`custom_schema.test_table2`
select * from mydb.`custom_schema.test_table2`;
select * from `custom_schema.test_table2`;
{% endhighlight %}