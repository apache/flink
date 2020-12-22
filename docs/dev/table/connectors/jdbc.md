---
title: "JDBC SQL Connector"
nav-title: JDBC
nav-parent_id: sql-connectors
nav-pos: 5
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

The JDBC sink operate in upsert mode for exchange UPDATE/DELETE messages with the external system if a primary key is defined on the DDL, otherwise, it operates in append mode and doesn't support to consume UPDATE/DELETE messages.

Dependencies
------------

{% assign connector = site.data.sql-connectors['jdbc'] %} 
{% include sql-connector-download-table.html 
    connector=connector
%}

<br>
A driver dependency is also required to connect to a specified database. Here are drivers currently supported:

| Driver      |      Group Id      |      Artifact Id       |      JAR         |
| :-----------| :------------------| :----------------------| :----------------|
| MySQL       |       `mysql`      | `mysql-connector-java` | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) |
| PostgreSQL  |  `org.postgresql`  |      `postgresql`      | [Download](https://jdbc.postgresql.org/download.html) |
| Derby       | `org.apache.derby` |        `derby`         | [Download](http://db.apache.org/derby/derby_downloads.html) |

<br>
JDBC connector and drivers are not currently part of Flink's binary distribution. See how to link with them for cluster execution [here]({% link dev/project-configuration.md %}).


How to create a JDBC table
----------------

The JDBC table can be defined as following:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- register a MySQL table 'users' in Flink SQL
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
   'table-name' = 'users'
);

-- write data into the JDBC table from the other table "T"
INSERT INTO MyUserTable
SELECT id, name, age, status FROM T;

-- scan data from the JDBC table
SELECT id, name, age, status FROM MyUserTable;

-- temporal join the JDBC table as a dimension table
SELECT * FROM myTopic
LEFT JOIN MyUserTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = MyUserTable.id;
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
      <td>Specify what connector to use, here should be <code>'jdbc'</code>.</td>
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
      <td>The JDBC user name. <code>'username'</code> and <code>'password'</code> must both be specified if any of them is specified.</td>
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
      <td>The column name used for partitioning the input. See the following <a href="#partitioned-scan">Partitioned Scan</a> section for more details.</td>
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
      <td><h5>scan.auto-commit</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Sets the <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#commit_transactions">auto-commit</a> flag on the JDBC driver,
      which determines whether each statement is committed in a transaction automatically. Some JDBC drivers, specifically
      <a href="https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor">Postgres</a>, may require this to be set to false in order to stream results.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max number of rows of lookup cache, over this value, the oldest rows will be expired.
      Lookup cache is disabled by default. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.ttl</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache, over this time, the oldest rows will be expired.
      Lookup cache is disabled by default. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details. </td>
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
      <td>The flush interval mills, over this time, asynchronous threads will flush data. Can be set to <code>'0'</code> to disable it. Note, <code>'sink.buffer-flush.max-rows'</code> can be set to <code>'0'</code> with the flush interval set allowing for complete async processing of buffered actions.</td>
    </tr>
    <tr>
      <td><h5>sink.max-retries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if writing records to database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the JDBC sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>          
    </tbody>
</table>

Features
--------

### Key handling

Flink uses the primary key that defined in DDL when writing data to external databases. The connector operate in upsert mode if the primary key was defined, otherwise, the connector operate in append mode.

In upsert mode, Flink will insert a new row or update the existing row according to the primary key, Flink can ensure the idempotence in this way. To guarantee the output result is as expected, it's recommended to define primary key for the table and make sure the primary key is one of the unique key sets or primary key of the underlying database table. In append mode, Flink will interpret all records as INSERT messages, the INSERT operation may fail if a primary key or unique constraint violation happens in the underlying database.

See [CREATE TABLE DDL]({% link dev/table/sql/create.md %}#create-table) for more details about PRIMARY KEY syntax.

### Partitioned Scan

To accelerate reading data in parallel `Source` task instances, Flink provides partitioned scan feature for JDBC table.

All the following scan partition options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple tasks.
The `scan.partition.column` must be a numeric, date, or timestamp column from the table in question. Notice that `scan.partition.lower-bound` and `scan.partition.upper-bound` are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.

- `scan.partition.column`: The column name used for partitioning the input.
- `scan.partition.num`: The number of partitions.
- `scan.partition.lower-bound`: The smallest value of the first partition.
- `scan.partition.upper-bound`: The largest value of the last partition.

### Lookup Cache

JDBC connector can be used in temporal join as a lookup source (aka. dimension table). Currently, only sync lookup mode is supported.

By default, lookup cache is not enabled. You can enable it by setting both `lookup.cache.max-rows` and `lookup.cache.ttl`.

The lookup cache is used to improve performance of temporal join the JDBC connector. By default, lookup cache is not enabled, so all the requests are sent to external database.
When lookup cache is enabled, each process (i.e. TaskManager) will hold a cache. Flink will lookup the cache first, and only send requests to external database when cache missing, and update cache with the rows returned.
The oldest rows in cache will be expired when the cache hit to the max cached rows `lookup.cache.max-rows` or when the row exceeds the max time to live `lookup.cache.ttl`.
The cached rows might not be the latest, users can tune `lookup.cache.ttl` to a smaller value to have a better fresh data, but this may increase the number of requests send to database. So this is a balance between throughput and correctness.

### Idempotent Writes

JDBC sink will use upsert semantics rather than plain INSERT statements if primary key is defined in DDL. Upsert semantics refer to atomically adding a new row or updating the existing row if there is a unique constraint violation in the underlying database, which provides idempotence.

If there are failures, the Flink job will recover and re-process from last successful checkpoint, which can lead to re-processing messages during recovery. The upsert mode is highly recommended as it helps avoid constraint violations or duplicate data if records need to be re-processed.

Aside from failure recovery, the source topic may also naturally contain multiple records over time with the same primary key, making upserts desirable.

As there is no standard syntax for upsert, the following table describes the database-specific DML that is used.

<table class="table table-bordered" style="width: 60%">
    <thead>
      <tr>
        <th class="text-left">Database</th>
        <th class="text-left">Upsert Grammar</th>
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

### Postgres Database as a Catalog

The `JdbcCatalog` enables users to connect Flink to relational databases over JDBC protocol.

Currently, `PostgresCatalog` is the only implementation of JDBC Catalog at the moment, `PostgresCatalog` only supports limited `Catalog` methods include:

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

#### Usage of PostgresCatalog

Please refer to [Dependencies](#dependencies) section for how to setup a JDBC connector and Postgres driver.

Postgres catalog supports the following options:
- `name`: required, name of the catalog.
- `default-database`: required, default database to connect to.
- `username`: required, username of Postgres account.
- `password`: required, password of the account.
- `base-url`: required, should be of format `"jdbc:postgresql://<ip>:<port>"`, and should not contain database name here.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE CATALOG mypg WITH(
    'type' = 'jdbc',
    'default-database' = '...',
    'username' = '...',
    'password' = '...',
    'base-url' = '...'
);

USE CATALOG mypg;
{% endhighlight %}
</div>
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
<div data-lang="Python" markdown="1">
{% highlight python %}
from pyflink.table.catalog import JdbcCatalog

environment_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=environment_settings)

name = "mypg"
default_database = "mydb"
username = "..."
password = "..."
base_url = "..."

catalog = JdbcCatalog(name, default_database, username, password, base_url)
t_env.register_catalog("mypg", catalog)

# set the JdbcCatalog as the current catalog of the session
t_env.use_catalog("mypg")
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

#### PostgresSQL Metaspace Mapping

PostgresSQL has an additional namespace as `schema` besides database. A Postgres instance can have multiple databases, each database can have multiple schemas with a default one named "public", each schema can have multiple tables.
In Flink, when querying tables registered by Postgres catalog, users can use either `schema_name.table_name` or just `table_name`. The `schema_name` is optional and defaults to "public".

Therefor the metaspace mapping between Flink Catalog and Postgres is as following:

| Flink Catalog Metaspace Structure    |   Postgres Metaspace Structure      |
| :------------------------------------| :-----------------------------------|
| catalog name (defined in Flink only) | N/A                                 |
| database name                        | database name                       |
| table name                           | [schema_name.]table_name            |

The full path of Postgres table in Flink should be ``"<catalog>.<db>.`<schema.table>`"`` if schema is specified, note the `<schema.table>` should be escaped.

Here are some examples to access Postgres tables:

{% highlight sql %}
-- scan table 'test_table' of 'public' schema (i.e. the default schema), the schema name can be omitted
SELECT * FROM mypg.mydb.test_table;
SELECT * FROM mydb.test_table;
SELECT * FROM test_table;

-- scan table 'test_table2' of 'custom_schema' schema,
-- the custom schema can not be omitted and must be escaped with table.
SELECT * FROM mypg.mydb.`custom_schema.test_table2`
SELECT * FROM mydb.`custom_schema.test_table2`;
SELECT * FROM `custom_schema.test_table2`;
{% endhighlight %}

Data Type Mapping
----------------
Flink supports connect to several databases which uses dialect like MySQL, PostgresSQL, Derby. The Derby dialect usually used for testing purpose. The field data type mappings from relational databases data types to Flink SQL data types are listed in the following table, the mapping table can help define JDBC table in Flink easily.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>TINYINT</code></td>
      <td></td>
      <td><code>TINYINT</code></td>
    </tr>
    <tr>
      <td>
        <code>SMALLINT</code><br>
        <code>TINYINT UNSIGNED</code></td>
      <td>
        <code>SMALLINT</code><br>
        <code>INT2</code><br>
        <code>SMALLSERIAL</code><br>
        <code>SERIAL2</code></td>
      <td><code>SMALLINT</code></td>
    </tr>
    <tr>
      <td>
        <code>INT</code><br>
        <code>MEDIUMINT</code><br>
        <code>SMALLINT UNSIGNED</code></td>
      <td>
        <code>INTEGER</code><br>
        <code>SERIAL</code></td>
      <td><code>INT</code></td>
    </tr>
    <tr>
      <td>
        <code>BIGINT</code><br>
        <code>INT UNSIGNED</code></td>
      <td>
        <code>BIGINT</code><br>
        <code>BIGSERIAL</code></td>
      <td><code>BIGINT</code></td>
    </tr>
   <tr>
      <td><code>BIGINT UNSIGNED</code></td>
      <td></td>
      <td><code>DECIMAL(20, 0)</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>
        <code>REAL</code><br>
        <code>FLOAT4</code></td>
      <td><code>FLOAT</code></td>
    </tr>
    <tr>
      <td>
        <code>DOUBLE</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td>
        <code>FLOAT8</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td><code>DOUBLE</code></td>
    </tr>
    <tr>
      <td>
        <code>NUMERIC(p, s)</code><br>
         <code>DECIMAL(p, s)</code></td>
      <td>
        <code>NUMERIC(p, s)</code><br>
        <code>DECIMAL(p, s)</code></td>
      <td><code>DECIMAL(p, s)</code></td>
    </tr>
    <tr>
      <td>
        <code>BOOLEAN</code><br>
         <code>TINYINT(1)</code></td>
      <td><code>BOOLEAN</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
    </tr>
    <tr>
      <td><code>TIME [(p)]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td><code>DATETIME [(p)]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td>
        <code>CHAR(n)</code><br>
        <code>VARCHAR(n)</code><br>
        <code>TEXT</code></td>
      <td>
        <code>CHAR(n)</code><br>
        <code>CHARACTER(n)</code><br>
        <code>VARCHAR(n)</code><br>
        <code>CHARACTER VARYING(n)</code><br>
        <code>TEXT</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td>
        <code>BINARY</code><br>
        <code>VARBINARY</code><br>
        <code>BLOB</code></td>
      <td><code>BYTEA</code></td>
      <td><code>BYTES</code></td>
    </tr>
    <tr>
      <td></td>
      <td><code>ARRAY</code></td>
      <td><code>ARRAY</code></td>
    </tr>
    </tbody>
</table>

{% top %}
