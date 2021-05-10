---
title: JDBC
weight: 6
type: docs
aliases:
  - /dev/table/connectors/jdbc.html
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

# JDBC SQL Connector

{{< label "Scan Source: Bounded" >}}
{{< label "Lookup Source: Sync Mode" >}}
{{< label "Sink: Batch" >}}
{{< label "Sink: Streaming Append & Upsert Mode" >}}

The JDBC connector allows for reading data from and writing data into any relational databases with a JDBC driver. This document describes how to setup the JDBC connector to run SQL queries against relational databases.

The JDBC sink operate in upsert mode for exchange UPDATE/DELETE messages with the external system if a primary key is defined on the DDL, otherwise, it operates in append mode and doesn't support to consume UPDATE/DELETE messages.

Dependencies
------------

{{< sql_download_table "jdbc" >}}

The JDBC connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

A driver dependency is also required to connect to a specified database. Here are drivers currently supported:

| Driver      |      Group Id      |      Artifact Id       |      JAR         |
| :-----------| :------------------| :----------------------| :----------------|
| MySQL       |       `mysql`      | `mysql-connector-java` | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) |
| Oracle      | `com.oracle.database.jdbc` |        `ojdbc8`        | [Download](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) |
| PostgreSQL  |  `org.postgresql`  |      `postgresql`      | [Download](https://jdbc.postgresql.org/download.html) |
| Derby       | `org.apache.derby` |        `derby`         | [Download](http://db.apache.org/derby/derby_downloads.html) |


JDBC connector and drivers are not part of Flink's binary distribution. See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration" >}}).


How to create a JDBC table
----------------

The JDBC table can be defined as following:

```sql
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
```

Connector Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use, here should be <code>'jdbc'</code>.</td>
    </tr>
    <tr>
      <td><h5>url</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC database url.</td>
    </tr>
    <tr>
      <td><h5>table-name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of JDBC table to connect.</td>
    </tr>
    <tr>
      <td><h5>driver</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The class name of the JDBC driver to use to connect to this URL, if not set, it will automatically be derived from the URL.</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC user name. <code>'username'</code> and <code>'password'</code> must both be specified if any of them is specified.</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The JDBC password.</td>
    </tr>
    <tr>
      <td><h5>connection.max-retry-timeout</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">60 s</td>
      <td>Duration</td>
      <td>Maximum timeout between retries. The timeout should be in second granularity and shouldn't be smaller than 1 second.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.column</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The column name used for partitioning the input. See the following <a href="#partitioned-scan">Partitioned Scan</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.num</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The number of partitions.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.lower-bound</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The smallest value of the first partition.</td>
    </tr>
    <tr>
      <td><h5>scan.partition.upper-bound</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The largest value of the last partition.</td>
    </tr>
    <tr>
      <td><h5>scan.fetch-size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>The number of rows that should be fetched from the database when reading per round trip. If the value specified is zero, then the hint is ignored.</td>
    </tr>
    <tr>
      <td><h5>scan.auto-commit</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Sets the <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#commit_transactions">auto-commit</a> flag on the JDBC driver,
      which determines whether each statement is committed in a transaction automatically. Some JDBC drivers, specifically
      <a href="https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor">Postgres</a>, may require this to be set to false in order to stream results.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.max-rows</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max number of rows of lookup cache, over this value, the oldest rows will be expired.
      Lookup cache is disabled by default. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.ttl</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The max time to live for each rows in lookup cache, over this time, the oldest rows will be expired.
      Lookup cache is disabled by default. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details. </td>
    </tr>
    <tr>
      <td><h5>lookup.cache.caching-missing-key</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Flag to cache missing key, true by default</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if lookup database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>The max size of buffered records before flush. Can be set to zero to disable it.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">1 s</td>
      <td>Duration</td>
      <td>The flush interval mills, over this time, asynchronous threads will flush data. Can be set to <code>'0'</code> to disable it. Note, <code>'sink.buffer-flush.max-rows'</code> can be set to <code>'0'</code> with the flush interval set allowing for complete async processing of buffered actions.</td>
    </tr>
    <tr>
      <td><h5>sink.max-retries</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The max retry times if writing records to database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the JDBC sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>          
    </tbody>
</table>

Features
--------

### Key handling

Flink uses the primary key that was defined in DDL when writing data to external databases. The connector operates in upsert mode if the primary key was defined, otherwise, the connector operates in append mode.

In upsert mode, Flink will insert a new row or update the existing row according to the primary key, Flink can ensure the idempotence in this way. To guarantee the output result is as expected, it's recommended to define primary key for the table and make sure the primary key is one of the unique key sets or primary key of the underlying database table. In append mode, Flink will interpret all records as INSERT messages, the INSERT operation may fail if a primary key or unique constraint violation happens in the underlying database.

See [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table) for more details about PRIMARY KEY syntax.

### Partitioned Scan

To accelerate reading data in parallel `Source` task instances, Flink provides partitioned scan feature for JDBC table.

All the following scan partition options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple tasks.
The `scan.partition.column` must be a numeric, date, or timestamp column from the table in question. Notice that `scan.partition.lower-bound` and `scan.partition.upper-bound` are used to decide the partition stride and filter the rows in table. If it is a batch job, it also doable to get the max and min value first before submitting the flink job.

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

By default, flink will cache the empty query result for a Primary key, you can toggle the behaviour by setting `lookup.cache.caching-missing-key` to false. 

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
            <td>Oracle</td>
            <td>MERGE INTO .. USING (..) ON (..) <br>
                WHEN MATCHED THEN UPDATE SET (..) <br>
                WHEN NOT MATCHED THEN INSERT (..) <br>
                VALUES (..)</td>
        </tr>
        <tr>
            <td>PostgreSQL</td>
            <td>INSERT .. ON CONFLICT .. DO UPDATE SET ..</td>
        </tr>
    </tbody>
</table>

JDBC Catalog
------------

The `JdbcCatalog` enables users to connect Flink to relational databases over JDBC protocol.

Currently, there are two JDBC catalog implementations, Postgres Catalog and MySQL Catalog. They support the following catalog methods. Other methods are currently not supported.

```java
// The supported methods by Postgres & MySQL Catalog.
databaseExists(String databaseName);
listDatabases();
getDatabase(String databaseName);
listTables(String databaseName);
getTable(ObjectPath tablePath);
tableExists(ObjectPath tablePath);
```

Other `Catalog` methods are currently not supported.

### Usage of JDBC Catalog

The section mainly describes how to create and use a Postgres Catalog or MySQL Catalog.
Please refer to [Dependencies](#dependencies) section for how to setup a JDBC connector and the corresponding driver.

The JDBC catalog supports the following options:
- `name`: required, name of the catalog.
- `default-database`: required, default database to connect to.
- `username`: required, username of Postgres/MySQL account.
- `password`: required, password of the account.
- `base-url`: required (should not contain the database name)
  - for Postgres Catalog this should be `"jdbc:postgresql://<ip>:<port>"`
  - for MySQL Catalog this should be `"jdbc:mysql://<ip>:<port>"`

{{< tabs "10bd8bfb-674c-46aa-8a36-385537df5791" >}}
{{< tab "SQL" >}}
```sql
CREATE CATALOG my_catalog WITH(
    'type' = 'jdbc',
    'default-database' = '...',
    'username' = '...',
    'password' = '...',
    'base-url' = '...'
);

USE CATALOG my_catalog;
```
{{< /tab >}}
{{< tab "Java" >}}
```java

EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name            = "my_catalog";
String defaultDatabase = "mydb";
String username        = "...";
String password        = "...";
String baseUrl         = "..."

JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
tableEnv.registerCatalog("my_catalog", catalog);

// set the JdbcCatalog as the current catalog of the session
tableEnv.useCatalog("my_catalog");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val settings = EnvironmentSettings.inStreamingMode()
val tableEnv = TableEnvironment.create(settings)

val name            = "my_catalog"
val defaultDatabase = "mydb"
val username        = "..."
val password        = "..."
val baseUrl         = "..."

val catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl)
tableEnv.registerCatalog("my_catalog", catalog)

// set the JdbcCatalog as the current catalog of the session
tableEnv.useCatalog("my_catalog")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.catalog import JdbcCatalog

environment_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings)

name = "my_catalog"
default_database = "mydb"
username = "..."
password = "..."
base_url = "..."

catalog = JdbcCatalog(name, default_database, username, password, base_url)
t_env.register_catalog("my_catalog", catalog)

# set the JdbcCatalog as the current catalog of the session
t_env.use_catalog("my_catalog")
```
{{< /tab >}}
{{< tab "YAML" >}}
```yaml

execution:
    ...
    current-catalog: my_catalog  # set the target JdbcCatalog as the current catalog of the session
    current-database: mydb

catalogs:
   - name: my_catalog
     type: jdbc
     default-database: mydb
     username: ...
     password: ...
     base-url: ...
```
{{< /tab >}}
{{< /tabs >}}

### JDBC Catalog for PostgreSQL

#### PostgreSQL Metaspace Mapping

PostgreSQL has an additional namespace as `schema` besides database. A Postgres instance can have multiple databases, each database can have multiple schemas with a default one named "public", each schema can have multiple tables.
In Flink, when querying tables registered by Postgres catalog, users can use either `schema_name.table_name` or just `table_name`. The `schema_name` is optional and defaults to "public".

Therefore, the metaspace mapping between Flink Catalog and Postgres is as following:

| Flink Catalog Metaspace Structure    |   Postgres Metaspace Structure      |
| :------------------------------------| :-----------------------------------|
| catalog name (defined in Flink only) | N/A                                 |
| database name                        | database name                       |
| table name                           | [schema_name.]table_name            |

The full path of Postgres table in Flink should be ``"<catalog>.<db>.`<schema.table>`"`` if schema is specified, note the `<schema.table>` should be escaped.

Here are some examples to access Postgres tables:

```sql
-- scan table 'test_table' of 'public' schema (i.e. the default schema), the schema name can be omitted
SELECT * FROM mypg.mydb.test_table;
SELECT * FROM mydb.test_table;
SELECT * FROM test_table;

-- scan table 'test_table2' of 'custom_schema' schema,
-- the custom schema can not be omitted and must be escaped with table.
SELECT * FROM mypg.mydb.`custom_schema.test_table2`
SELECT * FROM mydb.`custom_schema.test_table2`;
SELECT * FROM `custom_schema.test_table2`;
```

### JDBC Catalog for MySQL

#### MySQL Metaspace Mapping

The databases in a MySQL instance are at the same mapping level as the databases under the catalog registered with MySQL Catalog. A MySQL instance can have multiple databases, each database can have multiple tables.
In Flink, when querying tables registered by MySQL catalog, users can use either `database.table_name` or just `table_name`. The default value is the default database specified when MySQL Catalog was created.

Therefore, the metaspace mapping between Flink Catalog and MySQL Catalog is as following:

| Flink Catalog Metaspace Structure    |   MySQL Metaspace Structure         |
| :------------------------------------| :-----------------------------------|
| catalog name (defined in Flink only) | N/A                                 |
| database name                        | database name                       |
| table name                           | table_name                          |

The full path of MySQL table in Flink should be ``"`<catalog>`.`<db>`.`<table>`"``.

Here are some examples to access MySQL tables:

```sql
-- scan table 'test_table', the default database is 'mydb'.
SELECT * FROM mysql_catalog.mydb.test_table;
SELECT * FROM mydb.test_table;
SELECT * FROM test_table;

-- scan table 'test_table' with the given database.
SELECT * FROM mysql_catalog.given_database.test_table2;
SELECT * FROM given_database.test_table2;
```

Data Type Mapping
----------------
Flink supports connect to several databases which uses dialect like MySQL, Oracle, PostgreSQL, Derby. The Derby dialect usually used for testing purpose. The field data type mappings from relational databases data types to Flink SQL data types are listed in the following table, the mapping table can help define JDBC table in Flink easily.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left"><a href="https://dev.mysql.com/doc/refman/8.0/en/data-types.html">MySQL type</a></th>
        <th class="text-left"><a href="https://docs.oracle.com/database/121/SQLRF/sql_elements001.htm#SQLRF30020">Oracle type</a></th>
        <th class="text-left"><a href="https://www.postgresql.org/docs/12/datatype.html">PostgreSQL type</a></th>
        <th class="text-left"><a href="{{< ref "docs/dev/table/types" >}}">Flink SQL type</a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>TINYINT</code></td>
      <td></td>
      <td></td>
      <td><code>TINYINT</code></td>
    </tr>
    <tr>
      <td>
        <code>SMALLINT</code><br>
        <code>TINYINT UNSIGNED</code></td>
      <td></td>
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
      <td></td>
      <td>
        <code>INTEGER</code><br>
        <code>SERIAL</code></td>
      <td><code>INT</code></td>
    </tr>
    <tr>
      <td>
        <code>BIGINT</code><br>
        <code>INT UNSIGNED</code></td>
      <td></td>
      <td>
        <code>BIGINT</code><br>
        <code>BIGSERIAL</code></td>
      <td><code>BIGINT</code></td>
    </tr>
   <tr>
      <td><code>BIGINT UNSIGNED</code></td>
      <td></td>
      <td></td>
      <td><code>DECIMAL(20, 0)</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td></td>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>
        <code>BINARY_FLOAT</code></td>
      <td>
        <code>REAL</code><br>
        <code>FLOAT4</code></td>
      <td><code>FLOAT</code></td>
    </tr>
    <tr>
      <td>
        <code>DOUBLE</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td><code>BINARY_DOUBLE</code></td>
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
        <code>SMALLINT</code><br> 
        <code>FLOAT(s)</code><br> 
        <code>DOUBLE PRECISION</code><br> 
        <code>REAL</code><br>
        <code>NUMBER(p, s)</code></td>
      <td>
        <code>NUMERIC(p, s)</code><br>
        <code>DECIMAL(p, s)</code></td>
      <td><code>DECIMAL(p, s)</code></td>
    </tr>
    <tr>
      <td>
        <code>BOOLEAN</code><br>
        <code>TINYINT(1)</code></td>
      <td></td>
      <td><code>BOOLEAN</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
    </tr>
    <tr>
      <td><code>TIME [(p)]</code></td>
      <td><code>DATE</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td><code>DATETIME [(p)]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
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
        <code>VARCHAR(n)</code><br>
        <code>CLOB</code></td>
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
      <td>
        <code>RAW(s)</code><br>
        <code>BLOB</code></td>
      <td><code>BYTEA</code></td>
      <td><code>BYTES</code></td>
    </tr>
    <tr>
      <td></td>
      <td></td>
      <td><code>ARRAY</code></td>
      <td><code>ARRAY</code></td>
    </tr>
    </tbody>
</table>

{{< top >}}
