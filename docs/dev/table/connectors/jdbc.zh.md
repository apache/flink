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
<span class="label label-primary">Lookup Source: Async Mode</span>
<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

The JDBC connector allows for reading from and writing into an JDBC client. This document describes how to setup the JDBC Connector to run SQL queries against JDBC client.

The connector can operate in upsert mode for exchange changelog messages with the external system using a primary key defined on the DDL, the connector can also operate in append mode for exchanging only INSERT changelog messages with the external system.

**Supported Drivers:**

To use JDBC connector, need to choose an actual driver to use. Here are drivers currently supported:
<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 5%">Drvier name</th>
        <th class="text-left" style="width: 5%">Group Id</th>
        <th class="text-left" style="width: 10%">Artifact Id</th>
        <th class="text-left" style="width: 5%">JAR</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>MySQL</td>
      <td>mysql</td>
      <td>mysql-connector-java</td>
      <td><a href="https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/">Download</a></td>
    </tr>
    <tr>
      <td>PostgreSQL</td>
      <td>org.postgresql</td>
      <td>postgresql</td>
      <td><a href="https://jdbc.postgresql.org/download.html">Download</a></td>
    </tr>
    <tr>
       <td>Derby</td>
       <td>org.apache.derby</td>
       <td>derby</td>
       <td><a href="http://db.apache.org/derby/derby_downloads.html">Download</a></td>
    </tr>
    </tbody>
</table>

**Catalog**

JDBC Connector can be used together with [`JdbcCatalog`]({{ site.baseurl }}/dev/table/catalogs.html#jdbccatalog) to greatly simplify development effort and improve user experience.

Dependencies
------------

In order to setup the JDBC connector, the following table provide dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{% if site.is_stable %}

| Maven dependency                                          | SQL Client JAR         |
| :-------------------------------------------------------- | :----------------------|
| `flink-connector-jdbc{{site.scala_version_suffix}}`     | [Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc{{site.scala_version_suffix}}/{{site.version}}/flink-connector-hbase{{site.scala_version_suffix}}-{{site.version}}.jar) |

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
  a INT,
  b BIGINT,
  c DOUBLE,
  d STRING,
  e BOOLEAN,
  PRIMARY KEY (a, b) NOT ENFORCED
) WITH (
   'connector'='jdbc',
   'url'='jdbc:mysql://localhost:3306/flink-test',
   'table-name'='jdbc_table_name'
)

{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> Flink supports define primary key no longer extracts valid keys.

**Upsert mode:** If the JDBC table was defined a primary key, the sink will work on upsert mode. Please define primary key for the table and make sure the primary key is one of the unique key sets or primary key of the underlying database . This can guarantee the output result is as expected.

**Append mode:** If the JDBC table was not defined a primary key, the sink will work on append mode.

**Temporal Join:**  JDBC connector can be used in temporal join as a lookup source. Currently, only sync lookup mode is supported. The lookup cache options (`lookup.cache.max-rows` and `lookup.cache.ttl`) must all be specified if any of them is specified. The lookup cache is used to improve performance of temporal join JDBC connector by querying the cache first instead of send all requests to remote database. But the returned value might not be the latest if it is from the cache. So it's a balance between throughput and correctness. 

**Writing:** As default, the `sink.buffer-flush.interval` is `0s` and `sink.buffer-flush.max-rows` is `5000`, which means for low traffic queries, the buffered output rows may not be flushed to database for a long time. So the interval configuration is recommended to set.


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
      <td>The JDBC DB url.</td>
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
      <td>The JDBC user name, user name and password must be specified if any of them is specified.</td>
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
      <td>The column name used for partitioning the input, These scan partition options must all be specified if any of them is specified.</td>
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
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The number of rows that should be fetched from the database when reading per round trip.If the value specified is zero, then the hint is ignored. The default value is zero.</td>
    </tr>     
    <tr>
      <td><h5>lookup.cache.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max number of rows of lookup cache. "lookup.cache.max-rows" and "lookup.cache.ttl" options must all be specified if any of them is specified.</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.ttl</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max time to live for each rows in lookup cache.</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>The max retry times if lookup database failed.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Integer</td>
      <td>The max size of buffered records before flush.</td>
    </tr> 
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>The flush interval mills, over this time, asynchronous threads will flush data.</td>
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
Flink support connect to several JDBC dialect，the field type mappings between Flink and current supported databases are as follows:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Data Type</th>
        <th class="text-left">MySQL Type</th>
        <th class="text-left">PostgreSQL Type</th>
        <th class="text-left">Derby Type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>BOOLEAN</td>
      <td>TINYINT</td>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>TINYINT</td>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
      <td>SMALLINT</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT</td>
      <td>INT</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td>BIGINT</td>
      <td>BIGINT</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td>REAL</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td>DOUBLE PRECISION</td>
      <td>DOUBLE PRECISION</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>DECIMAL</td>
      <td>DECIMAL</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>CHAR</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
    </tr>
    <tr>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
    </tr>
    <tr>
      <td>STRING</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
      <td>VARCHAR</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td>DATE</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TIME</td>
      <td>TIME</td>
      <td>TIME</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>Unsupported</td>
      <td>ARRAY</td>
      <td>Unsupported</td>
    </tr>
    <tr>
      <td>BINARY</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
    </tr>
    <tr>
      <td>VARBINARY</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
    </tr>    
    <tr>
      <td>MAP/MULTISET</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
      <td>Unsupported</td>
    </tr>                     
    </tbody>
</table>