---
title: "HiveCatalog"
nav-parent_id: hive_tableapi
nav-pos: 1
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

Hive Metastore has evolved into the de facto metadata hub over the years in Hadoop ecosystem. Many companies have a single
Hive Metastore service instance in their production to manage all of their metadata, either Hive metadata or non-Hive metadata,
 as the source of truth.
 
For users who have both Hive and Flink deployments, `HiveCatalog` enables them to use Hive Metastore to manage Flink's metadata.

For users who have just Flink deployment, `HiveCatalog` is the only persistent catalog provided out-of-box by Flink.
Without a persistent catalog, users using [Flink SQL CREATE DDL]({{ site.baseurl }}/dev/table/sql/create.html) have to repeatedly
create meta-objects like a Kafka table in each session, which wastes a lot of time. `HiveCatalog` fills this gap by empowering
users to create tables and other meta-objects only once, and reference and manage them with convenience later on across sessions.


## Set up HiveCatalog

### Dependencies

Setting up a `HiveCatalog` in Flink requires the same [dependencies]({{ site.baseurl }}/dev/table/hive/#dependencies) 
as those of an overall Flink-Hive integration.

### Configuration

Setting up a `HiveCatalog` in Flink requires the same [configuration]({{ site.baseurl }}/dev/table/hive/#connecting-to-hive) 
as those of an overall Flink-Hive integration.


## How to use HiveCatalog

Once configured properly, `HiveCatalog` should just work out of box. Users can create Flink meta-objects with DDL, and should
see them immediately afterwards.

`HiveCatalog` can be used to handle two kinds of tables: Hive-compatible tables and generic tables. Hive-compatible tables
are those stored in a Hive-compatible way, in terms of both metadata and data in the storage layer. Therefore, Hive-compatible tables
created via Flink can be queried from Hive side.

Generic tables, on the other hand, are specific to Flink. When creating generic tables with `HiveCatalog`, we're just using
HMS to persist the metadata. While these tables are visible to Hive, it's unlikely Hive is able to understand
the metadata. And therefore using such tables in Hive leads to undefined behavior.

Flink uses the property '*is_generic*' to tell whether a table is Hive-compatible or generic. When creating a table with
`HiveCatalog`, it's by default considered generic. If you'd like to create a Hive-compatible table, make sure to set
`is_generic` to false in your table properties.

As stated above, generic tables shouldn't be used from Hive. In Hive CLI, you can call `DESCRIBE FORMATTED` for a table and
decide whether it's generic or not by checking the `is_generic` property. Generic tables will have `is_generic=true`.

### Example

We will walk through a simple example here.

#### step 1: set up a Hive Metastore

Have a Hive Metastore running. 

Here, we set up a local Hive Metastore and our `hive-site.xml` file in local path `/opt/hive-conf/hive-site.xml`.
We have some configs like the following:

{% highlight xml %}

<configuration>
   <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
      <description>metadata is stored in a MySQL server</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>MySQL JDBC driver class</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>...</value>
      <description>user name for connecting to mysql server</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>...</value>
      <description>password for connecting to mysql server</description>
   </property>

   <property>
       <name>hive.metastore.uris</name>
       <value>thrift://localhost:9083</value>
       <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
   </property>

   <property>
       <name>hive.metastore.schema.verification</name>
       <value>true</value>
   </property>

</configuration>
{% endhighlight %}


Test connection to the HMS with Hive Cli. Running some commands, we can see we have a database named `default` and there's no table in it.


{% highlight bash %}

hive> show databases;
OK
default
Time taken: 0.032 seconds, Fetched: 1 row(s)

hive> show tables;
OK
Time taken: 0.028 seconds, Fetched: 0 row(s)
{% endhighlight %}


#### step 2: configure Flink cluster and SQL CLI

Add all Hive dependencies to `/lib` dir in Flink distribution, and modify SQL CLI's yaml config file `sql-cli-defaults.yaml` as following:

{% highlight yaml %}

execution:
    planner: blink
    type: streaming
    ...
    current-catalog: myhive  # set the HiveCatalog as the current catalog of the session
    current-database: mydatabase
    
catalogs:
   - name: myhive
     type: hive
     hive-conf-dir: /opt/hive-conf  # contains hive-site.xml
{% endhighlight %}


#### step 3: set up a Kafka cluster

Bootstrap a local Kafka 2.3.0 cluster with a topic named "test", and produce some simple data to the topic as tuple of name and age.

{% highlight bash %}

localhost$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
>tom,15
>john,21

{% endhighlight %}


These message can be seen by starting a Kafka console consumer.

{% highlight bash %}
localhost$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

tom,15
john,21

{% endhighlight %}


#### step 4: start SQL Client, and create a Kafka table with Flink SQL DDL

Start Flink SQL Client, create a simple Kafka 2.3.0 table via DDL, and verify its schema.

{% highlight bash %}

Flink SQL> CREATE TABLE mykafka (name String, age Int) WITH (
   'connector.type' = 'kafka',
   'connector.version' = 'universal',
   'connector.topic' = 'test',
   'connector.properties.bootstrap.servers' = 'localhost:9092',
   'format.type' = 'csv',
   'update-mode' = 'append'
);
[INFO] Table has been created.

Flink SQL> DESCRIBE mykafka;
root
 |-- name: STRING
 |-- age: INT

{% endhighlight %}

Verify the table is also visible to Hive via Hive Cli, and note that the table has property `is_generic=true`:

{% highlight bash %}
hive> show tables;
OK
mykafka
Time taken: 0.038 seconds, Fetched: 1 row(s)

hive> describe formatted mykafka;
OK
# col_name            	data_type           	comment


# Detailed Table Information
Database:           	default
Owner:              	null
CreateTime:         	......
LastAccessTime:     	UNKNOWN
Retention:          	0
Location:           	......
Table Type:         	MANAGED_TABLE
Table Parameters:
	flink.connector.properties.bootstrap.servers	localhost:9092
	flink.connector.topic	test
	flink.connector.type	kafka
	flink.connector.version	universal
	flink.format.type   	csv
	flink.generic.table.schema.0.data-type	VARCHAR(2147483647)
	flink.generic.table.schema.0.name	name
	flink.generic.table.schema.1.data-type	INT
	flink.generic.table.schema.1.name	age
	flink.update-mode   	append
	is_generic          	true
	transient_lastDdlTime	......

# Storage Information
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat
OutputFormat:       	org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat
Compressed:         	No
Num Buckets:        	-1
Bucket Columns:     	[]
Sort Columns:       	[]
Storage Desc Params:
	serialization.format	1
Time taken: 0.158 seconds, Fetched: 36 row(s)

{% endhighlight %}


#### step 5: run Flink SQL to query the Kakfa table

Run a simple select query from Flink SQL Client in a Flink cluster, either standalone or yarn-session.

{% highlight bash %}
Flink SQL> select * from mykafka;

{% endhighlight %}


Produce some more messages in the Kafka topic

{% highlight bash %}
localhost$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

tom,15
john,21
kitty,30
amy,24
kaiky,18

{% endhighlight %}


You should see results produced by Flink in SQL Client now, as:


{% highlight bash %}
             SQL Query Result (Table)
 Refresh: 1 s    Page: Last of 1     

        name                       age
         tom                        15
        john                        21
       kitty                        30
         amy                        24
       kaiky                        18

{% endhighlight %}

## Supported Types

`HiveCatalog` supports all Flink types for generic tables.

For Hive-compatible tables, `HiveCatalog` needs to map Flink data types to corresponding Hive types as described in
the following table:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">Flink Data Type</th>
      <th class="text-center">Hive Data Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td class="text-center">CHAR(p)</td>
        <td class="text-center">CHAR(p)</td>
    </tr>
    <tr>
        <td class="text-center">VARCHAR(p)</td>
        <td class="text-center">VARCHAR(p)</td>
    </tr>
        <tr>
        <td class="text-center">STRING</td>
        <td class="text-center">STRING</td>
    </tr>
    <tr>
        <td class="text-center">BOOLEAN</td>
        <td class="text-center">BOOLEAN</td>
    </tr>
    <tr>
        <td class="text-center">TINYINT</td>
        <td class="text-center">TINYINT</td>
    </tr>
    <tr>
        <td class="text-center">SMALLINT</td>
        <td class="text-center">SMALLINT</td>
    </tr>
    <tr>
        <td class="text-center">INT</td>
        <td class="text-center">INT</td>
    </tr>
    <tr>
        <td class="text-center">BIGINT</td>
        <td class="text-center">LONG</td>
    </tr>
    <tr>
        <td class="text-center">FLOAT</td>
        <td class="text-center">FLOAT</td>
    </tr>
    <tr>
        <td class="text-center">DOUBLE</td>
        <td class="text-center">DOUBLE</td>
    </tr>
    <tr>
        <td class="text-center">DECIMAL(p, s)</td>
        <td class="text-center">DECIMAL(p, s)</td>
    </tr>
    <tr>
        <td class="text-center">DATE</td>
        <td class="text-center">DATE</td>
    </tr>
    <tr>
        <td class="text-center">TIMESTAMP(9)</td>
        <td class="text-center">TIMESTAMP</td>
    </tr>
    <tr>
        <td class="text-center">BYTES</td>
        <td class="text-center">BINARY</td>
    </tr>
    <tr>
        <td class="text-center">ARRAY&lt;T&gt;</td>
        <td class="text-center">LIST&lt;T&gt;</td>
    </tr>
    <tr>
        <td class="text-center">MAP<K, V></td>
        <td class="text-center">MAP<K, V></td>
    </tr>
    <tr>
        <td class="text-center">ROW</td>
        <td class="text-center">STRUCT</td>
    </tr>
  </tbody>
</table>

Something to note about the type mapping:
* Hive's `CHAR(p)` has a maximum length of 255
* Hive's `VARCHAR(p)` has a maximum length of 65535
* Hive's `MAP` only supports primitive key types while Flink's `MAP` can be any data type
* Hive's `UNION` type is not supported
* Hive's `TIMESTAMP` always has precision 9 and doesn't support other precisions. Hive UDFs, on the other hand, can process `TIMESTAMP` values with a precision <= 9.
* Hive doesn't support Flink's `TIMESTAMP_WITH_TIME_ZONE`, `TIMESTAMP_WITH_LOCAL_TIME_ZONE`, and `MULTISET`
* Flink's `INTERVAL` type cannot be mapped to Hive `INTERVAL` type yet

## Scala Shell

注意：目前 blink planner 还不能很好的支持 Scala Shell，因此 **不** 建议在 Scala Shell 中使用 Hive 连接器。
