---
title: "Hive Compatibility"
is_beta: true
nav-parent_id: tableapi
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

With its wide adoption in streaming processing, Flink has also shown its potentials in batch processing. Improving Flink’s batch processing, especially in terms of SQL, would offer user a complete set of solutions for both their streaming and batch processing needs.

On the other hand, Hive has established its focal point in big data technology and its complete ecosystem. For most of big data users, Hive is not only a SQL engine for big data analytics and ETL, but also a data management platform, on which data are discovered, defined, and evolved. In another words, Hive is a de facto standard for big data on Hadoop.

Therefore, it’s imperative for Flink to integrate with Hive ecosystem to further its reach to batch and SQL users. In doing that, integration with Hive metadata and data is necessary. 

The goal here is neither to replace nor to replicate Hive. Rather, we leverage Hive as much as we can. Flink is an alternative batch engine to Hive's batch engine, and Flink SQL Cli offers a Hive-syntax-compatible SQL client. With Flink and Flink SQL, both Hive and Flink users can enjoy Hive’s rich SQL functionality and ecosystem as well as Flink's outstanding batch processing performance.


Supported Hive version
----------------------

The target version is Hive 2.3.4, which is the latest stable version.

Other versioned Hive may also be used with Flink, but there's no guarantee on compatibility.


* This will be replaced by the TOC
{:toc}


Hive Metastore Integration
--------------------------

There are two aspects of Hive metastore integration:

1. Make Hive’s meta-objects such as tables and views available to Flink and Flink is also able to create such meta-objects for and in Hive. This is achieved through `HiveCatalog`.

2. Persist Flink’s meta-objects (tables, views, and UDFs) using Hive metastore as an persistent storage. This is achieved through `GenericHiveMetastoreCatalog`, which is under active development.

For how to use `HiveCatalog` and `GenericHiveMetastoreCatalog` in Flink, see [Catalogs]({{ site.baseurl }}/dev/table/catalog.html)


Hive Data Integration
---------------------

Please refer to [Connecting to other systems]({{ site.baseurl }}/dev/batch/connectors.html) for how to connect an existing Hive service with Flink using Flink's Hive data connector. 


Example
-------

Here we present a quick example of querying Hive metadata and data in Flink.

Environment :

Assume all physical machines can be accessed in the working environment, and the following components have been successfully setup:

- Hadoop Cluster (HDFS + YARN)
- Hive 2.3.4
- Flink cluster

## Setup Flink

### Setup Flink cluster in yarn-session mode

Start a yarn session

{% highlight bash %}
$ ./bin/yarn-session.sh -n 4 -qu root.default -s 4 -tm 2048 -nm test_session_001
{% endhighlight %}

### Setup Flink cluster in local mode

Simply run

{% highlight bash %}
$ ./bin/start-cluster.sh
{% endhighlight %}

### Setup Flink SQL Cli

Let's set the SQL Cli yaml config file. For more detailed instructions on Flink SQL Cli, see [Flink SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html).

{% highlight yaml %}
execution:
    # Use batch mode
    type: batch
    time-characteristic: event-time
    periodic-watermarks-interval: 200
    # Use table result mode
    result-mode: table
    parallelism: 1
    max-parallelism: 12
    min-idle-state-retention: 0
    max-idle-state-retention: 0

deployment:
  response-timeout: 5000
  gateway-address: ""
  gateway-port: 0
  # (Optional) For users who use yarn-session mode
  yid: application_1543205128210_0045

catalogs:
   - name: myHive
     catalog:
      type: hive
      connector:
        # Hive metastore thrift uri
        Hive.metastore.uris: thrift://<ip1>:<port1>,thrift://<ip2>:<port2>
{% endhighlight %}

Note that, if users are using Flink yarn-session mode, you'll get the sessionId as `\${appId}`. Set it in `yid: ${appId}` of `deployment` section in the `conf/sql-client-defaults.yaml` file

If users are using Flink local mode, no other config is required.

Make sure all the required jars are in the `/lib` dir, including jars of `flink-connector-hive` and `flink-hadoop-compatibility`.

Get Flink SQL Cli running by execute command

{% highlight bash %}
$ ./bin/sql-client.sh embedded
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/hive-compatibility/1.png" ><img class="img-responsive" 
src="{{ site.baseurl }}/page/img/hive-compatibility/1.png" alt="SQL CLIENT"/></a>

### Prepare Hive

Assuming that Hive has been successfully set up and running, let's prepare some data in Hive.

First, we locate the current database in Hive, which is `default` in this case, and make sure no table exists in the database at this time.

{% highlight bash %}
hive> show databases;
OK
default
Time taken: 0.841 seconds, Fetched: 1 row(s)

hive> show tables;
OK
Time taken: 0.087 seconds
{% endhighlight %}

Second, we create a simple table with two columns - name as string and value as double - in a textfile format and each row is delimited by ','.

{% highlight bash %}
hive> CREATE TABLE mytable(name string, value double) row format delimited fields terminated by ',' stored as textfile;
OK
Time taken: 0.127 seconds
{% endhighlight %}

This way, we created a table named `mytable` under the `default` database in Hive.


Then let's load the data into table `mytable` and make sure it's successful. Here's some data we prepared to load into the table, and assume the file path is '/tmp/data.txt'. 

{% highlight txt %}
Tom,4.72
John,8.00
Tom,24.2
Bob,3.14
Bob,4.72
Tom,34.9
Mary,4.79
Tiff,2.72
Bill,4.33
Mary,77.7
{% endhighlight %}

Load and check data by running: 

{% highlight bash %}
hive> load data local inpath '/tmp/data.txt' into table mytable;
Loading data to table default.mytable
OK
Time taken: 0.324 seconds

hive> select * from mytable;
OK
Tom	4.72
John	8.0
Tom	24.2
Bob	3.14
Bob	4.72
Tom	34.9
Mary	4.79
Tiff	2.72
Bill	4.33
Mary	77.7
Time taken: 0.097 seconds, Fetched: 10 row(s)
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/hive-compatibility/1.png" ><img class="img-responsive" 
src="{{ site.baseurl }}/page/img/hive-compatibility/2.png" alt="hive data prepare"/></a>


### Access Hive metadata and data in Flink SQL Cli

In Flink SQL Cli, we can start using Hive.

{% highlight bash %}
# ------ See the catalog 'myhive' in the yaml config file is registered successfully and showing up here ------

Flink SQL> show catalogs;
myhive
builtin

# ------ Set the default catalog and database to be 'myhive' catalog and the 'default' database ------

Flink SQL> use myhive.default;

# ------ See the previously registered table 'mytable' ------

Flink SQL> show tables;
mytable

# ------ The table schema that Flink sees is the same that we created in Hive, two columns - name as string and value as double ------ 
Flink SQL> describe mytable;
root
 |-- name: name
 |-- type: StringType
 |-- isNullable: true
 |-- name: value
 |-- type: DoubleType
 |-- isNullable: true


Flink SQL> select * from mytable;

      name      value
__________ __________

      Tom        4.72
      John	     8.0
      Tom	     24.2
      Bob	     3.14
      Bob	     4.72
      Tom	     34.9
      Mary	     4.79
      Tiff	     2.72
      Bill	     4.33
      Mary	     77.7

{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/hive-compatibility/hive1.gif" ><img class="img-responsive" 
src="{{ site.baseurl }}/page/img/hive-compatibility/hive1.gif" alt="query hive data"/></a>


## Another Example

We have prepared two tables in Hive, order_details and products, which can be described in Hive SQL Cli:

{% highlight bash %}
Hive> describe order_details;
OK
orderid               bigint
productid             bigint
unitprice             double
quantity              int
discount              double

Hive> describe products;
OK
productid             bigint
productname           string
supplierid            bigint
categoryid            bigint
quantityperunit       string
unitprice             double
unitsinstock          bigint
unitsonorder          bigint
reorderlevel          int
discontinued          int
);
{% endhighlight %}

We can run a few SQL query to get Hive data

{% highlight sql %}
Flink SQL> select * from products;

Flink SQL> select count(*) from order_details;

Flink SQL> select
   t.productid,
   t.productname,
   sum(t.price) as sale
from
  (select
      A.productid,
      A.productname as productname,
        B.unitprice * discount as price
     from
      products as A, order_details as B
     where A.productid = B.productid) as t
  group by t.productid, t.productname;

{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/hive-compatibility/hive2.gif" ><img class="img-responsive" 
src="{{ site.baseurl }}/page/img/hive-compatibility/hive2.gif" alt="query hive data"/></a>

## Limitations & Future

Integrations of both Hive metadata and data are still in progress. Flink currently only supports reading metadata and data from Hive.
