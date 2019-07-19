---
title: "Hive Compatibility Example"
is_beta: true
nav-parent_id: examples
nav-pos: 21
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


The following examples showcase how to use Flink SQL Client to query Hive meta-data and tables.

## Environment

Assume all physical machines are accessible in the working environment, and the following components have been successfully setup:

- Hadoop Cluster (HDFS + YARN)
- Hive 2.3.4
- Flink cluster

## Setup Flink

### Setup Flink Cluster in yarn-session mode

Start a yarn session

{% highlight bash %}
$ ./bin/yarn-session.sh -n 4 -qu root.default -s 4 -tm 2048 -nm test_session_001
{% endhighlight %}

### Setup Flink Cluster in local mode

Simply run

{% highlight bash %}
$ ./bin/start-cluster.sh
{% endhighlight %}

### Setup Flink SQL Cli

Let's set the SQL Cli yaml config file now. For more detailed instructions on Flink SQL Client and catalogs, see [Flink SQL CLI]({{ site.baseurl }}/dev/table/sqlClient.html) and [Catalogs]({{ site.baseurl }}/dev/table/catalog.html).

{% highlight yaml %}
execution:
    # Use batch mode
    type: batch
    # Use table result mode
    result-mode: table
    time-characteristic: event-time
    periodic-watermarks-interval: 200
    parallelism: 1
    max-parallelism: 12
    min-idle-state-retention: 0
    max-idle-state-retention: 0
    current-catalog: myhive
    current-database: default

deployment:
  response-timeout: 5000

catalogs:
   - name: myhive
   type: hive
   property-version: 1
   hive-conf-dir: /opt/hive-conf
   hive-version: 2.3.4
   
{% endhighlight %}


Note that, if users are using Flink yarn-session mode, you'll get the sessionId as `\${appId}`. Set it in `yid: ${appId}` of `deployment` section in the `conf/sql-client-defaults.yaml` file

If users are using Flink local mode, no other config is required.

Make sure all the required jars are in the `/lib` dir, including jars of `flink-connector-hive`, `flink-hadoop-compatibility`, `flink-shaded-hadoop-2-uber`, and Hive's jars. See [Catalogs]({{ site.baseurl }}/dev/table/catalog.html) for more details.

Get Flink SQL CLI running by execute command

{% highlight bash %}
$ ./bin/sql-client.sh embedded
{% endhighlight %}

## Flink SQL Client Examples

### Example 1

#### Prepare Hive

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

Then let's load the data into table `mytable`. Here's some data we prepared to load into the table, and assume the file path is '/tmp/data.txt'. 

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

#### Access Hive metadata and data in Flink SQL CLI

In Flink SQL CLI, we can start query Hive metadata.

{% highlight bash %}

Flink SQL> show catalogs;
myhive
default_catalog

# ------ Set the current catalog to be 'myhive' catalog if you haven't set it in the yaml file ------

Flink SQL> use catalog myhive;

# ------ See all registered database in catalog 'mytable' ------

Flink SQL> show databases;
default

# ------ See the previously registered table 'mytable' ------

Flink SQL> show tables;
mytable

# ------ The table schema that Flink sees is the same that we created in Hive, two columns - name as string and value as double ------ 
Flink SQL> describe mytable;
root
 |-- name: name
 |-- type: STRING
 |-- name: value
 |-- type: DOUBLE


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

Flink SQL> select * from mytable;

{% endhighlight %}


### Example 2

Following all the common steps above, we have prepared another two tables in Hive, order_details and products, which can be described in Hive SQL CLI:

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


Flink Table API Example
-----------------------

To be added.
