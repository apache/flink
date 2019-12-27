---
title: "Reading & Writing Hive Tables"
nav-parent_id: hive_tableapi
nav-pos: 2
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

Using the `HiveCatalog` and Flink's connector to Hive, Flink can read and write from Hive data as an alternative to Hive's batch engine.
Be sure to follow the instructions to include the correct [dependencies]({{ site.baseurl }}/dev/table/hive/#depedencies) in your application.

* This will be replaced by the TOC
{:toc}

## Reading From Hive

Assume Hive contains a single table in its `default` database, named people that contains several rows.

{% highlight bash %}
hive> show databases;
OK
default
Time taken: 0.841 seconds, Fetched: 1 row(s)

hive> show tables;
OK
Time taken: 0.087 seconds

hive> CREATE TABLE mytable(name string, value double);
OK
Time taken: 0.127 seconds

hive> SELECT * FROM mytable;
OK
Tom   4.72
John  8.0
Tom   24.2
Bob   3.14
Bob   4.72
Tom   34.9
Mary  4.79
Tiff  2.72
Bill  4.33
Mary  77.7
Time taken: 0.097 seconds, Fetched: 10 row(s)
{% endhighlight %}

With the data ready your can connect to Hive [connect to an existing Hive installation]({{ site.baseurl }}/dev/table/hive/#connecting-to-hive) and begin querying. 

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

# ------ Select from hive table or hive view ------ 
Flink SQL> SELECT * FROM mytable;

   name      value
__________ __________

    Tom      4.72
    John     8.0
    Tom      24.2
    Bob      3.14
    Bob      4.72
    Tom      34.9
    Mary     4.79
    Tiff     2.72
    Bill     4.33
    Mary     77.7

{% endhighlight %}

### Querying Hive views

If you need to query Hive views, please note:

1. You have to use the Hive catalog as your current catalog before you can query views in that catalog. It can be done by either `tableEnv.useCatalog(...)` in Table API or `USE CATALOG ...` in SQL Client.
2. Hive and Flink SQL have different syntax, e.g. different reserved keywords and literals. Make sure the view's query is compatible with Flink grammar.

## Writing To Hive

Similarly, data can be written into hive using an `INSERT` clause.

Consider there is an example table named "mytable" with two columns: name and age, in string and int type.

{% highlight bash %}
# ------ INSERT INTO will append to the table or partition, keeping the existing data intact ------ 
Flink SQL> INSERT INTO mytable SELECT 'Tom', 25;

# ------ INSERT OVERWRITE will overwrite any existing data in the table or partition ------ 
Flink SQL> INSERT OVERWRITE mytable SELECT 'Tom', 25;
{% endhighlight %}

We support partitioned table too, Consider there is a partitioned table named myparttable with four columns: name, age, my_type and my_date, in types ...... my_type and my_date are the partition keys.

{% highlight bash %}
# ------ Insert with static partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;

# ------ Insert with dynamic partition ------ 
Flink SQL> INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';

# ------ Insert with static(my_type) and dynamic(my_date) partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
{% endhighlight %}


## Formats

We have tested on the following of table storage formats: text, csv, SequenceFile, ORC, and Parquet.


## Optimizations

### Partition Pruning

Flink uses partition pruning as a performance optimization to limits the number of files and partitions
that Flink reads when querying Hive tables. When your data is partitioned, Flink only reads a subset of the partitions in 
a Hive table when a query matches certain filter criteria.

### Projection Pushdown

Flink leverages projection pushdown to minimize data transfer between Flink and Hive tables by omitting 
unnecessary fields from table scans.

It is especially beneficial when a table contains many columns.

### Limit Pushdown

For queries with LIMIT clause, Flink will limit the number of output records wherever possible to minimize the
amount of data transferred across network.

### ORC Vectorized Optimization upon Read

Optimization is used automatically when the following conditions are met:

- Columns without complex data type, like hive types: List, Map, Struct, Union.
- Hive version greater than or equal to version 2.0.0.

This feature is turned on by default. If there is a problem, you can use this config option to close ORC Vectorized Optimization:

{% highlight bash %}
table.exec.hive.fallback-mapred-reader=true
{% endhighlight %}


## Roadmap

We are planning and actively working on supporting features like

- ACID tables
- bucketed tables
- more formats

Please reach out to the community for more feature request https://flink.apache.org/community.html#mailing-lists
