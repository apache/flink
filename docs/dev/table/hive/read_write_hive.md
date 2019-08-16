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

Using the `HiveCatalog` and Flink's connector to Hive, Flink can read and write from Hive data as an alternative to Hive's batch engine. Be sure to follow the instructions to include the correct [dependencies]({{ site.baseurl }}/dev/table/hive/#depedencies) in your application.  


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

## Writing To Hive

Similarly, data can be written into hive using an `INSERT INTO` clause. 

{% highlight bash %}
Flink SQL> INSERT INTO mytable (name, value) VALUES ('Tom', 4.72);
{% endhighlight %}

### Limitations

The following is a list of major limitations of the Hive connector. And we're actively working to close these gaps.

1. INSERT OVERWRITE is not supported.
2. Inserting into partitioned tables is not supported.
3. ACID tables are not supported.
4. Bucketed tables are not supported.
5. Some data types are not supported. See the [limitations]({{ site.baseurl }}/dev/table/hive/#limitations) for details.
6. Only a limited number of table storage formats have been tested, namely text, SequenceFile, ORC, and Parquet.
7. Views are not supported.
