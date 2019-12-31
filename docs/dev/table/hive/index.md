---
title: "Hive Integration"
nav-id: hive_tableapi
nav-parent_id: tableapi
nav-pos: 100
is_beta: true
nav-show_overview: true
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

[Apache Hive](https://hive.apache.org/) has established itself as a focal point of the data warehousing ecosystem.
It serves as not only a SQL engine for big data analytics and ETL, but also a data management platform, where data is discovered, defined, and evolved.

Flink offers a two-fold integration with Hive.

The first is to leverage Hive's Metastore as a persistent catalog with Flink's `HiveCatalog` for storing Flink specific metadata across sessions.
For example, users can store their Kafka or ElasticSearch tables in Hive Metastore by using `HiveCatalog`, and reuse them later on in SQL queries.

The second is to offer Flink as an alternative engine for reading and writing Hive tables.

The `HiveCatalog` is designed to be “out of the box” compatible with existing Hive installations.
You do not need to modify your existing Hive Metastore or change the data placement or partitioning of your tables.

* Note that we highly recommend users using the [blink planner]({{ site.baseurl }}/dev/table/#dependency-structure) with Hive integration.

* This will be replaced by the TOC
{:toc}

## Supported Hive Versions

Flink supports the following Hive versions.

- 1.0
    - 1.0.0
    - 1.0.1
- 1.1
    - 1.1.0
    - 1.1.1
- 1.2
    - 1.2.0
    - 1.2.1
    - 1.2.2
- 2.0
    - 2.0.0
    - 2.0.1
- 2.1
    - 2.1.0
    - 2.1.1
- 2.2
    - 2.2.0
- 2.3
    - 2.3.0
    - 2.3.1
    - 2.3.2
    - 2.3.3
    - 2.3.4
    - 2.3.5
    - 2.3.6
- 3.1
    - 3.1.0
    - 3.1.1
    - 3.1.2

Please note Hive itself have different features available for different versions, and these issues are not caused by Flink:

- Hive built-in functions are supported in 1.2.0 and later.
- Column constraints, i.e. PRIMARY KEY and NOT NULL, are supported in 3.1.0 and later.
- Altering table statistics is supported in 1.2.0 and later.
- `DATE` column statistics are supported in 1.2.0 and later.
- Writing to ORC tables is not supported in 2.0.x.

### Dependencies

To integrate with Hive, you need to add some extra dependencies to the `/lib/` directory in Flink distribution
to make the integration work in Table API program or SQL in SQL Client.
Alternatively, you can put these dependencies in a dedicated folder, and add them to classpath with the `-C`
or `-l` option for Table API program or SQL Client respectively.

Please find the required dependencies for different Hive major versions below.


<div class="codetabs" markdown="1">
<div data-lang="Hive 2.3.4" markdown="1">
{% highlight txt %}

/flink-{{ site.version }}
   /lib

       // Flink's Hive connector.Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.7.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-2.3.4.jar

{% endhighlight %}
</div>

<div data-lang="Hive 1.0.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-metastore-1.0.0.jar
       hive-exec-1.0.0.jar
       libfb303-0.9.0.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

{% endhighlight %}
</div>

<div data-lang="Hive 1.1.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-metastore-1.1.0.jar
       hive-exec-1.1.0.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

{% endhighlight %}
</div>

<div data-lang="Hive 1.2.1" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-metastore-1.2.1.jar
       hive-exec-1.2.1.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

{% endhighlight %}
</div>

<div data-lang="Hive 2.0.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.7.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-2.0.0.jar

{% endhighlight %}
</div>

<div data-lang="Hive 2.1.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.7.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-2.1.0.jar

{% endhighlight %}
</div>

<div data-lang="Hive 2.2.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.7.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-2.2.0.jar

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

{% endhighlight %}
</div>

<div data-lang="Hive 3.1.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector. Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.8.3-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-3.1.0.jar
       libfb303-0.9.3.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

{% endhighlight %}
</div>
</div>


If you are building your own program, you need the following dependencies in your mvn file.
It's recommended not to include these dependencies in the resulting jar file.
You're supposed to add dependencies as stated above at runtime.

{% highlight xml %}
<!-- Flink Dependency -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-hive{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<!-- Hive Dependency -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>${hive.version}</version>
    <scope>provided</scope>
</dependency>
{% endhighlight %}

## Connecting To Hive

Connect to an existing Hive installation using the [catalog interface]({{ site.baseurl }}/dev/table/catalogs.html) 
and [HiveCatalog]({{ site.baseurl }}/dev/table/hive/hive_catalog.html) through the table environment or YAML configuration.

If the `hive-conf/hive-site.xml` file is stored in remote storage system, users should download 
the hive configuration file to their local environment first. 

Take Hive version 2.3.4 for example:

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}

String name            = "myhive";
String defaultDatabase = "mydatabase";
String hiveConfDir     = "/opt/hive-conf"; // a local path
String version         = "2.3.4";

HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive");
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">
{% highlight scala %}

val name            = "myhive"
val defaultDatabase = "mydatabase"
val hiveConfDir     = "/opt/hive-conf" // a local path
val version         = "2.3.4"

val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
tableEnv.registerCatalog("myhive", hive)

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive")
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
{% highlight yaml %}

execution:
    planner: blink
    ...
    current-catalog: myhive  # set the HiveCatalog as the current catalog of the session
    current-database: mydatabase
    
catalogs:
   - name: myhive
     type: hive
     hive-conf-dir: /opt/hive-conf
     hive-version: 2.3.4
{% endhighlight %}
</div>
</div>


## DDL

DDL to create Hive tables, views, partitions, functions within Flink will be supported soon.

## DML

Flink supports DML writing to Hive tables. Please refer to details in [Reading & Writing Hive Tables]({{ site.baseurl }}/dev/table/hive/read_write_hive.html)

## Supported Types

Currently `HiveCatalog` supports most Flink data types with the following mapping:

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


* Hive's `CHAR(p)` has a maximum length of 255
* Hive's `VARCHAR(p)` has a maximum length of 65535
* Hive's `MAP` only supports primitive key types while Flink's `MAP` can be any data type


Note that:

* Flink doesn't support Hive's `UNION` type is not supported
* Hive's `TIMESTAMP` always has precision 9 and doesn't support other precisions. As a result, `HiveCatalog` cannot store `TIMESTAMP` columns whose precisions are not 9. Hive UDFs, on the other hand, can process `TIMESTAMP` values with a precision <= 9.
* Hive doesn't support Flink's `TIMESTAMP_WITH_TIME_ZONE`, `TIMESTAMP_WITH_LOCAL_TIME_ZONE`, and `MULTISET`
* Flink's `INTERVAL` type cannot be mapped to Hive `INTERVAL` type yet
