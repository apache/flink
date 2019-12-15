---
title: "Hive Integration"
nav-id: hive_tableapi
nav-parent_id: tableapi
nav-pos: 110
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

### Dependencies

To integrate with Hive, users need some dependencies in your `/lib/` directory in Flink distribution
to make the integration work in Table API program or SQL in SQL Client.

We are using Hive 2.3.4 and 1.2.1 as examples here.


<div class="codetabs" markdown="1">
<div data-lang="Hive 2.3.4" markdown="1">
{% highlight txt %}

/flink-{{ site.version }}
   /lib
       flink-dist{{ site.scala_version_suffix }}-{{ site.version }}.jar
       flink-table{{ site.scala_version_suffix }}-{{ site.version }}.jar
       // we highly recommend using Flink's blink planner with Hive integration
       flink-table-blink{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // Pick the correct Hadoop dependency for your project.
       // Hive 2.3.4 is built with Hadoop 2.7.2. We pick 2.7.5 which flink-shaded-hadoop is pre-built with,
       // but users can pick their own hadoop version, as long as it's compatible with Hadoop 2.7.2
       flink-hadoop-compatibility{{ site.scala_version_suffix }}-{{ site.version }}.jar
       flink-shaded-hadoop-2-uber-2.7.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-exec-2.3.4.jar

       ...
{% endhighlight %}
</div>

<div data-lang="Hive 1.2.1" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib
       flink-dist{{ site.scala_version_suffix }}-{{ site.version }}.jar
       flink-table{{ site.scala_version_suffix }}-{{ site.version }}.jar
       // we highly recommend using Flink's blink planner with Hive integration
       flink-table-blink{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hadoop dependencies
       // Pick the correct Hadoop dependency for your project.
       // Hive 1.2.1 is built with Hadoop 2.6.0. We pick 2.6.5 which flink-shaded-hadoop is pre-built with,
       // but users can pick their own hadoop version, as long as it's compatible with Hadoop 2.6.0
       flink-hadoop-compatibility{{ site.scala_version_suffix }}-{{ site.version }}.jar
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

       // Hive dependencies
       hive-metastore-1.2.1.jar
       hive-exec-1.2.1.jar
       libfb303-0.9.3.jar

       ...
{% endhighlight %}
</div>
</div>


Similarly, If you are building your own program, you need the above dependencies in your mvn file.

<div class="codetabs" markdown="1">
<div data-lang="Hive 2.3.4" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-hive{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<!-- Hadoop Dependencies -->

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-hadoop-compatibility{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<!-- Pick the correct Hadoop dependency for your project.
Hive 2.3.4 is built with Hadoop 2.7.2. We pick 2.7.5 which flink-shaded-hadoop is pre-built with,
 but users can pick their own hadoop version, as long as it's compatible with Hadoop 2.7.2 -->

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-shaded-hadoop-2-uber</artifactId>
  <version>2.7.5-{{ site.shaded_version }}</version>
  <scope>provided</scope>
</dependency>

<!-- Hive Dependency -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>2.3.4</version>
</dependency>
{% endhighlight %}
</div>

<div data-lang="Hive 1.2.1" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-hive{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<!-- Hadoop Dependencies -->

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-hadoop-compatibility{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
  <scope>provided</scope>
</dependency>

<!-- Pick the correct Hadoop dependency for your project.
Hive 1.2.1 is built with Hadoop 2.6.0. We pick 2.6.5 which flink-shaded-hadoop is pre-built with,
but users can pick their own hadoop version, as long as it's compatible with Hadoop 2.6.0 -->

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-shaded-hadoop-2-uber</artifactId>
  <version>2.6.5-{{ site.shaded_version }}</version>
  <scope>provided</scope>
</dependency>

<!-- Hive Dependency -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-metastore</artifactId>
    <version>1.2.1</version>
</dependency>

<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>1.2.1</version>
</dependency>

<dependency>
    <groupId>org.apache.thrift</groupId>
    <artifactId>libfb303</artifactId>
    <version>0.9.3</version>
</dependency>
{% endhighlight %}
</div>
</div>

## Connecting To Hive

Connect to an existing Hive installation using the Hive [Catalog]({{ site.baseurl }}/dev/table/catalogs.html) through the table environment or YAML configuration.

If the `hive-conf/hive-site.xml` file is stored in remote storage system, users should download 
the hive configuration file to their local environment first. 

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}

String name            = "myhive";
String defaultDatabase = "mydatabase";
String hiveConfDir     = "/opt/hive-conf"; // a local path
String version         = "2.3.4"; // or 1.2.1

HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
tableEnv.registerCatalog("myhive", hive);
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">
{% highlight scala %}

val name            = "myhive"
val defaultDatabase = "mydatabase"
val hiveConfDir     = "/opt/hive-conf" // a local path
val version         = "2.3.4" // or 1.2.1

val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
tableEnv.registerCatalog("myhive", hive)
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
{% highlight yaml %}
catalogs:
   - name: myhive
     type: hive
     property-version: 1
     hive-conf-dir: /opt/hive-conf
     hive-version: 2.3.4 # or 1.2.1
{% endhighlight %}
</div>
</div>

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
        <td class="text-center">TIMESTAMP</td>
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
* Hive doesn't support Flink's `TIMESTAMP_WITH_TIME_ZONE`, `TIMESTAMP_WITH_LOCAL_TIME_ZONE`, and `MULTISET`
* Flink's `INTERVAL` type cannot be mapped to Hive `INTERVAL` type yet
