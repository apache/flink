---
title: "Hive"
nav-id: hive_tableapi
nav-parent_id: sql-connectors
nav-pos: 15
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

* Note that we highly recommend users using the [blink planner]({% link dev/table/index.md %}#dependency-structure) with Hive integration.

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

Apache Hive is built on Hadoop, so you need to provide Hadoop dependencies, by setting the `HADOOP_CLASSPATH` 
environment variable:
```
export HADOOP_CLASSPATH=`hadoop classpath`
```

There are two ways to add Hive dependencies. First is to use Flink's bundled Hive jars. You can choose a bundled Hive jar according to the version of the metastore you use. Second is to add each of the required jars separately. The second way can be useful if the Hive version you're using is not listed here.

**NOTE**: the recommended way to add dependency is to use a bundled jar. Separate jars should be used only if bundled jars don't meet your needs.

#### Using bundled hive jar

The following tables list all available bundled hive jars. You can pick one to the `/lib/` directory in Flink distribution.

| Metastore version | Maven dependency             | SQL Client JAR         |
| :---------------- | :--------------------------- | :----------------------|
| 1.0.0 - 1.2.2     | `flink-sql-connector-hive-1.2.2` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-1.2.2{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-1.2.2{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 2.0.0 - 2.2.0     | `flink-sql-connector-hive-2.2.0` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-2.2.0{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 2.3.0 - 2.3.6     | `flink-sql-connector-hive-2.3.6` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-2.3.6{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 3.0.0 - 3.1.2     | `flink-sql-connector-hive-3.1.2` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-3.1.2{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |

#### User defined dependencies

Please find the required dependencies for different Hive major versions below.


<div class="codetabs" markdown="1">
<div data-lang="Hive 2.3.4" markdown="1">
{% highlight txt %}

/flink-{{ site.version }}
   /lib

       // Flink's Hive connector.Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-exec-2.3.4.jar

{% endhighlight %}
</div>

<div data-lang="Hive 1.0.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-metastore-1.0.0.jar
       hive-exec-1.0.0.jar
       libfb303-0.9.0.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately
       
       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

{% endhighlight %}
</div>

<div data-lang="Hive 1.1.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-metastore-1.1.0.jar
       hive-exec-1.1.0.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

{% endhighlight %}
</div>

<div data-lang="Hive 1.2.1" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-metastore-1.2.1.jar
       hive-exec-1.2.1.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

{% endhighlight %}
</div>

<div data-lang="Hive 2.0.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-exec-2.0.0.jar

{% endhighlight %}
</div>

<div data-lang="Hive 2.1.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-exec-2.1.0.jar

{% endhighlight %}
</div>

<div data-lang="Hive 2.2.0" markdown="1">
{% highlight txt %}
/flink-{{ site.version }}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

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

       // Flink's Hive connector
       flink-connector-hive{{ site.scala_version_suffix }}-{{ site.version }}.jar

       // Hive dependencies
       hive-exec-3.1.0.jar
       libfb303-0.9.3.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

{% endhighlight %}
</div>
</div>

### Program maven

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

Connect to an existing Hive installation using the [catalog interface]({% link dev/table/catalogs.md %}) 
and [HiveCatalog]({% link dev/table/connectors/hive/hive_catalog.md %}) through the table environment or YAML configuration.

Please note while HiveCatalog doesn't require a particular planner, reading/writing Hive tables only works with blink planner.
Therefore it's highly recommended that you use blink planner when connecting to your Hive warehouse.

Following is an example of how to connect to Hive:

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">

{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name            = "myhive";
String defaultDatabase = "mydatabase";
String hiveConfDir     = "/opt/hive-conf";

HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive");
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">

{% highlight scala %}

val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
val tableEnv = TableEnvironment.create(settings)

val name            = "myhive"
val defaultDatabase = "mydatabase"
val hiveConfDir     = "/opt/hive-conf"

val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
tableEnv.registerCatalog("myhive", hive)

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive")
{% endhighlight %}
</div>
<div data-lang="Python" markdown="1">
{% highlight python %}
from pyflink.table import *
from pyflink.table.catalog import HiveCatalog

settings = EnvironmentSettings.new_instance().use_blink_planner().build()
t_env = BatchTableEnvironment.create(environment_settings=settings)

catalog_name = "myhive"
default_database = "mydatabase"
hive_conf_dir = "/opt/hive-conf"

hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
t_env.register_catalog("myhive", hive_catalog)

# set the HiveCatalog as the current catalog of the session
tableEnv.use_catalog("myhive")
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
{% endhighlight %}
</div>
<div data-lang="SQL" markdown="1">
{% highlight sql %}

CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'mydatabase',
    'hive-conf-dir' = '/opt/hive-conf'
);
-- set the HiveCatalog as the current catalog of the session
USE CATALOG myhive;
{% endhighlight %}
</div>
</div>

Below are the options supported when creating a `HiveCatalog` instance with YAML file or DDL.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-center" style="width: 5%">Required</th>
      <th class="text-center" style="width: 5%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>type</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Type of the catalog. Must be set to <code>'hive'</code> when creating a HiveCatalog.</td>
    </tr>
    <tr>
      <td><h5>name</h5></td>
      <td>Yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The unique name of the catalog. Only applicable to YAML file.</td>
    </tr>
    <tr>
      <td><h5>hive-conf-dir</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>URI to your Hive conf dir containing hive-site.xml. The URI needs to be supported by Hadoop FileSystem. If the URI is relative, i.e. without a scheme, local file system is assumed. If the option is not specified, hive-site.xml is searched in class path.</td>
    </tr>
    <tr>
      <td><h5>default-database</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">default</td>
      <td>String</td>
      <td>The default database to use when the catalog is set as the current catalog.</td>
    </tr>
    <tr>
      <td><h5>hive-version</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>HiveCatalog is capable of automatically detecting the Hive version in use. It's recommended <b>NOT</b> to specify the Hive version, unless the automatic detection fails.</td>
    </tr>
    <tr>
      <td><h5>hadoop-conf-dir</h5></td>
      <td>No</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Path to Hadoop conf dir. Only local file system paths are supported. The recommended way to set Hadoop conf is via the <b>HADOOP_CONF_DIR</b> environment variable. Use the option only if environment variable doesn't work for you, e.g. if you want to configure each HiveCatalog separately.</td>
    </tr>
    </tbody>
</table>


## DDL

It's recommended to use [Hive dialect]({% link dev/table/connectors/hive/hive_dialect.md %}) to execute DDLs to create
Hive tables, views, partitions, functions within Flink.

## DML

Flink supports DML writing to Hive tables. Please refer to details in [Reading & Writing Hive Tables]({% link dev/table/connectors/hive/hive_read_write.md %})
