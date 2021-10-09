---
title: "Overview"
weight: 1
type: docs
aliases:
  - docs/connectors/table/hive/
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

# Apache Hive

[Apache Hive](https://hive.apache.org/) has established itself as a focal point of the data warehousing ecosystem.
It serves as not only a SQL engine for big data analytics and ETL, but also a data management platform, where data is discovered, defined, and evolved.

Flink offers a two-fold integration with Hive.

The first is to leverage Hive's Metastore as a persistent catalog with Flink's `HiveCatalog` for storing Flink specific metadata across sessions.
For example, users can store their Kafka or ElasticSearch tables in Hive Metastore by using `HiveCatalog`, and reuse them later on in SQL queries.

The second is to offer Flink as an alternative engine for reading and writing Hive tables.

The `HiveCatalog` is designed to be “out of the box” compatible with existing Hive installations.
You do not need to modify your existing Hive Metastore or change the data placement or partitioning of your tables.

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
| 1.0.0 - 1.2.2     | `flink-sql-connector-hive-1.2.2` | {{< stable >}}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-1.2.2{{< scala_version >}}/{{< version >}}/flink-sql-connector-hive-1.2.2{{< scala_version >}}-{{< version >}}.jar) {{< /stable >}}{{< unstable >}} Only available for stable releases {{< /unstable >}} |
| 2.0.0 - 2.2.0     | `flink-sql-connector-hive-2.2.0` | {{< stable >}}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0{{< scala_version >}}/{{< version >}}/flink-sql-connector-hive-2.2.0{{< scala_version >}}-{{< version >}}.jar) {{< /stable >}}{{< unstable >}} Only available for stable releases {{< /unstable >}} |
| 2.3.0 - 2.3.6     | `flink-sql-connector-hive-2.3.6` | {{< stable >}}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6{{< scala_version >}}/{{< version >}}/flink-sql-connector-hive-2.3.6{{< scala_version >}}-{{< version >}}.jar) {{< /stable >}}{{< unstable >}} Only available for stable releases {{< /unstable >}} |
| 3.0.0 - 3.1.2     | `flink-sql-connector-hive-3.1.2` | {{< stable >}}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2{{< scala_version >}}/{{< version >}}/flink-sql-connector-hive-3.1.2{{< scala_version >}}-{{< version >}}.jar) {{< /stable >}}{{< unstable >}} Only available for stable releases {{< /unstable >}} |

#### User defined dependencies

Please find the required dependencies for different Hive major versions below.

{{< tabs "8623cd64-8623-4922-92d2-ee82ec410d96" >}}
{{< tab "Hive 2.3.4" >}}
```txt

/flink-{{< version >}}
   /lib

       // Flink's Hive connector.Contains flink-hadoop-compatibility and flink-orc jars
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-exec-2.3.4.jar

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 1.0.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-metastore-1.0.0.jar
       hive-exec-1.0.0.jar
       libfb303-0.9.0.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately
       
       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 1.1.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-metastore-1.1.0.jar
       hive-exec-1.1.0.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 1.2.1" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-metastore-1.2.1.jar
       hive-exec-1.2.1.jar
       libfb303-0.9.2.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3-nohive.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 2.0.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-exec-2.0.0.jar

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 2.1.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-exec-2.1.0.jar

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 2.2.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-exec-2.2.0.jar

       // Orc dependencies -- required by the ORC vectorized optimizations
       orc-core-1.4.3.jar
       aircompressor-0.8.jar // transitive dependency of orc-core

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< tab "Hive 3.1.0" >}}
```txt
/flink-{{< version >}}
   /lib

       // Flink's Hive connector
       flink-connector-hive{{< scala_version >}}-{{< version >}}.jar

       // Hive dependencies
       hive-exec-3.1.0.jar
       libfb303-0.9.3.jar // libfb303 is not packed into hive-exec in some versions, need to add it separately

       // add antlr-runtime if you need to use hive dialect
       antlr-runtime-3.5.2.jar

```
{{< /tab >}}
{{< /tabs >}}

### Program maven

If you are building your own program, you need the following dependencies in your mvn file.
It's recommended not to include these dependencies in the resulting jar file.
You're supposed to add dependencies as stated above at runtime.

```xml
<!-- Flink Dependency -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-hive{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>

<!-- Hive Dependency -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>${hive.version}</version>
    <scope>provided</scope>
</dependency>
```

## Connecting To Hive

Connect to an existing Hive installation using the [catalog interface]({{< ref "docs/dev/table/catalogs" >}}) 
and [HiveCatalog]({{< ref "docs/connectors/table/hive/hive_catalog" >}}) through the table environment or YAML configuration.

Following is an example of how to connect to Hive:

{{< tabs "5d3cc7e1-a304-4f9e-b36e-ff1f32394ec7" >}}
{{< tab "Java" >}}

```java

EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name            = "myhive";
String defaultDatabase = "mydatabase";
String hiveConfDir     = "/opt/hive-conf";

HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive");
```
{{< /tab >}}
{{< tab "Scala" >}}

```scala

val settings = EnvironmentSettings.inStreamingMode()
val tableEnv = TableEnvironment.create(settings)

val name            = "myhive"
val defaultDatabase = "mydatabase"
val hiveConfDir     = "/opt/hive-conf"

val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
tableEnv.registerCatalog("myhive", hive)

// set the HiveCatalog as the current catalog of the session
tableEnv.useCatalog("myhive")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import *
from pyflink.table.catalog import HiveCatalog

settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

catalog_name = "myhive"
default_database = "mydatabase"
hive_conf_dir = "/opt/hive-conf"

hive_catalog = HiveCatalog(catalog_name, default_database, hive_conf_dir)
t_env.register_catalog("myhive", hive_catalog)

# set the HiveCatalog as the current catalog of the session
tableEnv.use_catalog("myhive")
```
{{< /tab >}}
{{< tab "YAML" >}}
```yaml

execution:
    ...
    current-catalog: myhive  # set the HiveCatalog as the current catalog of the session
    current-database: mydatabase
    
catalogs:
   - name: myhive
     type: hive
     hive-conf-dir: /opt/hive-conf
```
{{< /tab >}}
{{< tab "SQL" >}}
```sql

CREATE CATALOG myhive WITH (
    'type' = 'hive',
    'default-database' = 'mydatabase',
    'hive-conf-dir' = '/opt/hive-conf'
);
-- set the HiveCatalog as the current catalog of the session
USE CATALOG myhive;
```
{{< /tab >}}
{{< /tabs >}}

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

It's recommended to use [Hive dialect]({{< ref "docs/connectors/table/hive/hive_dialect" >}}) to execute DDLs to create
Hive tables, views, partitions, functions within Flink.

## DML

Flink supports DML writing to Hive tables. Please refer to details in [Reading & Writing Hive Tables]({{< ref "docs/connectors/table/hive/hive_read_write" >}})
