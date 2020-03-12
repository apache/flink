---
title: "Hive Integration"
nav-id: hive_tableapi
nav-parent_id: tableapi
nav-pos: 100
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

[Apache Hive](https://hive.apache.org/) 已经成为了数据仓库生态系统中的核心。
它不仅仅是一个用于大数据分析和ETL场景的SQL引擎，同样它也是一个数据管理平台，可用于发现，定义，和演化数据。

Flink提供了两种和Hive集成的方式。

第一种方式是利用了Hive的MetaStore作为持久化的目录，和Flink在`HiveCatalog`存储的特定元数据进行跨系统会话。
例如，用户可以使用`HiveCatalog`将其Kafka信息或ElasticSearch表存储在Hive Metastore中，并后续在SQL查询中重新使用它们。

第二种方式是将Flink作为读写Hive表的替代引擎。

`HiveCatalog`设计为与现有的Hive安装"开箱即用"兼容。
您不需要修改现有的Hive Metastore，也不需要更改表的数据位置或分区。

* Note that we highly recommend users using the [blink planner]({{ site.baseurl }}/dev/table/#dependency-structure) with Hive integration.

* This will be replaced by the TOC
{:toc}

## 支持的Hive版本

Flink支持下列Hive版本。

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

请注意，Hive本身在不同功能上有着不同的适用版本，这些适配性问题不是由Flink所引起的：

- Hive内置函数，已在1.2.0及更高版本予以支持。
- 列约束，也就是PRIMARY KEY 和 NOT NULL，已在3.1.0及更高版本予以支持。
- 更改表的统计信息，已在1.2.0及更高版本予以支持。
- `DATE`列统计，已在1.2.0及更高版本予以支持。
- 2.0.x版本不支持写入ORC表。


### 依赖项

要与Hive集成，您需要在Flink下的`/lib/`目录中添加一些额外的依赖关系，
使得集成在Table API的程序或SQL Client中的SQL能够起到作用。
或者，您可以将这些依赖项放在专用文件夹中，并分别使用Table API程序或SQL Client的`-C`或`-l`选项将它们添加到classpath中。

您可以在下方找到不同Hive主版本所需要的依赖项。

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

       // Flink's Hive connector
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

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

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

       // Hadoop dependencies
       // You can pick a pre-built Hadoop uber jar provided by Flink, alternatively
       // you can use your own hadoop jars. Either way, make sure it's compatible with your Hadoop
       // cluster and the Hive version you're using.
       flink-shaded-hadoop-2-uber-2.6.5-{{ site.shaded_version }}.jar

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

       // Flink's Hive connector
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

       // Flink's Hive connector
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

       // Flink's Hive connector
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



如果您在构建自己的应用程序，则需要在mvn文件中添加以下依赖项。
您应该在运行时添加以上的这些依赖项，而不要在已生成的jar文件中去包含它们。

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

## 连接到Hive

通过表环境或者YAML配置，使用 [catalog interface]({{ site.baseurl }}/dev/table/catalogs.html) 和[HiveCatalog]({{ site.baseurl }}/dev/table/hive/hive_catalog.html)连接到现有的Hive集群。

如果`hive-conf/hive-site.xml`文件存储在远端存储系统，则用户首先应该将hive配置文件下载至其本地环境中。

请注意，虽然HiveCatalog不需要特定的planner，但读写Hive表仅适用于blink planner。因此，强烈建议您在连接到Hive仓库时使用blink planner。


以Hive 2.3.4版本为例：

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

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

val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
val tableEnv = TableEnvironment.create(settings)

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
{% endhighlight %}
</div>
</div>


## DDL
即将支持在Flink中创建Hive表，视图，分区和函数的DDL。

## DML
Flink支持DML写入Hive表，请参考[Reading & Writing Hive Tables]({{ site.baseurl }}/dev/table/hive/read_write_hive.html)
