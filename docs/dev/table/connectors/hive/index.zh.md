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

[Apache Hive](https://hive.apache.org/) 已经成为了数据仓库生态系统中的核心。
它不仅仅是一个用于大数据分析和ETL场景的SQL引擎，同样它也是一个数据管理平台，可用于发现，定义，和演化数据。

Flink 与 Hive 的集成包含两个层面。

一是利用了 Hive 的 MetaStore 作为持久化的 Catalog，用户可通过`HiveCatalog`将不同会话中的 Flink 元数据存储到 Hive Metastore 中。
例如，用户可以使用`HiveCatalog`将其 Kafka 表或 Elasticsearch 表存储在 Hive Metastore 中，并后续在 SQL 查询中重新使用它们。

二是利用 Flink 来读写 Hive 的表。

`HiveCatalog`的设计提供了与 Hive 良好的兼容性，用户可以"开箱即用"的访问其已有的 Hive 数仓。
您不需要修改现有的 Hive Metastore，也不需要更改表的数据位置或分区。

* 我们强烈建议用户使用 [Blink planner]({% link dev/table/index.zh.md %}#dependency-structure) 与 Hive 集成。

* This will be replaced by the TOC
{:toc}

## 支持的Hive版本

Flink 支持一下的 Hive 版本。

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

请注意，某些功能是否可用取决于您使用的 Hive 版本，这些限制不是由 Flink 所引起的：

- Hive 内置函数在使用 Hive-1.2.0 及更高版本时支持。
- 列约束，也就是 PRIMARY KEY 和 NOT NULL，在使用 Hive-3.1.0 及更高版本时支持。
- 更改表的统计信息，在使用 Hive-1.2.0 及更高版本时支持。
- `DATE`列统计信息，在使用 Hive-1.2.0 及更高版时支持。
- 使用 Hive-2.0.x 版本时不支持写入 ORC 表。

### 依赖项

要与 Hive 集成，您需要在 Flink 下的`/lib/`目录中添加一些额外的依赖包，
以便通过 Table API 或 SQL Client 与 Hive 进行交互。
或者，您可以将这些依赖项放在专用文件夹中，并分别使用 Table API 程序或 SQL Client 的`-C`或`-l`选项将它们添加到 classpath 中。

Apache Hive 是基于 Hadoop 之上构建的, 首先您需要 Hadoop 的依赖，请参考
Providing Hadoop classes:
```
export HADOOP_CLASSPATH=`hadoop classpath`
```

有两种添加 Hive 依赖项的方法。第一种是使用 Flink 提供的 Hive Jar包。您可以根据使用的 Metastore 的版本来选择对应的 Hive jar。第二个方式是分别添加每个所需的 jar 包。如果您使用的 Hive 版本尚未在此处列出，则第二种方法会更适合。

**注意**：建议您优先使用 Flink 提供的 Hive jar 包。仅在 Flink 提供的 Hive jar 不满足您的需求时，再考虑使用分开添加 jar 包的方式。

#### 使用 Flink 提供的 Hive jar

下表列出了所有可用的 Hive jar。您可以选择一个并放在 Flink 发行版的`/lib/` 目录中。

| Metastore version | Maven dependency             | SQL Client JAR         |
| :---------------- | :--------------------------- | :----------------------|
| 1.0.0 - 1.2.2     | `flink-sql-connector-hive-1.2.2` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-1.2.2{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-1.2.2{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 2.0.0 - 2.2.0     | `flink-sql-connector-hive-2.2.0` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.2.0{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-2.2.0{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 2.3.0 - 2.3.6     | `flink-sql-connector-hive-2.3.6` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-2.3.6{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |
| 3.0.0 - 3.1.2     | `flink-sql-connector-hive-3.1.2` | {% if site.is_stable %}[Download](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-hive-3.1.2{{site.scala_version_suffix}}-{{site.version}}.jar) {% else %} Only available for stable releases {% endif %} |

#### 用户定义的依赖项

您可以在下方找到不同Hive主版本所需要的依赖项。


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

### Maven 依赖

如果您在构建自己的应用程序，则需要在 mvn 文件中添加以下依赖项。
您应该在运行时添加以上的这些依赖项，而不要在已生成的 jar 文件中去包含它们。

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

通过 TableEnvironment 或者 YAML 配置，使用 [Catalog 接口]({% link dev/table/catalogs.zh.md %}) 和 [HiveCatalog]({% link dev/table/connectors/hive/hive_catalog.zh.md %})连接到现有的 Hive 集群。

请注意，虽然 HiveCatalog 不需要特定的 planner，但读写Hive表仅适用于 Blink planner。因此，强烈建议您在连接到 Hive 仓库时使用 Blink planner。

以下是如何连接到 Hive 的示例：

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

下表列出了通过 YAML 文件或 DDL 定义 `HiveCatalog` 时所支持的参数。

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">参数</th>
      <th class="text-center" style="width: 5%">必选</th>
      <th class="text-center" style="width: 5%">默认值</th>
      <th class="text-center" style="width: 10%">类型</th>
      <th class="text-center" style="width: 60%">描述</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>type</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>Catalog 的类型。 创建 HiveCatalog 时，该参数必须设置为<code>'hive'</code>。</td>
    </tr>
    <tr>
      <td><h5>name</h5></td>
      <td>是</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>Catalog 的名字。仅在使用 YAML file 时需要指定。</td>
    </tr>
    <tr>
      <td><h5>hive-conf-dir</h5></td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>指向包含 hive-site.xml 目录的 URI。 该 URI 必须是 Hadoop 文件系统所支持的类型。 如果指定一个相对 URI，即不包含 scheme，则默认为本地文件系统。如果该参数没有指定，我们会在 class path 下查找hive-site.xml。</td>
    </tr>
    <tr>
      <td><h5>default-database</h5></td>
      <td>否</td>
      <td style="word-wrap: break-word;">default</td>
      <td>String</td>
      <td>当一个catalog被设为当前catalog时，所使用的默认当前database。</td>
    </tr>
    <tr>
      <td><h5>hive-version</h5></td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>HiveCatalog 能够自动检测使用的 Hive 版本。我们建议<b>不要</b>手动设置 Hive 版本，除非自动检测机制失败。</td>
    </tr>
    <tr>
      <td><h5>hadoop-conf-dir</h5></td>
      <td>否</td>
      <td style="word-wrap: break-word;">(无)</td>
      <td>String</td>
      <td>Hadoop 配置文件目录的路径。目前仅支持本地文件系统路径。我们推荐使用 <b>HADOOP_CONF_DIR</b> 环境变量来指定 Hadoop 配置。因此仅在环境变量不满足您的需求时再考虑使用该参数，例如当您希望为每个 HiveCatalog 单独设置 Hadoop 配置时。</td>
    </tr>
    </tbody>
</table>


## DDL

即将支持在 Flink 中创建 Hive 表，视图，分区和函数的DDL。

## DML

Flink 支持 DML 写入 Hive 表，请参考[读写 Hive 表]({% link dev/table/connectors/hive/hive_read_write.zh.md %})
