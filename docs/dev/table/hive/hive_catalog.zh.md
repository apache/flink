---
title: "HiveCatalog"
nav-parent_id: hive_tableapi
nav-pos: 1
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

多年来，Hive Metastore 在 Hadoop 生态系统中已发展成为事实上的元数据中心。 实际上，许多公司在其生产环境中都有一个 Hive Metastore 服务实例，以管理其所有元数据（ Hive 元数据或非 Hive 元数据）。

对于同时部署了 Hive 和 Flink 部署的用户，`HiveCatalog` 使他们能够使用 Hive Metastore 来管理 Flink 的元数据。

对于仅部署了 Flink 的用户，`HiveCatalog` 是 Flink 提供的唯一现成的持久 Catalog。如果没有持久 Catalog，使用 [Flink SQL CREATE DDL]({{site.baseurl}}/zh/dev/table/SQL/CREATE.html) 的用户必须在每个会话（session）中重复创建像 Kafka 表这样的元对象（meta-objects），这会浪费很多时间。`HiveCatalog` 填补了这一空白，用户只需要创建一次表和其他元对象，就可以在以后的会话中方便地引用和管理它们。

## 设置 HiveCatalog

### 依赖

在 Flink 中设置 `HiveCatalog` 需要的依赖和设置整个 Flink-Hive 集成需要的[依赖]({{ site.baseurl }}/zh/dev/table/hive/#dependencies)是一样的。

### 配置

在 Flink 中设置 `HiveCatalog` 需要的配置和设置整个 Flink-Hive 集成需要的[配置]({{ site.baseurl }}/zh/dev/table/hive/#connecting-to-hive)是一样的。


## 怎么使用 HiveCatalog

一旦正确的完成配置，`HiveCatalog` 就可以正常工作了。用户可以使用 DDL 来创建 Flink 元对象，然后就可以立即看到这些元对象。

`HiveCatalog` 可用于处理两种表： Hive 兼容表（Hive-compatible tables）和通用表（generic tables）。Hive 兼容表就存储层中的元数据和数据而言，是以 Hive 兼容方式存储的数据。因此，通过 Flink 创建的 Hive 兼容表可以从 Hive 端查询。

另一方面，通用表是 Flink 特有的。使用 `HiveCatalog` 创建通用表时，我们使用 HMS 来持久化元数据。虽然这些表对 Hive 可见，但 Hive 不太可能理解这些元数据。因此，在 Hive 中使用这样的表会导致不可知的行为。

Flink 使用属性 '*is_generic*' 来判断表是 Hive 兼容表还是通用表。使用 `HiveCatalog` 创建表时，默认情况下被视为通用表。如果要创建 Hive 兼容表，请确保在表属性中将 `is_generic` 设置为 false 。

如上所述，不应在 Hive 中使用通用表。在 Hive CLI 中，可以调用表的 `DESCRIBE FORMATTED` ，并通过检查 `is_generic` 属性来确定它是否是通用表。通用表有 `is_generic=true` 的属性。

### 示例

我们将在这里解析一个简单的例子。

#### 第一步：设置 Hive Metastore

运行 Hive Metastore。

在这里，我们设置了本地 Hive Metastore，`hive-site.xml`文件位于本地路径  `/opt/hive-conf/hive-site.xml` 中。我们有如下配置：

{% highlight xml %}

<configuration>
   <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
      <description>metadata is stored in a MySQL server</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>MySQL JDBC driver class</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>...</value>
      <description>user name for connecting to mysql server</description>
   </property>

   <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>...</value>
      <description>password for connecting to mysql server</description>
   </property>

   <property>
       <name>hive.metastore.uris</name>
       <value>thrift://localhost:9083</value>
       <description>IP address (or fully-qualified domain name) and port of the metastore host</description>
   </property>

   <property>
       <name>hive.metastore.schema.verification</name>
       <value>true</value>
   </property>

</configuration>
{% endhighlight %}

用 Hive Cli 测试与 HMS 的连接。运行一些命令，可以看到我们有一个名为 `default` 的数据库，并且其中没有表。

{% highlight bash %}

hive> show databases;
OK
default
Time taken: 0.032 seconds, Fetched: 1 row(s)

hive> show tables;
OK
Time taken: 0.028 seconds, Fetched: 0 row(s)
{% endhighlight %}


#### 第二步：配置 Flink 集群和 SQL CLI

将所有的 Hive 依赖添加到 Flink 中的 `/lib` 目录中，然后修改 SQL CLI 的 yaml 配置文件 `sql-cli-defaults.yaml`，如下所示：

{% highlight yaml %}

execution:
    planner: blink
    type: streaming
    ...
    current-catalog: myhive  # set the HiveCatalog as the current catalog of the session
    current-database: mydatabase
    
catalogs:
   - name: myhive
     type: hive
     hive-conf-dir: /opt/hive-conf  # contains hive-site.xml
{% endhighlight %}


#### 第三部：设置 kafka 集群

启动本地 Kafka 2.3.0 集群，创建一个 topic，名为 “test” ，并为该 topic 产生一些简单的由名称和年龄的元组数据。

{% highlight bash %}

localhost$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
>tom,15
>john,21

{% endhighlight %}

通过启动Kafka控制台消费组（Kafka console consumer），可以看到这些消息。

{% highlight bash %}
localhost$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

tom,15
john,21

{% endhighlight %}


#### 第四步：启动 SQL Client, 通过 Flink SQL DDL 创建 Kafka 表

启动 Flink SQL Client，通过 DDL 创建一个简单的 Kafka 2.3.0 的表，然后验证表的 schema。

{% highlight bash %}

Flink SQL> CREATE TABLE mykafka (name String, age Int) WITH (
   'connector.type' = 'kafka',
   'connector.version' = 'universal',
   'connector.topic' = 'test',
   'connector.properties.bootstrap.servers' = 'localhost:9092',
   'format.type' = 'csv',
   'update-mode' = 'append'
);
[INFO] Table has been created.

Flink SQL> DESCRIBE mykafka;
root
 |-- name: STRING
 |-- age: INT

{% endhighlight %}

通过 Hive Cli 验证该表对 Hive 也是可见的，值得注意的是该表具有 `is_generic=true` 的属性：

{% highlight bash %}
hive> show tables;
OK
mykafka
Time taken: 0.038 seconds, Fetched: 1 row(s)

hive> describe formatted mykafka;
OK
# col_name            	data_type           	comment


# Detailed Table Information
Database:           	default
Owner:              	null
CreateTime:         	......
LastAccessTime:     	UNKNOWN
Retention:          	0
Location:           	......
Table Type:         	MANAGED_TABLE
Table Parameters:
	flink.connector.properties.bootstrap.servers	localhost:9092
	flink.connector.topic	test
	flink.connector.type	kafka
	flink.connector.version	universal
	flink.format.type   	csv
	flink.generic.table.schema.0.data-type	VARCHAR(2147483647)
	flink.generic.table.schema.0.name	name
	flink.generic.table.schema.1.data-type	INT
	flink.generic.table.schema.1.name	age
	flink.update-mode   	append
	is_generic          	true
	transient_lastDdlTime	......

# Storage Information
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat
OutputFormat:       	org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat
Compressed:         	No
Num Buckets:        	-1
Bucket Columns:     	[]
Sort Columns:       	[]
Storage Desc Params:
	serialization.format	1
Time taken: 0.158 seconds, Fetched: 36 row(s)

{% endhighlight %}


#### 第五步：运行 Flink SQL 查询 Kafka 表

使用 Flink 集群中的 Flink SQL Client 运行一个简单的选择查询，集群可以是 standalone 模式也可以是 yarn-session 模式。

{% highlight bash %}
Flink SQL> select * from mykafka;

{% endhighlight %}

向 Kafka topic 中生产更多的消息

{% highlight bash %}
localhost$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

tom,15
john,21
kitty,30
amy,24
kaiky,18

{% endhighlight %}

现在，你应该可以在 SQL Client 中看到 Flink 生成的结果，如下所示：

{% highlight bash %}
             SQL Query Result (Table)
 Refresh: 1 s    Page: Last of 1     

        name                       age
         tom                        15
        john                        21
       kitty                        30
         amy                        24
       kaiky                        18

{% endhighlight %}

## 支持的类型

`HiveCatalog` 支持通用表的所有 Flink 类型。

对于Hive兼容表，`HiveCatalog` 需要将 Flink 数据类型映射到相应的 Hive 类型，如下表所述：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-center" style="width: 25%">Flink 数据类型</th>
      <th class="text-center">Hive 数据类型</th>
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

有关类型映射的注意事项：
* Hive 的 `CHAR(p)` 最大长度为255
* Hive 的 `VARCHAR(p)` 最大长度为65535
* Hive 的 `MAP` 仅支持 primitive 类型的 key，而 Flink 的 `MAP` 可以是任何数据类型
* Hive 的 `UNION` 类型不受支持
* Hive 的 `TIMESTAMP` 精度始终是 9，不支持其他精度。而 Hive UDF 可以处理精度 <= 9 的 `TIMESTAMP` 值。
* Hive 不支持 Flink 的 `TIMESTAMP_WITH_TIME_ZONE`、`TIMESTAMP_WITH_LOCAL_TIME_ZONE` 和 `MULTISET`
* Flink 的 `INTERVAL` 类型尚未映射到 Hive 的 `INTERVAL` 类型

## Scala Shell

注意：目前 blink planner 还不能很好的支持 Scala Shell，因此 **不** 建议在 Scala Shell 中使用 Hive 连接器。

