---
title: "JDBC SQL 连接器"
nav-title: JDBC
nav-parent_id: sql-connectors
nav-pos: 5
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

<span class="label label-primary">Scan Source: Bounded</span>
<span class="label label-primary">Lookup Source: Sync Mode</span>
<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Append & Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

JDBC连接器允许使用JDBC驱动向任意类型的关系型数据库读取或者写入数据。本文档描述了针对关系型数据库如何通过建立JDBC连接器来执行SQL查询。

如果一个主键定义在 DDL 中，JDBC sink 将以 upsert 模式与外部系统交换 UPDATE/DELETE 消息；否则，它将以appened 模式与外部系统交换消息且不支持消费 UPDATE/DELETE 消息。


依赖
------------

{% assign connector = site.data.sql-connectors['jdbc'] %}
{% include sql-connector-download-table.zh.html connector='connector' %}

<br>
在连接到具体数据库时，也需要对应的驱动依赖，目前支持的驱动如下：

| Driver      |      Group Id      |      Artifact Id       |      JAR         |
| :-----------| :------------------| :----------------------| :----------------|
| MySQL       |       `mysql`      | `mysql-connector-java` | [下载](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) |
| PostgreSQL  |  `org.postgresql`  |      `postgresql`      | [下载](https://jdbc.postgresql.org/download.html) |
| Derby       | `org.apache.derby` |        `derby`         | [下载](http://db.apache.org/derby/derby_downloads.html) |

<br>
当前，JDBC 连接器和驱动不在 Flink 二进制发布包中，请参阅 [这里]({% link dev/project-configuration.zh.md %}) 了解在集群上执行时何连接它们。

如何创建 JDBC 表
----------------

JDBC table可以按如下定义：

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- register a MySQL table 'users' in Flink SQL
CREATE TABLE MyUserTable (
  id BIGINT,
  name STRING,
  age INT,
  status BOOLEAN,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
   'table-name' = 'users'
);

-- write data into the JDBC table from the other table "T"
INSERT INTO MyUserTable
SELECT id, name, age, status FROM T;

-- scan data from the JDBC table
SELECT id, name, age, status FROM MyUserTable;

-- temporal join the JDBC table as a dimension table
SELECT * FROM myTopic
LEFT JOIN MyUserTable FOR SYSTEM_TIME AS OF myTopic.proctime
ON myTopic.key = MyUserTable.id;
{% endhighlight %}
</div>
</div>

连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-left" style="width: 8%">是否必填</th>
        <th class="text-left" style="width: 7%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必填</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定使用什么类型的 connector, 这里应该是 <code>'jdbc'</code>。</td>
    </tr>
    <tr>
      <td><h5>url</h5></td>
      <td>必填</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>JDBC 数据库 url</td>
    </tr>
    <tr>
      <td><h5>table-name</h5></td>
      <td>必填</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接到 JDBC 表的名称。</td>
    </tr>
    <tr>
      <td><h5>driver</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接到这个 URL 的 JDBC 驱动类名，如果不设置它将自动从该 URL 中推导。</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> JDBC 用户名： <code>'username'</code> 和 <code>'password'</code> 必须都被指定，如果指定了两者中任一参数。</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> JDBC 密码</td>
    </tr>
    <tr>
      <td><h5>connection.max-retry-timeout</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">60s</td>
      <td>Duration</td>
      <td>最大重试超时时间，超时时间应该是以秒为单位，并且不应该小于1秒。</td>
    </tr>
    <tr>
      <td><h5>scan.partition.column</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于将输入进行分区的列名，请参阅下面的 <a href="#partitioned-scan">分区扫描</a>部分了解更多详情。</td>
    </tr>
    <tr>
      <td><h5>scan.partition.num</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>分区的数量</td>
    </tr>
    <tr>
      <td><h5>scan.partition.lower-bound</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>第一个分区的最小值</td>
    </tr>
    <tr>
      <td><h5>scan.partition.upper-bound</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>最后一个分区的最大值</td>
    </tr>
    <tr>
      <td><h5>scan.fetch-size</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>每次循环读取时应该从数据库中获取的行数；如果指定的值为0，则该配置项会被忽略。</td>
    </tr>
    <tr>
      <td><h5>scan.auto-commit</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>在 JDBC 驱动程序上设置 <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#commit_transactions">auto-commit</a> 标志，它决定了每个语句是否在事务中自动提交。有一些 JDBC 驱动程序，特别是 <a href="https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor">Postgres</a> ，可能需要将此设置为 false 以便流化结果。</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.max-rows</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Lookup Cache 的最大行数，若超过该值，则最老的行记录将会过期。默认情况下，Lookup Cache 是未开启的。请查阅如下 <a href="#lookup-cache">Lookup Cache</a> 章节了解更详细的信息。</td>
    </tr>
    <tr>
      <td><h5>lookup.cache.ttl</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>查找缓存中每一行记录的最大存活时间，若超过该时间，则最老的行记录将被设置成已过期。默认情况下，查找缓存是未开启的。查看如下 <a href="#lookup-cache">Lookup Cache</a> 部分可以了解到更详细的信息。</td>
    </tr>
    <tr>
      <td><h5>lookup.max-retries</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>查询数据库失败的最大重试时间</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>flush前缓存记录的最大大小,可以设置为<code>0</code> 来禁用它。</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>flush 的间隔时间，超过该值后异步线程将刷新数据，可以将刷新的间隔时间设置为<code>'0'</code> 来禁用该配置。注意, 为了完全异步地处理缓存的 flush 事件，可以将<code>'sink.buffer-flush.max-rows'</code> 设置为'0'并配置适当的 flush 时间间隔。</td>
 </tr>
<tr>
      <td><h5>sink.max-retries</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>写入记录到数据库失败后的最大重试次数</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>用于定义 JDBC sink 算子的并行度。默认情况下，并行度是由框架决定：使用与上游链式算子相同的并行度。</td>
    </tr>
    </tbody>
</table>


特性
--------

### 键处理

在写入数据到外部数据库时 Flink 会使用定义在 DDL 中的主键，如果定义了主键，则连接器工作在 upsert 模式，否则连接器工作在 append 模式。

在 upsert 模式下，Flink 根据主键插入新加的一行记录或者更新已存在的记录，这种方式可以确保幂等性。为了确保输出结果是符合预期的，推荐为表定义主键并且确保主键是底层数据库中表的主键集或唯一建。在 append 模式下，Flink 会把所有记录解释为插入消息，如果违反了底层数据库中主键或者唯一约束，INSERT 插入可能会失败。

可以查看[CREATE TABLE DDL]({% link dev/table/sql/create.zh.md %}#create-table) 更多关于 PRIMARY KEY 语法。

### 分区扫描

为了在 并行`Source` task 实例下加速读取数据，Flink 为 JDBC table 提供了分区扫描的特性。

下述所有的分区扫描选项必须要指定当其中任一参数被指定时，这些参数描述了在多个 task 并行读取数据时如何对表进行分区。 `scan.partition.column` 必须是相关表中的数字、日期或时间戳列。注意 ，`scan.partition.lower-bound` 和 `scan.partition.upper-bound` 用于决定分区的起始位置和过滤表中的数据。如果是批处理作业，也可以在提交 flink 作业之前获取最大值和最小值。

- `scan.partition.column`: 输入用于进行分区的列名
- `scan.partition.num`: 分区的数量
- `scan.partition.lower-bound`: 第一个分区的最小值
- `scan.partition.upper-bound`: 最后一个分区的最大值

### lookup cache

JDBC 连接器可以用在时态表关联中作为一个可 lookup 的 Source (又称为维表)，当前只支持同步的查找模式。默认情况下，lookup cache 是未启用的，你可以设置 `lookup.cache.max-rows` and `lookup.cache.ttl` 参数来启用。

lookup cache 的主要目的是用于提高时态表关联 JDBC 连接器的性能。默认情况，lookup cache 不开启，所以所有请求都会发送到外部的数据库。当 lookup cache 被启用时，每个进程（即 TaskManager ）将维护一个缓存。Flink 将优先查找缓存，只有当缓存未查找到时才向外部数据库发送请求，并使用返回的数据更新缓存。

当缓存命中最大缓存行时 `lookup.cache.max-rows` 或者当行超过最大存活时间 `lookup.cache.ttl`，缓存中最老的行将被设置为已过期。

缓存中的记录可能不是最新的，用户可以将 `lookup.cache.ttl` 设置为一个更小的值以获得更好的刷新数据，但这可能会增加发送到数据库的请求数，所以要做好吞吐量和正确性之间的平衡。


### 幂等写入

如果在 DDL 中定义了主键，JDBC sink 将使用 upsert 语义而不是普通的 INSERT 语句。Upsert 语义指的是如果底层数据库中存在违反唯一性约束，则原子地添加新行或更新现有行，这种方式确保了幂等性。

如果出现故障，Flink 作业会从上次成功的 checkpoint 恢复并重新处理，这可能导致在恢复过程中重复处理消息。强烈推荐使用 upsert 模式，因为如果需要重复处理记录，它有助于避免违反数据库主键约束和产生重复数据。

除了故障恢复场景外，数据源（kafka topic）也可能随着时间的推移自然地包含多个具有相同主键的记录，这使得 upsert 模式是用户期待的。

由于 upsert 没有标准的语法，因此下表描述了不同数据库的 DML 语法：

<table class="table table-bordered" style="width: 60%">
    <thead>
      <tr>
        <th class="text-left">Database</th>
        <th class="text-left">Upsert Grammar</th>
       </tr>
    </thead>
    <tbody>
        <tr>
            <td>MySQL</td>
            <td>INSERT .. ON DUPLICATE KEY UPDATE ..</td>
        </tr>
        <tr>
            <td>PostgreSQL</td>
            <td>INSERT .. ON CONFLICT .. DO UPDATE SET ..</td>
        </tr>
    </tbody>
</table>

### Postgres 数据库作为 Catalog

`JdbcCatalog` 允许用户通过 JDBC 协议将 Flink 连接到关系数据库。目前，`PostgresCatalog` 是 JDBC Catalog 的唯一实现，`PostgresCatalog` 只支持有限的 `Catalog` 方法，包括：

{% highlight java %}
// The supported methods by Postgres Catalog.
PostgresCatalog.databaseExists(String databaseName)
PostgresCatalog.listDatabases()
PostgresCatalog.getDatabase(String databaseName)
PostgresCatalog.listTables(String databaseName)
PostgresCatalog.getTable(ObjectPath tablePath)
PostgresCatalog.tableExists(ObjectPath tablePath)
{% endhighlight %}

其他的 `Catalog` 方法现在还是不支持的。

#### PostgresCatalog 的使用

请参考[依赖](#依赖) 章节了解如何配置 JDBC 连接器和 Postgres 驱动。

Postgres catalog 支持以下参数:
- `name`: 必填, catalog的名称
- `default-database`:  必填, 默认要连接的数据库
- `username`: 必填, Postgres 账户的用户名
- `password`: 必填, 账户的密码
- `base-url`: 必填, 应该符合`"jdbc:postgresql://<ip>:<port>"`的格式, 同时这里不应该包含数据库名。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE CATALOG mypg WITH(
    'type' = 'jdbc',
    'default-database' = '...',
    'username' = '...',
    'password' = '...',
    'base-url' = '...'
);

USE CATALOG mypg;
{% endhighlight %}
</div>
<div data-lang="Java" markdown="1">
{% highlight java %}

EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

String name            = "mypg";
String defaultDatabase = "mydb";
String username        = "...";
String password        = "...";
String baseUrl         = "..."

JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
tableEnv.registerCatalog("mypg", catalog);

// 设置 JdbcCatalog 为会话的当前 catalog
tableEnv.useCatalog("mypg");
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">
{% highlight scala %}

val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
val tableEnv = TableEnvironment.create(settings)

val name            = "mypg"
val defaultDatabase = "mydb"
val username        = "..."
val password        = "..."
val baseUrl         = "..."

val catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl)
tableEnv.registerCatalog("mypg", catalog)

// 设置 JdbcCatalog 为会话的当前 catalog
tableEnv.useCatalog("mypg")
{% endhighlight %}
</div>
<div data-lang="Python" markdown="1">
{% highlight python %}
from pyflink.table.catalog import JdbcCatalog

environment_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=environment_settings)

name = "mypg"
default_database = "mydb"
username = "..."
password = "..."
base_url = "..."

catalog = JdbcCatalog(name, default_database, username, password, base_url)
t_env.register_catalog("mypg", catalog)

// 设置JdbcCatalog为会话的当前目录
t_env.use_catalog("mypg")
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
{% highlight yaml %}

execution:
        planner: blink
        ...
        current-catalog: mypg  # set the JdbcCatalog as the current catalog of the session
        current-database: mydb

catalogs:
   - name: mypg
     type: jdbc
     default-database: mydb
     username: ...
     password: ...
     base-url: ...
{% endhighlight %}
</div>
</div>

#### PostgresSQL 元空间映射

除了数据库之外，postgresSQL 还有一个额外的命名空间`模式`。一个 Postgres 实例可以拥有多个数据库， 每个数据库可以拥有多个模式，其中一个模式默认名为“public”，每个模式可以包含多张表。

在 Flink 中，当查询由 Postgres catalog 注册的表时，用户可以使用 `schema_name.table_name` 或只有 `table_name`，其中 `schema_name` 是可选的，默认值为 "public"。

因此，Flink Catalog 和 Postgres 之间的元空间映射如下：

| Flink Catalog Metaspace Structure    |   Postgres Metaspace Structure      |
| :------------------------------------| :-----------------------------------|
| catalog name (defined in Flink only) | N/A                                 |
| database name                        | database name                       |
| table name                           | [schema_name.]table_name            |

Flink 中的 Postgres 表的完整路径应该是 ``"<catalog>.<db>.`<schema.table>`"``。如果指定了 schema，请注意需要转义 `<schema.table>`。

这里提供了一些访问 Postgres 表的例子：

{% highlight sql %}
-- scan table 'test_table' of 'public' schema (i.e. the default schema), the schema name can be omitted
SELECT * FROM mypg.mydb.test_table;
SELECT * FROM mydb.test_table;
SELECT * FROM test_table;

-- scan table 'test_table2' of 'custom_schema' schema,
-- the custom schema can not be omitted and must be escaped with table.
SELECT * FROM mypg.mydb.`custom_schema.test_table2`
SELECT * FROM mydb.`custom_schema.test_table2`;
SELECT * FROM `custom_schema.test_table2`;
{% endhighlight %}

数据类型映射
----------------
Flink 支持连接到多个使用方言（dialect）的数据库，如 MySQL、PostgresSQL、Derby 等。其中，Derby 通常是用于测试目的。下表列出了从关系数据库数据类型到 Flink SQL 数据类型的类型映射，映射表可以使得在 Flink 中定义 JDBC 表更加简单。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left">PostgreSQL type<a href="https://www.postgresql.org/docs/12/datatype.html"></a></th>
        <th class="text-left">Flink SQL type<a href="{% link dev/table/types.zh.md %}"></a></th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>TINYINT</code></td>
      <td></td>
      <td><code>TINYINT</code></td>
    </tr>
    <tr>
      <td>
        <code>SMALLINT</code><br>
        <code>TINYINT UNSIGNED</code></td>
      <td>
        <code>SMALLINT</code><br>
        <code>INT2</code><br>
        <code>SMALLSERIAL</code><br>
        <code>SERIAL2</code></td>
      <td><code>SMALLINT</code></td>
    </tr>
    <tr>
      <td>
        <code>INT</code><br>
        <code>MEDIUMINT</code><br>
        <code>SMALLINT UNSIGNED</code></td>
      <td>
        <code>INTEGER</code><br>
        <code>SERIAL</code></td>
      <td><code>INT</code></td>
    </tr>
    <tr>
      <td>
        <code>BIGINT</code><br>
        <code>INT UNSIGNED</code></td>
      <td>
        <code>BIGINT</code><br>
        <code>BIGSERIAL</code></td>
      <td><code>BIGINT</code></td>
    </tr>
   <tr>
      <td><code>BIGINT UNSIGNED</code></td>
      <td></td>
      <td><code>DECIMAL(20, 0)</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
      <td><code>BIGINT</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>
        <code>REAL</code><br>
        <code>FLOAT4</code></td>
      <td><code>FLOAT</code></td>
    </tr>
    <tr>
      <td>
        <code>DOUBLE</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td>
        <code>FLOAT8</code><br>
        <code>DOUBLE PRECISION</code></td>
      <td><code>DOUBLE</code></td>
    </tr>
    <tr>
      <td>
        <code>NUMERIC(p, s)</code><br>
         <code>DECIMAL(p, s)</code></td>
      <td>
        <code>NUMERIC(p, s)</code><br>
        <code>DECIMAL(p, s)</code></td>
      <td><code>DECIMAL(p, s)</code></td>
    </tr>
    <tr>
      <td>
        <code>BOOLEAN</code><br>
         <code>TINYINT(1)</code></td>
      <td><code>BOOLEAN</code></td>
      <td><code>BOOLEAN</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
      <td><code>DATE</code></td>
    </tr>
    <tr>
      <td><code>TIME [(p)]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIME [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td><code>DATETIME [(p)]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
      <td><code>TIMESTAMP [(p)] [WITHOUT TIMEZONE]</code></td>
    </tr>
    <tr>
      <td>
        <code>CHAR(n)</code><br>
        <code>VARCHAR(n)</code><br>
        <code>TEXT</code></td>
      <td>
        <code>CHAR(n)</code><br>
        <code>CHARACTER(n)</code><br>
        <code>VARCHAR(n)</code><br>
        <code>CHARACTER VARYING(n)</code><br>
        <code>TEXT</code></td>
      <td><code>STRING</code></td>
    </tr>
    <tr>
      <td>
        <code>BINARY</code><br>
        <code>VARBINARY</code><br>
        <code>BLOB</code></td>
      <td><code>BYTEA</code></td>
      <td><code>BYTES</code></td>
    </tr>
    <tr>
      <td></td>
      <td><code>ARRAY</code></td>
      <td><code>ARRAY</code></td>
    </tr>
    </tbody>
</table>

{% top %}
