---
title: "概览"
weight: 1
type: docs
aliases:
  - /zh/dev/table/connectors/
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

# Table & SQL 连接器


Flink 的 Table API 和 SQL 程序可以连接到外部系统来进行批表和流表的读写。table source 用于读取存储在外部系统（例如数据库，键值存储，消息队列或文件系统）中的数据。table sink 可以将表存储到另一个外部存储系统中。不同的 source 和 sink 支持不同的数据格式，例如 CSV，Avro，Parquet 或 ORC。

本文档描述了如何使用内置支持的连接器注册 table source 和 table sink。在 source 或 sink 注册完成之后，就可以通过 Table API 和 SQL 语句访问它们了。

如果你想实现*自定义*的 table source 或 sink，可以查看[自定义 sources 和 sinks]({{< ref "docs/dev/table/sourcessinks" >}})。

已经支持的连接器
------------

Flink 内置支持各种不同的连接器。下表列出了所有可用的连接器。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">名称</th>
        <th class="text-center">版本</th>
        <th class="text-center">源端</th>
        <th class="text-center">目标端</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
      <td></td>
      <td>有界和无界的扫描和查询</td>
      <td>流处理，批处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/elasticsearch" >}}">Elasticsearch</a></td>
      <td>6.x & 7.x</td>
      <td>不支持</td>
      <td>流处理，批处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a></td>
      <td>0.10+</td>
      <td>无界的扫描</td>
      <td>流处理，批处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a></td>
      <td></td>
      <td>无界的扫描</td>
      <td>流处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/jdbc" >}}">JDBC</a></td>
      <td></td>
      <td>有界的扫描和查询</td>
      <td>流处理，批处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/hbase" >}}">Apache HBase</a></td>
      <td>1.4.x & 2.2.x</td>
      <td>有界的扫描和查询</td>
      <td>流处理，批处理</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/hive/overview" >}}">Apache Hive</a></td>
      <td><a href="{{< ref "docs/connectors/table/hive/overview" >}}#supported-hive-versions">支持的版本</a></td>
      <td>无界的扫描，有界的扫描和查询</td>
      <td>流处理，批处理</td>
    </tr>
    </tbody>
</table>

{{< top >}}

请查阅[配置]({{< ref "docs/dev/configuration/connector" >}})小节了解如何添加连接器依赖。

如何使用连接器
--------

FLink 支持使用 SQL `CREATE TABLE` 语句来注册表。
可以在语句中定义表名，表结构，以及连接外部系统用的表参数。

参见 [SQL 部分以获取有关创建表的更多信息]({{< ref "docs/dev/table/sql/create" >}}#create-table)。

以下代码展示了如何读取 Kafka 并且用 Json 解析数据的一个完整例子。

{{< tabs "6d4f00e3-0a94-4ebd-b6b5-c5171851b500" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyUserTable (
  -- 声明表结构
  `user` BIGINT,
  `message` STRING,
  `rowtime` TIMESTAMP(3) METADATA FROM 'timestamp',    -- 使用元数据列来访问 Kafka 记录的时间戳
  `proctime` AS PROCTIME(),    -- 使用计算列来定义处理时间属性
  WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '5' SECOND    -- 使用 WATERMARK 语句来定义事件时间属性
) WITH (
  -- 声明要连接的外部系统
  'connector' = 'kafka',
  'topic' = 'topic_name',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'   -- 声明此系统的格式
)
```
{{< /tab >}}
{{< /tabs >}}

所需的连接器属性是以字符串键值对来配置的。[工厂]({{< ref "docs/dev/table/sourcessinks" >}})将从键值对中根据工厂标识符（本例中的 `kafka` 和 `json`）构造配置的 table source，table sink 以及相应格式。为每个组件搜索一个完全匹配的工厂时，能通过 Java 的 [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) 找到的所有工厂都会被考虑在内。

如果根据给定的属性找不到工厂或找到多个匹配的工厂，将抛出异常并附带找到的工厂和支持的属性的相关信息。


转换表连接器/格式资源
--------

Flink 使用 Java 的 [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) 技术通过工厂标识符来加载表连接器/格式工厂。由于每个表连接器/格式的名为 `org.apache.flink.table.factories.Factory` 的 SPI 资源文件位于相同目录 `META-INF/services` 下，当使用多个表连接器/格式来构建项目的 uber-jar 时这些资源文件会相互覆盖，这将导致 Flink 加载表连接器/格式工厂时失败。

这种情况下，推荐的方式是通过 maven shade 插件的 [ServicesResourceTransformer](https://maven.apache.org/plugins/maven-shade-plugin/examples/resource-transformers.html) 在 `META-INF/services` 目录下转换这些资源文件。以下是一个包含 `flink-sql-connector-hive-3.1.2` 连接器和 `flink-parquet` 格式的项目的 pom.xml 文件内容示例。

```xml

    <modelVersion>4.0.0</modelVersion>
    <groupId>org.example</groupId>
    <artifactId>myProject</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!--  other project dependencies  ...-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-sql-connector-hive-3.1.2{{< scala_version >}}</artifactId>
            <version>{{< version >}}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet{{< scala_version >}}</artifactId>
            <version>{{< version >}}</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <id>shade</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers combine.children="append">
                                <!-- The service transformer is needed to merge META-INF/services files -->
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                                <!-- ... -->
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```

配置 `ServicesResourceTransformer` 后，在构建上述项目的 uber-jar 时，将会合并 `META-INF/services` 目录下的表连接器/格式资源文件，而不是相互覆盖。

{{< top >}}

Schema 映射
------------

SQL `CREATE TABLE` 语句的主体子句中可以定义物理列的名称和类型，约束以及水印。Flink 不保存数据，因此 schema 定义只声明如何从外部系统把物理列映射到 Flink 的表示形式。映射可能不会按照名称映射，这取决于格式和连接器的实现。例如，MySQL 数据库表通过字段名称映射（不区分大小写），CSV 文件系统通过字段顺序映射（字段名称可以是任意的）。这将在每个连接器中进行说明。

以下示例展示了一个简单的 schema，没有时间属性并且输入/输出到表列的一对一字段映射。

{{< tabs "0c267c40-32ef-4a00-b4eb-fa39bfe3f14d" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

### 元数据

一些连接器和格式提供了额外的元数据字段，这些字段可以在有效的物理列旁边的元数据列中被访问。有关元数据列的更多信息，请参见 [`CREATE TABLE` 部分]({{< ref "docs/dev/table/sql/create" >}}#columns)。

### 主键

主键约束表明表的一个列或一些列是唯一的并且不包含空值。主键唯一地标识表中的一行。

source 表的主键是用于优化的元数据信息。sink 的实现通常将 sink 表的主键用于 upsert。

SQL 标准指定约束可以为 ENFORCED 或 NOT ENFORCED。这将控制是否对传入/传出的数据执行约束检查。Flink 不拥有数据，支持的唯一模式是 NOT ENFORCED。由用户来确保查询执行键的完整性。

{{< tabs "9e32660c-868b-4b6a-9632-3b3ea482fe7d" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN,
  PRIMARY KEY (MyField1, MyField2) NOT ENFORCED  -- 在列上定义主键
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

### 时间属性

使用无界流表时，时间属性至关重要。因此，处理时间和事件时间属性都可以定义为 schema 的一部分。

有关 Flink 中时间处理的更多信息，尤其是事件时间，推荐使用常规的[事件时间]({{< ref "docs/dev/table/concepts/time_attributes" >}})。

#### 处理时间属性

为了在 schema 中声明处理时间属性，你可以使用[计算列语法]({{< ref "docs/dev/table/sql/create" >}}#create-table)声明一个由 `PROCTIME()` 内置函数生成的计算列。
计算列是一个不存储在物理数据中的虚拟列。

{{< tabs "5d1f475b-a002-4e85-84f4-00ab0a55a548" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN,
  MyField4 AS PROCTIME() -- 声明处理时间属性
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

#### 事件时间属性

为了控制表的事件时间行​​为，Flink 提供了预定义的时间戳提取器和水印策略。

有关在 DDL 中定义时间属性的更多信息，请参见 [CREATE TABLE 语句]({{< ref "docs/dev/table/sql/create" >}}#create-table)。

支持以下时间戳提取器：

{{< tabs "b40272ba-b259-4a26-9651-815006b283e7" >}}
{{< tab "DDL" >}}
```sql
-- 使用 schema 中存在的 TIMESTAMP(3) 字段作为事件时间属性
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)

-- 使用系统函数或 UDF 或表达式提取预期的 TIMESTAMP(3) 事件时间字段
CREATE TABLE MyTable (
  log_ts STRING,
  ts_field AS TO_TIMESTAMP(log_ts),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

支持以下水印策略：

{{< tabs "e004ebfb-75b1-4d81-80ff-ac5420744b75" >}}
{{< tab "DDL" >}}
```sql
-- 为严格上升的事件时间属性设置水印策略。发出到目前为止观察到的最大时间戳的水印。
-- 时间戳大于最大时间戳的行不会迟到。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field
) WITH (
  ...
)

-- 为上升的事件时间属性设置水印策略。发出到目前为止观察到的最大时间戳减去 0.001 秒的水印。
-- 时间戳大于或等于最大时间戳的行不会迟到。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '0.001' SECOND
) WITH (
  ...
)

-- 为事件时间属性设置水印策略，该属性在有界的时间间隔内是乱序的。
-- 发出水印，该水印是观察到的最大时间戳减去指定的延迟，例如 2 秒。
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '2' SECOND
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

确保始终声明时间戳和水印。触发基于时间的操作需要水印。

### SQL Types

请参阅[数据类型]({{< ref "docs/dev/table/types" >}})页面，了解如何在 SQL 中声明类型。

{{< top >}}
