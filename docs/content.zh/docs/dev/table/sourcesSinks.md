---
title: "User-defined Sources & Sinks"
weight: 131
type: docs
aliases:
  - /zh/dev/table/sourceSinks.html
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

# 用户自定义 Sources & Sinks

*动态表*是 Flink Table & SQL API的核心概念，用于统一有界和无界数据的处理。

动态表只是一个逻辑概念，因此 Flink 并不拥有数据。相应的，动态表的内容存储在外部系统（ 如数据库、键值存储、消息队列 ）或文件中。

*动态 sources* 和*动态 sinks* 可用于从外部系统读取数据和向外部系统写入数据。在文档中，sources 和 sinks 常在术语*连接器* 下进行总结。

Flink 为 Kafka、Hive 和不同的文件系统提供了预定义的连接器。有关内置 table sources 和 sinks 的更多信息，请参阅[连接器部分]({{< ref "docs/connectors/table/overview" >}})

本页重点介绍如何开发自定义的用户定义连接器。

{{< hint warning >}}从 Flink v1.16 开始, TableEnvironment 引入了一个用户类加载器，以在 table 程序、SQL Client、SQL Gateway 中保持一致的类加载行为。该类加载器会统一管理所有的用户 jar 包，包括通过 `ADD JAR` 或 `CREATE FUNCTION .. USING JAR ..` 添加的 jar 资源。
 在用户自定义连接器中，应该将 `Thread.currentThread().getContextClassLoader()` 替换成该用户类加载器去加载类。否则，可能会发生 `ClassNotFoundException` 的异常。该用户类加载器可以通过 `DynamicTableFactory.Context` 获得。
{{< /hint >}}

概述
--------

在许多情况下，开发人员不需要从头开始创建新的连接器，而是希望稍微修改现有的连接器或 hook 到现有的 stack。在其他情况下，开发人员希望创建专门的连接器。

本节对这两种用例都有帮助。它解释了表连接器的一般体系结构，从 API 中的纯粹声明到在集群上执行的运行时代码

实心箭头展示了在转换过程中对象如何从一个阶段到下一个阶段转换为其他对象。

{{< img width="90%" src="/fig/table_connectors.svg" alt="Translation of table connectors" >}}

### 元数据

Table API 和 SQL 都是声明式 API。这包括表的声明。因此，执行 `CREATE TABLE` 语句会导致目标 catalog 中的元数据更新。

对于大多数 catalog 实现，外部系统中的物理数据不会针对此类操作进行修改。特定于连接器的依赖项不必存在于类路径中。在 `WITH` 子句中声明的选项既不被验证也不被解释。

动态表的元数据（ 通过 DDL 创建或由 catalog 提供 ）表示为 `CatalogTable` 的实例。必要时，表名将在内部解析为 `CatalogTable`。

### 解析器

在解析和优化以 table 编写的程序时，需要将 `CatalogTable` 解析为 `DynamicTableSource`（ 用于在 `SELECT` 查询中读取 ）和 `DynamicTableSink`（ 用于在 `INSERT INTO` 语句中写入 ）。

`DynamicTableSourceFactory` 和 `DynamicTableSinkFactory` 提供连接器特定的逻辑，用于将 `CatalogTable` 的元数据转换为 `DynamicTableSource` 和 `DynamicTableSink` 的实例。在大多数情况下，以工厂模式设计的目的是验证选项（例如示例中的 `'port'` = `'5022'` ），配置编码解码格式（ 如果需要 ），并创建表连接器的参数化实例。

默认情况下，`DynamicTableSourceFactory` 和 `DynamicTableSinkFactory` 的实例是使用 Java的 [Service Provider Interfaces (SPI)] (https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) 发现的。 `connector` 选项（例如示例中的 `'connector' = 'custom'`）必须对应于有效的工厂标识符。


尽管在类命名中可能不明显，但 `DynamicTableSource` 和 `DynamicTableSink` 也可以被视为有状态的工厂，它们最终会产生具体的运行时实现来读写实际数据。

规划器使用 source 和 sink 实例来执行连接器特定的双向通信，直到找到最佳逻辑规划。取决于声明可选的接口（ 例如 `SupportsProjectionPushDown` 或 `SupportsOverwrite`），规划器可能会将更改应用于实例并且改变产生的运行时实现。

### 运行时的实现

一旦逻辑规划完成，规划器将从表连接器获取 *runtime implementation*。运行时逻辑在 Flink 的核心连接器接口中实现，例如 `InputFormat` 或 `SourceFunction`。

这些接口按另一个抽象级别被分组为 `ScanRuntimeProvider`、`LookupRuntimeProvider` 和 `SinkRuntimeProvider` 的子类。

例如，`OutputFormatProvider`（ 提供 `org.apache.flink.api.common.io.OutputFormat` ）和 `SinkFunctionProvider`（ 提供`org.apache.flink.streaming.api.functions.sink.SinkFunction`）都是规划器可以处理的 `SinkRuntimeProvider` 具体实例。

{{< top >}}


项目配置
---------------------

如果要实现自定义连接器或自定义格式，通常以下依赖项就足够了：

{{< artifact_tabs flink-table-common withProvidedScope >}}

如果你想开发一个需要与 DataStream API 桥接的连接器（ 即：如果你想将 DataStream 连接器适配到 Table API），你需要添加此依赖项：

{{< artifact_tabs flink-table-api-java-bridge withProvidedScope >}}

在开发 connector/format 时，我们建议同时提供 Thin JAR 和 uber JAR，以便用户可以轻松地在 SQL 客户端或 Flink 发行版中加载 uber JAR 并开始使用它。 uber JAR 应该包含连接器的所有第三方依赖，不包括上面列出的表依赖。

{{< hint warning >}}
你不应该在生产代码中依赖 `flink-table-planner{{< scala_version >}}`。
使用 Flink 1.15 中引入的新模块 `flink-table-planner-loader`，应用程序的类路径将不再直接访问 `org.apache.flink.table.planner` 类。
如果你需要 `org.apache.flink.table.planner` 的包和子包内部可用的功能，请开启一个 issue。要了解更多信息，请查看
[Table 依赖解读]({{< ref "docs/dev/configuration/advanced" >}}#anatomy-of-table-dependencies)。
{{< /hint >}}

<a name="extension-points"></a>

扩展点
----------------

这一部分主要介绍扩展 Flink table connector 时可能用到的接口。

<a name="dynamic-table-factories"></a>

### 动态表的工厂类

在根据 catalog 与 Flink 运行时上下文信息，为某个外部存储系统配置动态表连接器时，需要用到动态表的工厂类。

比如，通过实现 `org.apache.flink.table.factories.DynamicTableSourceFactory` 接口完成一个工厂类，来生产 `DynamicTableSource` 类。

通过实现 `org.apache.flink.table.factories.DynamicTableSinkFactory` 接口完成一个工厂类，来生产 `DynamicTableSink` 类。

默认情况下，Java 的 SPI 机制会自动识别这些工厂类，同时将 `connector` 配置项作为工厂类的”标识符“。

在 JAR 文件中，需要将实现的工厂类路径放入到下面这个配置文件：

`META-INF/services/org.apache.flink.table.factories.Factory`

Flink 会对工厂类逐个进行检查，确保其“标识符”是全局唯一的，并且按照要求实现了上面提到的接口 (比如 `DynamicTableSourceFactory`)。

如果必要的话，也可以在实现 catalog 时绕过上述 SPI 机制识别工厂类的过程。即在实现 catalog 接口时，在`org.apache.flink.table.catalog.Catalog#getFactory` 方法中直接返回工厂类的实例。

<a name="dynamic-table-source"></a>

### 动态表的 source 端

按照定义，动态表是随时间变化的。

在读取动态表时，表中数据可以是以下情况之一：
- changelog 流（支持有界或无界），在 changelog 流结束前，所有的改变都会被源源不断地消费，由 `ScanTableSource` 接口表示。
- 处于一直变换或数据量很大的外部表，其中的数据一般不会被全量读取，除非是在查询某个值时，由 `LookupTableSource` 接口表示。

一个类可以同时实现这两个接口，Planner 会根据查询的 Query 选择相应接口中的方法。

<a name= "scan-table-source"></a>

#### Scan Table Source

在运行期间，`ScanTableSource` 接口会按行扫描外部存储系统中所有数据。

被扫描的数据可以是 insert、update、delete 三种操作类型，因此数据源可以用作读取 changelog （支持有界或无界）。在运行时，返回的 **_changelog mode_** 表示 Planner 要处理的操作类型。

在常规批处理的场景下，数据源可以处理 insert-only 操作类型的有界数据流。

在常规流处理的场景下，数据源可以处理 insert-only 操作类型的无界数据流。

在变更日志数据捕获（即 CDC）场景下，数据源可以处理 insert、update、delete 操作类型的有界或无界数据流。

可以实现更多的功能接口来优化数据源，比如实现 `SupportsProjectionPushDown` 接口，这样在运行时在 source 端就处理数据。在 `org.apache.flink.table.connector.source.abilities` 包下可以找到各种功能接口，更多内容可查看 [source abilities table](#source-abilities)。

实现 `ScanTableSource` 接口的类必须能够生产 Flink 内部数据结构，因此每条记录都会按照`org.apache.flink.table.data.RowData` 的方式进行处理。Flink 运行时提供了转换机制保证 source 端可以处理常见的数据结构，并且在最后进行转换。

<a name="lookup-table-source"></a>

#### Lookup Table Source

在运行期间，`LookupTableSource` 接口会在外部存储系统中按照 key 进行查找。

相比于`ScanTableSource`，`LookupTableSource` 接口不会全量读取表中数据，只会在需要时向外部存储（其中的数据有可能会一直变化）发起查询请求，惰性地获取数据。

同时相较于`ScanTableSource`，`LookupTableSource` 接口目前只支持处理 insert-only 数据流。

暂时不支持扩展功能接口，可查看 `org.apache.flink.table.connector.source.LookupTableSource` 中的文档了解更多。

`LookupTableSource` 的实现方法可以是 `TableFunction` 或者 `AsyncTableFunction`，Flink运行时会根据要查询的 key 值，调用这个实现方法进行查询。

<a name="source-abilities"></a>

#### source 端的功能接口

<table class="table table-bordered">
    <thead>
        <tr>
        <th class="text-left" style="width: 25%">接口</th>
        <th class="text-center" style="width: 75%">描述</th>
        </tr>
    </thead>
    <tbody>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsFilterPushDown.java" name="SupportsFilterPushDown" >}}</td>
        <td>支持将过滤条件下推到 <code>DynamicTableSource</code>。为了更高效处理数据，source 端会将过滤条件下推，以便在数据产生时就处理。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsLimitPushDown.java" name="SupportsLimitPushDown" >}}</td>
        <td>支持将 limit（期望生产的最大数据条数）下推到 <code>DynamicTableSource</code>。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsPartitionPushDown.java" name="SupportsPartitionPushDown" >}}</td>
        <td>支持将可用的分区信息提供给 planner 并且将分区信息下推到 <code>DynamicTableSource</code>。在运行时为了更高效处理数据，source 端会只从提供的分区列表中读取数据。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsProjectionPushDown.java" name="SupportsProjectionPushDown" >}}</td>
        <td>支持将查询列(可嵌套)下推到 <code>DynamicTableSource</code>。为了更高效处理数据，source 端会将查询列下推，以便在数据产生时就处理。如果 source 端同时实现了 <code>SupportsReadingMetadata</code>，那么 source 端也会读取相对应列的元数据信息。
        </td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsReadingMetadata.java" name="SupportsReadingMetadata" >}}</td>
        <td>支持通过 <code>DynamicTableSource</code> 读取列的元数据信息。source 端会在生产数据行时，在最后添加相应的元数据信息，其中包括元数据的格式信息。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsWatermarkPushDown.java" name="SupportsWatermarkPushDown" >}}</td>
        <td>支持将水印策略下推到 <code>DynamicTableSource</code>。水印策略可以通过工厂模式或 Builder 模式来构建，用于抽取时间戳以及水印的生成。在运行时，source 端内部的水印生成器会为每个分区生产水印。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsSourceWatermark.java" name="SupportsSourceWatermark" >}}</td>
        <td>支持使用 <code>ScanTableSource</code> 中提供的水印策略。当使用 <code>CREATE TABLE</code> DDL 时，<可以使用></可以使用> <code>SOURCE_WATERMARK()</code> 来告诉 planner 调用这个接口中的水印策略方法。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsRowLevelModificationScan.java" name="SupportsRowLevelModificationScan" >}}</td>
        <td>支持将读数据的上下文 <code>RowLevelModificationScanContext</code> 从 <code>ScanTableSource</code> 传递给实现了 <code>SupportsRowLevelDelete</code>，<code>SupportsRowLevelUpdate</code> 的 sink 端。
    </tr>
    </tbody>
</table>

<span class="label label-danger">注意</span>上述接口当前只适用于 `ScanTableSource`，不适用于`LookupTableSource`。

<a name="dynamic-table-sink"></a>

### 动态表的 sink 端

按照定义，动态表是随时间变化的。

当写入一个动态表时，数据流可以被看作是 changelog （有界或无界都可），在 changelog 结束前，所有的变更都会被持续写入。在运行时，返回的 **_changelog mode_** 会显示 sink 端支持的数据操作类型。

在常规批处理的场景下，sink 端可以持续接收 insert-only 操作类型的数据，并写入到有界数据流中。

在常规流处理的场景下，sink 端可以持续接收 insert-only 操作类型的数据，并写入到无界数据流中。

在变更日志数据捕获（即 CDC）场景下，sink 端可以将 insert、update、delete 操作类型的数据写入有界或无界数据流。

可以实现 `SupportsOverwrite` 等功能接口，在 sink 端处理数据。可以在 `org.apache.flink.table.connector.sink.abilities` 包下找到各种功能接口，更多内容可查看[sink abilities table](#sink-abilities)。

实现 `DynamicTableSink` 接口的类必须能够处理 Flink 内部数据结构，因此每条记录都会按照 `org.apache.flink.table.data.RowData` 的方式进行处理。Flink 运行时提供了转换机制来保证在最开始进行数据类型转换，以便 sink 端可以处理常见的数据结构。

<a name="sink-abilities"></a>

#### sink 端的功能接口

<table class="table table-bordered">
    <thead>
        <tr>
        <th class="text-left" style="width: 25%">接口</th>
        <th class="text-center" style="width: 75%">描述</th>
        </tr>
    </thead>
    <tbody>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsOverwrite.java" name="SupportsOverwrite" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 覆盖写入已存在的数据。默认情况下，如果不实现这个接口，在使用 <code>INSERT OVERWRITE</code> SQL 语法时，已存在的表或分区不会被覆盖写入</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsPartitioning.java" name="SupportsPartitioning" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 写入分区数据。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsWritingMetadata.java" name="SupportsWritingMetadata" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 写入元数据列。Sink 端会在消费数据行时，在最后接受相应的元数据信息并进行持久化，其中包括元数据的格式信息。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsDeletePushDown.java" name="SupportsDeletePushDown" >}}</td>
        <td>支持将 <code>DELETE</code> 语句中的过滤条件下推到 <code>DynamicTableSink</code>，sink 端可以直接根据过滤条件来删除数据。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsRowLevelDelete.java" name="SupportsRowLevelDelete" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 根据行级别的变更来删除已有的数据。该接口的实现者需要告诉 Planner 如何产生这些行变更，并且需要消费这些行变更从而达到删除数据的目的。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsRowLevelUpdate.java" name="SupportsRowLevelUpdate" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 根据行级别的变更来更新已有的数据。该接口的实现者需要告诉 Planner 如何产生这些行变更，并且需要消费这些行变更从而达到更新数据的目的。</td>
    </tr>
    <tr>
        <td>{{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsStaging.java" name="SupportsStaging" >}}</td>
        <td>支持 <code>DynamicTableSink</code> 提供 CTAS(CREATE TABLE AS SELECT) 或 RTAS([CREATE OR] REPLACE TABLE AS SELECT) 的原子性语义。该接口的实现者需要返回一个提供原子性语义实现的 <code>StagedTable</code> 对象。</td>
    </tr>
    </tbody>
</table>

<a name="encoding--decoding-formats"></a>

### 编码与解码

有的表连接器支持 K/V 型数据的各类编码与解码方式。

编码与解码格式器的工作原理类似于 `DynamicTableSourceFactory -> DynamicTableSource -> ScanRuntimeProvider`，其中工厂类负责传参，source 负责提供处理逻辑。

由于编码与解码格式器处于不同的代码模块，类似于[table factories](#dynamic-table-factories)，它们都需要通过 Java 的 SPI 机制自动识别。为了找到格式器的工厂类，动态表工厂类会根据该格式器工厂类的”标识符“来搜索，并确认其实现了连接器相关的基类。

比如，Kafka 的 source 端需要一个实现了 `DeserializationSchema` 接口的类，用来为数据解码。那么 Kafka 的 source 端工厂类会使用配置项 `value.format` 的值来发现 `DeserializationFormatFactory`。

目前，支持使用如下格式器工厂类:

```
org.apache.flink.table.factories.DeserializationFormatFactory
org.apache.flink.table.factories.SerializationFormatFactory
```

格式器工厂类再将配置传参给 `EncodingFormat` 或 `DecodingFormat`。这些接口是另外一种工厂类，用于为所给的数据类型生成指定的格式器。

例如 Kafka 的 source 端工厂类 `DeserializationFormatFactory` 会为 Kafka 的 source 端返回 `EncodingFormat<DeserializationSchema>`

{{< top >}}

<a name="full-stack-example"></a>

全栈实例
------------------

这一部分介绍如何实现一个 CDC 场景下 scan table 类型的 source 端，同时支持自定义解码器下面的例子中使用了上面提到的所有内容，作为一个参考。

这个例子展示了:
- 创建工厂类实现配置项的解析与校验
- 实现表连接器
- 实现与发现自定义的编码/解码格式器
- 其他工具类，数据结构的转换器以及一个`FactoryUtil`类。

source 端通过实现一个单线程的 `SourceFunction` 接口，绑定一个 socket 端口来监听字节流字节流会被解码为一行一行的数据，解码器是可插拔的。解码方式是将第一列数据作为这条数据的操作类型。

我们使用上文提到的一系列接口来构建这个例子，并通过如下这个 DDL 来创建表:

```sql
CREATE TABLE UserScores (name STRING, score INT)
WITH (
  'connector' = 'socket',
  'hostname' = 'localhost',
  'port' = '9999',
  'byte-delimiter' = '10',
  'format' = 'changelog-csv',
  'changelog-csv.column-delimiter' = '|'
);
```

由于解码时支持 CDC 语义，我们可以在运行时更新数据并且创建一个更新视图，并源源不断地处理变更数据：

```sql
SELECT name, SUM(score) FROM UserScores GROUP BY name;
```

在命令行中输入如下指令，并输入数据:
```text
> nc -lk 9999
INSERT|Alice|12
INSERT|Bob|5
DELETE|Alice|12
INSERT|Alice|18
```

<a name="factories"></a>

### 工厂类

这一部分主要介绍如何从 catalog 中解析元数据信息来构建表连接器的实例。

下面的工厂类都以添加到 `META-INF/services` 目录下。

**`SocketDynamicTableFactory`**

`SocketDynamicTableFactory` 根据 catalog 表信息，生成表的 source 端。由于 source 端需要进行对数据解码，我们通过 `FactoryUtil` 类来找到解码器。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableFactory implements DynamicTableSourceFactory {

  // 定义所有配置项
  public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
    .intType()
    .noDefaultValue();

  public static final ConfigOption<Integer> BYTE_DELIMITER = ConfigOptions.key("byte-delimiter")
    .intType()
    .defaultValue(10); // 等同于 '\n'

  @Override
  public String factoryIdentifier() {
    return "socket"; // 用于匹配： `connector = '...'`
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HOSTNAME);
    options.add(PORT);
    options.add(FactoryUtil.FORMAT); // 解码的格式器使用预先定义的配置项
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(BYTE_DELIMITER);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    // 使用提供的工具类或实现你自己的逻辑进行校验
    final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

    // 找到合适的解码器
    final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
      DeserializationFormatFactory.class,
      FactoryUtil.FORMAT);

    // 校验所有的配置项
    helper.validate();

    // 获取校验完的配置项
    final ReadableConfig options = helper.getOptions();
    final String hostname = options.get(HOSTNAME);
    final int port = options.get(PORT);
    final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

    // 从 catalog 中抽取要生产的数据类型 (除了需要计算的列) 
    final DataType producedDataType =
            context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

    // 创建并返回动态表 source
    return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
  }
}
```

**`ChangelogCsvFormatFactory`**

`ChangelogCsvFormatFactory` 根据解码器相关的配置构建解码器。`SocketDynamicTableFactory` 中的 `FactoryUtil` 会适配好配置项中的键，并处理 `changelog-csv.column-delimiter` 这样带有前缀的键。

由于这个工厂类实现了 `DeserializationFormatFactory` 接口，它也可以为其他连接器（比如 Kafka 连接器）提供反序列化的解码支持。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;

public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

  // 定义所有配置项
  public static final ConfigOption<String> COLUMN_DELIMITER = ConfigOptions.key("column-delimiter")
    .stringType()
    .defaultValue("|");

  @Override
  public String factoryIdentifier() {
    return "changelog-csv";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(COLUMN_DELIMITER);
    return options;
  }

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context,
      ReadableConfig formatOptions) {
    // 使用提供的工具类或实现你自己的逻辑进行校验
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    // 获取校验完的配置项
    final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);

    // 创建并返回解码器
    return new ChangelogCsvFormat(columnDelimiter);
  }
}
```

<a name="table-source-and-decoding-format"></a>

### source 端与解码

这部分主要介绍在计划阶段的 source 与 解码器实例，是如何转化为运行时实例，以便于提交给集群。

**`SocketDynamicTableSource`**

`SocketDynamicTableSource` 在计划阶段中会被用到。在我们的例子中，我们不会实现任何功能接口，因此，`getScanRuntimeProvider(...)` 方法中就是主要逻辑：对 `SourceFunction` 以及其用到的 `DeserializationSchema` 进行实例化，作为运行时的实例。两个实例都被参数化来返回内部数据结构（比如 `RowData`）。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableSource implements ScanTableSource {

  private final String hostname;
  private final int port;
  private final byte byteDelimiter;
  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType producedDataType;

  public SocketDynamicTableSource(
      String hostname,
      int port,
      byte byteDelimiter,
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType) {
    this.hostname = hostname;
    this.port = port;
    this.byteDelimiter = byteDelimiter;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // 在我们的例子中，由解码器来决定 changelog 支持的模式
    // 但是在 source 端指定也可以
    return decodingFormat.getChangelogMode();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

    // 创建运行时类用于提交给集群

    final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
      runtimeProviderContext,
      producedDataType);

    final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(
      hostname,
      port,
      byteDelimiter,
      deserializer);

    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
  }

  @Override
  public String asSummaryString() {
    return "Socket Table Source";
  }
}
```

**`ChangelogCsvFormat`**

`ChangelogCsvFormat` 在运行时使用 `DeserializationSchema` 为数据进行解码，这里支持处理 `INSERT`、`DELETE` 变更类型的数据。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

  private final String columnDelimiter;

  public ChangelogCsvFormat(String columnDelimiter) {
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DeserializationSchema<RowData> createRuntimeDecoder(
      DynamicTableSource.Context context,
      DataType producedDataType) {
    // 为 DeserializationSchema 创建类型信息
    final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(
      producedDataType);

    // DeserializationSchema 中的大多数代码无法处理内部数据结构
    // 在最后为转换创建一个转换器
    final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

    // 在运行时，为解析过程提供逻辑类型
    final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

    // 创建运行时类
    return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // 支持处理 `INSERT`、`DELETE` 变更类型的数据。
    return ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .build();
  }
}
```

<a name="runtime-1"></a>

### 运行时

为了让例子更完整，这部分主要介绍 `SourceFunction` 和 `DeserializationSchema` 的运行逻辑。

**ChangelogCsvDeserializer**

`ChangelogCsvDeserializer` 的解析逻辑比较简单：将字节流数据解析为由 `Integer` 和 `String` 组成的 `Row` 类型，并附带这条数据的操作类型，最后将其转换为内部数据结构。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

  private final List<LogicalType> parsingTypes;
  private final DataStructureConverter converter;
  private final TypeInformation<RowData> producedTypeInfo;
  private final String columnDelimiter;

  public ChangelogCsvDeserializer(
      List<LogicalType> parsingTypes,
      DataStructureConverter converter,
      TypeInformation<RowData> producedTypeInfo,
      String columnDelimiter) {
    this.parsingTypes = parsingTypes;
    this.converter = converter;
    this.producedTypeInfo = producedTypeInfo;
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    // 为 Flink 的核心接口提供类型信息。
    return producedTypeInfo;
  }

  @Override
  public void open(InitializationContext context) {
    // 转化器必须要被开启。
    converter.open(Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
  }

  @Override
  public RowData deserialize(byte[] message) {
    // 按列解析数据，其中一列是 changelog 标记
    final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
    final RowKind kind = RowKind.valueOf(columns[0]);
    final Row row = new Row(kind, parsingTypes.size());
    for (int i = 0; i < parsingTypes.size(); i++) {
      row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
    }
    // 转换为内部数据结构
    return (RowData) converter.toInternal(row);
  }

  private static Object parse(LogicalTypeRoot root, String value) {
    switch (root) {
      case INTEGER:
        return Integer.parseInt(value);
      case VARCHAR:
        return value;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }
}
```

**SocketSourceFunction**

`SocketSourceFunction` 会监听一个 socket 端口并持续消费字节流。它会按照给定的分隔符拆分每条记录，并由插件化的 `DeserializationSchema` 进行解码。目前这个 source 功能只支持平行度为 1。

```java
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

  private final String hostname;
  private final int port;
  private final byte byteDelimiter;
  private final DeserializationSchema<RowData> deserializer;

  private volatile boolean isRunning = true;
  private Socket currentSocket;

  public SocketSourceFunction(String hostname, int port, byte byteDelimiter, DeserializationSchema<RowData> deserializer) {
    this.hostname = hostname;
    this.port = port;
    this.byteDelimiter = byteDelimiter;
    this.deserializer = deserializer;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    deserializer.open(() -> getRuntimeContext().getMetricGroup());
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    while (isRunning) {
      // 持续从 socket 消费数据
      try (final Socket socket = new Socket()) {
        currentSocket = socket;
        socket.connect(new InetSocketAddress(hostname, port), 0);
        try (InputStream stream = socket.getInputStream()) {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          int b;
          while ((b = stream.read()) >= 0) {
            // 持续写入 buffer 直到遇到分隔符
            if (b != byteDelimiter) {
              buffer.write(b);
            }
            // 解码并处理记录
            else {
              ctx.collect(deserializer.deserialize(buffer.toByteArray()));
              buffer.reset();
            }
          }
        }
      } catch (Throwable t) {
        t.printStackTrace(); // 打印并继续
      }
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    try {
      currentSocket.close();
    } catch (Throwable t) {
      // 忽略
    }
  }
}
```

{{< top >}}
