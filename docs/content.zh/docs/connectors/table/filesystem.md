---
title: 文件系统
weight: 8
type: docs
aliases:
- /zh/dev/table/connectors/filesystem.html
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

<a name="filesystem-sql-connector"></a>

# 文件系统 SQL 连接器

此连接器提供了对 [Flink FileSystem abstraction]({{< ref "docs/deployment/filesystems/overview" >}}) 支持的文件系统中分区文件的访问。

在 Flink 中包含了该文件系统连接器，不需要添加额外的依赖。相应的 jar 包可以在 Flink 工程项目的 `/lib` 目录下找到。从文件系统中读取或者向文件系统中写入行时，需要指定相应的 format。

文件系统连接器允许从本地或分布式文件系统进行读写。文件系统表可以定义为：

```sql
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- 必选：指定连接器类型
  'path' = 'file:///path/to/whatever',  -- 必选：指定路径
  'format' = '...',                     -- 必选：文件系统连接器指定 format
                                        -- 有关更多详情，请参考 Table Formats
  'partition.default-name' = '...',     -- 可选：默认的分区名，动态分区模式下分区字段值是 null 或空字符串

  -- 可选：该属性开启了在 sink 阶段通过动态分区字段来 shuffle 数据，该功能可以大大减少文件系统 sink 的文件数，但是可能会导致数据倾斜，默认值是 false
  'sink.shuffle-by-partition.enable' = '...',
  ...
)
```

{{< hint info >}}
请确保包含 [Flink File System specific dependencies]({{< ref "docs/deployment/filesystems/overview" >}})。
{{< /hint >}}

{{< hint info >}}
基于流的文件系统 sources 仍在开发中。未来，社区将增加对常见地流式用例的支持，例如，对分区和目录的监控等。
{{< /hint >}}

{{< hint warning >}}
文件系统连接器的特性与 `previous legacy filesystem connector` 有很大不同：
path 属性指定的是目录，而不是文件，该目录下的文件也不是肉眼可读的。
{{< /hint >}}

<a name="partition-files"></a>

## 分区文件

Flink 的文件系统连接器支持分区，使用了标准的 hive。但是，不需要预先注册分区到 table catalog，而是基于目录结构自动做了分区发现。例如，根据下面的目录结构，分区表将被推断包含 `datetime` 和 `hour` 分区。

```
path
└── datetime=2019-08-25
    └── hour=11
        ├── part-0.parquet
        ├── part-1.parquet
    └── hour=12
        ├── part-0.parquet
└── datetime=2019-08-26
    └── hour=6
        ├── part-0.parquet
```

文件系统表支持分区新增插入和分区覆盖插入。请参考 [INSERT Statement]({{< ref "docs/dev/table/sql/insert" >}})。当对分区表进行分区覆盖插入时，只有相应的分区会被覆盖，而不是整个表。

<a name="file-formats"></a>

## File Formats

文件系统连接器支持多种 format：

- CSV：[RFC-4180](https://tools.ietf.org/html/rfc4180)。是非压缩的。
- JSON：注意，文件系统连接器的 JSON format 与传统的标准的 JSON file 的不同，而是非压缩的。[换行符分割的 JSON](http://jsonlines.org/)。
- Avro：[Apache Avro](http://avro.apache.org)。通过配置 `avro.codec` 属性支持压缩。
- Parquet：[Apache Parquet](http://parquet.apache.org)。兼容 hive。
- Orc：[Apache Orc](http://orc.apache.org)。兼容 hive。
- Debezium-JSON：[debezium-json]({{< ref "docs/connectors/table/formats/debezium" >}})。
- Canal-JSON：[canal-json]({{< ref "docs/connectors/table/formats/canal" >}})。
- Raw：[raw]({{< ref "docs/connectors/table/formats/raw" >}})。

<a name="source"></a>

## Source

文件系统连接器可用于将单个文件或整个目录的数据读取到单个表中。

当使用目录作为 source 路径时，对目录中的文件进行 **无序的读取**。

<a name="directory-watching"></a>

### 目录监控

当运行模式为流模式时，文件系统连接器会自动监控输入目录。

可以使用以下属性修改监控时间间隔。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>Duration</td>
        <td> 设置新文件的监控时间间隔，并且必须设置 > 0 的值。 
        每个文件都由其路径唯一标识，一旦发现新文件，就会处理一次。 
        已处理的文件在 source 的整个生命周期内存储在 state 中，因此，source 的 state 在 checkpoint 和 savepoint 时进行保存。 
        更短的时间间隔意味着文件被更快地发现，但也意味着更频繁地遍历文件系统/对象存储。 
        如果未设置此配置选项，则提供的路径仅被扫描一次，因此源将是有界的。</td>
    </tr>
  </tbody>
</table>

<a name="available-metadata"></a>

### 可用的 Metadata

以下连接器 metadata 可以在表定义时作为 metadata 列进行访问。所有 metadata 都是只读的。

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">键</th>
      <th class="text-center" style="width: 30%">数据类型</th>
      <th class="text-center" style="width: 40%">描述</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>file.path</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td> 输入文件的完整路径。</td>
    </tr>
    <tr>
      <td><code>file.name</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td> 文件名，即距离文件根路径最远的元素。</td>
    </tr>
    <tr>
      <td><code>file.size</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td> 文件的字节数。</td>
    </tr>
    <tr>
      <td><code>file.modification-time</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td> 文件的修改时间。</td>
    </tr>
    </tbody>
</table>

扩展的 `CREATE TABLE` 示例演示了标识某个字段为 metadata 的语法：

```sql
CREATE TABLE MyUserTableWithFilepath (
  column_name1 INT,
  column_name2 STRING,
  `file.path` STRING NOT NULL METADATA
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///path/to/whatever',
  'format' = 'json'
)
```

<a name="streaming-sink"></a>

## Streaming Sink

文件系统连接器支持流写入，是基于 Flink 的 [文件系统]({{< ref "docs/connectors/datastream/filesystem" >}}) 写入文件的。CSV 和 JSON 使用的是 Row-encoded Format。Parquet、ORC 和 Avro 使用的是 Bulk-encoded Format。

可以直接编写 SQL，将流数据插入到非分区表。
如果是分区表，可以配置分区操作相关的属性。请参考[分区提交](#partition-commit)了解更多详情。

<a name="rolling-policy"></a>

### 滚动策略

分区目录下的数据被分割到 part 文件中。每个分区对应的 sink 的收到的数据的 subtask 都至少会为该分区生成一个 part 文件。根据可配置的滚动策略，当前 in-progress part 文件将被关闭，生成新的 part 文件。该策略基于大小，和指定的文件可被打开的最大 timeout 时长，来滚动 part 文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.rolling-policy.file-size</h5></td>
        <td style="word-wrap: break-word;">128MB</td>
        <td>MemorySize</td>
        <td> 滚动前，part 文件最大大小。</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td> 滚动前，part 文件处于打开状态的最大时长（默认值30分钟，以避免产生大量小文件）。
        检查频率是由 'sink.rolling-policy.check-interval' 属性控制的。</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td style="word-wrap: break-word;">1 min</td>
        <td>Duration</td>
        <td> 基于时间的滚动策略的检查间隔。该属性控制了基于 'sink.rolling-policy.rollover-interval' 属性检查文件是否该被滚动的检查频率。</td>
    </tr>
  </tbody>
</table>

**注意：** 对于 bulk formats 数据 (parquet、orc、avro)，滚动策略与 checkpoint 间隔（pending 状态的文件会在下个 checkpoint 完成）控制了 part 文件的大小和个数。

**注意：** 对于 row formats 数据 (csv、json)，如果想使得分区文件更快在文件系统中可见，可以设置  `sink.rolling-policy.file-size` 或 `sink.rolling-policy.rollover-interval` 属性以及在 flink-conf.yaml 中的 `execution.checkpointing.interval` 属性。
对于其他 formats (avro、orc)，可以只设置 flink-conf.yaml 中的 `execution.checkpointing.interval` 属性。

<a name="file-compaction"></a>

### 文件合并

file sink 支持文件合并，允许应用程序使用较小的 checkpoint 间隔而不产生大量小文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>auto-compaction</h5></td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td> 在流式 sink 中是否开启自动合并功能。数据首先会被写入临时文件。当 checkpoint 完成后，该检查点产生的临时文件会被合并。这些临时文件在合并前不可见。</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>MemorySize</td>
        <td> 合并目标文件大小，默认值为滚动文件大小。</td>
    </tr>
  </tbody>
</table>

如果启用文件合并功能，会根据目标文件大小，将多个小文件合并成大文件。
在生产环境中使用文件合并功能时，需要注意：
- 只有 checkpoint 内部的文件才会被合并，至少生成的文件个数与 checkpoint 个数相同。
- 合并前文件是可见的，那么文件的可见时间是：checkpoint 间隔时长 + 合并时长。
- 如果合并时间过长，将导致反压，延长 checkpoint 所需时间。

<a name="partition-commit"></a>

### 分区提交

数据写入分区之后，通常需要通知下游应用。例如，在 hive metadata 中新增分区或者在目录下生成 `_SUCCESS` 文件。分区提交策略是可定制的。具体分区提交行为是基于 `triggers` 和 `policies` 的组合。

- Trigger：分区提交时机，可以基于从分区中提取的时间对应的 watermark，或者基于处理时间。
- Policy：分区提交策略，内置策略包括生成 `_SUCCESS` 文件和提交 hive metastore，也可以实现自定义策略，例如触发 hive 生成统计信息，合并小文件等。

**注意：** 分区提交仅在动态分区插入模式下才有效。

<a name="partition-commit-trigger"></a>

#### 分区提交触发器

通过配置分区提交触发策略，来决定何时提交分区：

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.trigger</h5></td>
        <td style="word-wrap: break-word;">process-time</td>
        <td>String</td>
        <td> 分区提交触发器类型：
        'process-time'：基于机器时间，既不需要分区时间提取器也不需要 watermark 生成器。一旦 "当前系统时间" 超过了 "分区创建系统时间" 和 'sink.partition-commit.delay' 之和立即提交分区。
        'partition-time'：基于提取的分区时间，需要 watermark 生成。一旦 watermark 超过了 "分区创建系统时间" 和 'sink.partition-commit.delay' 之和立即提交分区。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td> 该延迟时间之前分区不会被提交。如果是按天分区，可以设置为 '1 d'，如果是按小时分区，应设置为 '1 h'。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.watermark-time-zone</h5></td>
        <td style="word-wrap: break-word;">UTC</td>
        <td>String</td>
        <td> 解析 Long 类型的 watermark 到 TIMESTAMP 类型时所采用的时区，解析得到的 watermark 的 TIMESTAMP 会被用来跟分区时间进行比较以判断是否该被提交。这个属性仅当 `sink.partition-commit.trigger` 被设置为 'partition-time' 时有效。如果这个属性设置的不正确，例如，在 TIMESTAMP_LTZ 类型的列上定义了 source rowtime，如果没有设置该属性，那么用户可能会在若干个小时后才看到分区的提交。默认值为 'UTC'，意味着 watermark 是定义在 TIMESTAMP 类型的列上或者没有定义 watermark。如果 watermark 定义在 TIMESTAMP_LTZ 类型的列上，watermark 时区必须是会话时区（session time zone）。该属性的可选值要么是完整的时区名比如 'America/Los_Angeles'，要么是自定义时区，例如 'GMT-08:00'。</td>
    </tr>    
  </tbody>
</table>

Flink 提供了两种类型分区提交触发器：
- 第一种是根据分区的处理时间。既不需要额外的分区时间，也不需要 watermark 生成。这种分区提交触发器基于分区创建时间和当前系统时间。
  这种触发器更具通用性，但不是很精确。例如，数据延迟或故障将导致过早提交分区。
- 第二种是根据从分区字段提取的时间以及 watermark。
  这需要 job 支持 watermark 生成，分区是根据时间来切割的，例如，按小时或按天分区。

不管分区数据是否完整而只想让下游尽快感知到分区：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='0s' (默认值)
  一旦数据进入分区，将立即提交分区。注意：这个分区可能会被提交多次。

如果想让下游只有在分区数据完整时才感知到分区，并且 job 中有 watermark 生成，也能从分区字段的值中提取到时间：
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1h' (根据分区类型指定，如果是按小时分区可配置为 '1h')
  该方式是最精准地提交分区的方式，尽力确保提交分区的数据完整。

如果想让下游系统只有在数据完整时才感知到分区，但是没有 watermark，或者无法从分区字段的值中提取时间：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='1h' (根据分区类型指定，如果是按小时分区可配置为 '1h')
  该方式尽量精确地提交分区，但是数据延迟或者故障将导致过早提交分区。

延迟数据的处理：延迟的记录会被写入到已经提交的对应分区中，且会再次触发该分区的提交。

<a name="partition-time-extractor"></a>

#### 分区时间提取器

时间提取器从分区字段值中提取时间。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>partition.time-extractor.kind</h5></td>
        <td style="word-wrap: break-word;">default</td>
        <td>String</td>
        <td> 从分区字段中提取时间的时间提取器。支持 default 和 custom。默认情况下，可以配置 timestamp pattern/formatter。对于 custom，应指定提取器类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td> 实现 PartitionTimeExtractor 接口的提取器类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td> 允许用户使用分区字段来获取合法的 timestamp pattern 的默认 construction 方式。默认支持第一个字段按 'yyyy-MM-dd hh:mm:ss' 这种模式提取。
        如果需要从一个分区字段 'dt' 提取 timestamp，可以配置成：'$dt'。
        如果需要从多个分区字段，比如 'year'、'month'、'day' 和 'hour' 提取 timestamp，可以配置成: '$year-$month-$day $hour:00:00'。
        如果需要从两个分区字段 'dt' 和 'hour' 提取 timestamp，可以配置成：'$dt $hour:00:00'。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-formatter</h5></td>
        <td style="word-wrap: break-word;">yyyy-MM-dd&nbsp;HH:mm:ss</td>
        <td>String</td>
        <td> 转换分区 timestamp 字符串值为 timestamp 的 formatter，分区 timestamp 字符串值通过 'partition.time-extractor.timestamp-pattern' 属性表达。例如，分区 timestamp 提取来自多个分区字段，比如 'year'、'month' 和 'day'，可以配置 'partition.time-extractor.timestamp-pattern' 属性为 '$year$month$day'，并且配置 `partition.time-extractor.timestamp-formatter` 属性为 'yyyyMMdd'。默认的 formatter 是 'yyyy-MM-dd HH:mm:ss'。
            <br>这的 timestamp-formatter 和 Java 的 <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a> 是通用的。
 				</td>
    </tr>
  </tbody>
</table>

默认情况下，提取器基于由分区字段组成的 timestamp pattern。也可以指定一个实现接口 `PartitionTimeExtractor` 的自定义提取器。

```java

public class HourPartTimeExtractor implements PartitionTimeExtractor {
    @Override
    public LocalDateTime extract(List<String> keys, List<String> values) {
        String dt = values.get(0);
        String hour = values.get(1);
		return Timestamp.valueOf(dt + " " + hour + ":00:00").toLocalDateTime();
	}
}

```

<a name="partition-commit-policy"></a>

#### 分区提交策略

分区提交策略定义了提交分区时的具体操作。

- 第一种是 metadata 存储（metastore），仅 hive 表支持该策略，该策略下文件系统通过目录层次结构来管理分区。
- 第二种是 success 文件，该策略下会在分区对应的目录下生成一个名为 `_SUCCESS` 的空文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.policy.kind</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td> 分区提交策略通知下游某个分区已经写完毕可以被读取了。
        metastore：向 metadata 增加分区。仅 hive 支持 metastore 策略，文件系统通过目录结构管理分区；
        success-file：在目录中增加 '_success' 文件；
        上述两个策略可以同时指定：'metastore,success-file'。
        custom：通过指定的类来创建提交策略。
        支持同时指定多个提交策略：'metastore,success-file'。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td> 实现 PartitionCommitPolicy 接口的分区提交策略类。只有在 custom 提交策略下才使用该类。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td style="word-wrap: break-word;">_SUCCESS</td>
        <td>String</td>
        <td> 使用 success-file 分区提交策略时的文件名，默认值是 '_SUCCESS'。</td>
    </tr>
  </tbody>
</table>

也可以自定义提交策略，例如：

```java

public class AnalysisCommitPolicy implements PartitionCommitPolicy {
    private HiveShell hiveShell;
	
    @Override
	public void commit(Context context) throws Exception {
	    if (hiveShell == null) {
	        hiveShell = createHiveShell(context.catalogName());
	    }
	    
        hiveShell.execute(String.format(
            "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s = '%s') location '%s'",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0),
	        context.partitionPath()));
	    hiveShell.execute(String.format(
	        "ANALYZE TABLE %s PARTITION (%s = '%s') COMPUTE STATISTICS FOR COLUMNS",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0)));
	}
}

```

<a name="sink-parallelism"></a>

## Sink Parallelism

在流模式和批模式下，向外部文件系统（包括 hive）写文件时的 parallelism 可以通过相应的 table 配置项指定。默认情况下，该 sink parallelism 与上游 chained operator 的 parallelism 一样。当配置了跟上游的 chained operator 不一样的 parallelism 时，写文件和合并文件的算子（如果开启的话）会使用指定的 sink parallelism。


<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">键</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.parallelism</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>Integer</td>
        <td>将文件写入外部文件系统的 parallelism。这个值应该大于0否则抛异常。</td>
    </tr>

  </tbody>
</table>

**注意：** 目前，当且仅当上游的 changelog 模式为 **INSERT-ONLY** 时，才支持配置 sink parallelism。否则，程序将会抛出异常。

<a name="full-example"></a>

## 完整示例

以下示例展示了如何使用文件系统连接器编写流式查询语句，将数据从 Kafka 写入文件系统，然后运行批式查询语句读取数据。

```sql

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  log_ts TIMESTAMP(3),
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
) WITH (...);

CREATE TABLE fs_table (
  user_id STRING,
  order_amount DOUBLE,
  dt STRING,
  `hour` STRING
) PARTITIONED BY (dt, `hour`) WITH (
  'connector'='filesystem',
  'path'='...',
  'format'='parquet',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.policy.kind'='success-file'
);

-- 流式 sql，插入文件系统表
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(log_ts, 'yyyy-MM-dd'),
    DATE_FORMAT(log_ts, 'HH') 
FROM kafka_table;

-- 批式 sql，使用分区修剪进行选择
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

如果 watermark 被定义在 TIMESTAMP_LTZ 类型的列上并且使用 `partition-time` 模式进行提交，`sink.partition-commit.watermark-time-zone` 这个属性需要设置成会话时区，否则分区提交可能会延迟若干个小时。
```sql

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  ts BIGINT, -- 以毫秒为单位的时间
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 TIMESTAMP_LTZ 列上定义 watermark
) WITH (...);

CREATE TABLE fs_table (
  user_id STRING,
  order_amount DOUBLE,
  dt STRING,
  `hour` STRING
) PARTITIONED BY (dt, `hour`) WITH (
  'connector'='filesystem',
  'path'='...',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- 假设用户配置的时区为 'Asia/Shanghai'
  'sink.partition-commit.policy.kind'='success-file'
);

-- 流式 sql，插入文件系统表
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'),
    DATE_FORMAT(ts_ltz, 'HH') 
FROM kafka_table;

-- 批式 sql，使用分区修剪进行选择
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

{{< top >}}
