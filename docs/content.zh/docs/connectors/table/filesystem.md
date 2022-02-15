---
title: 文件系统
weight: 8
type: docs
aliases:
- /dev/table/connectors/filesystem.html
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

此连接器提供对文件系统中分区文件的访问，
由 [Flink FileSystem abstraction]({{< ref "docs/deployment/filesystems/overview" >}}) 提供支持。

在 Flink 中包含了该文件系统连接器，不需要添加额外的依赖。
相应的 jar 包可以在 Flink 工程项目的 `/lib` 目录下找到。
从文件系统中读取或者写入行时，需要指定相应的格式。

文件系统连接器允许从本地或分布式文件系统进行读写。文件系统表可以定义为：

```sql
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- 必选项：指定连接器
  'path' = 'file:///path/to/whatever',  -- 必选项：路径
  'format' = '...',                     -- 必选项：文件系统连接器指定格式
                                        -- 有关更多详情，请参考 Table Formats
  'partition.default-name' = '...',     -- 可选项：默认为 partition-name，防止动态分区
                                        -- 默认值为空字符串

  -- 可选项：true：代表开启在 Sink 阶段对动态分区字段的 shuffle 操作，这可以大大减少文件系统 Sink 输出的文件数量，但是可能会导致数据倾斜，默认值：false
  'sink.shuffle-by-partition.enable' = '...',
  ...
)
```

{{< hint info >}}
确定包含 [Flink File System specific dependencies]({{< ref "docs/deployment/filesystems/overview" >}}).
{{< /hint >}}

{{< hint info >}}
基于流的文件系统源仍在开发中。未来，社区将增加对常见的流式用例的支持，例如，分区和目录监控。
{{< /hint >}}

{{< hint warning >}}
文件系统连接器的行为与 `previous legacy filesystem connector` 有很大不同：
路径参数是指定目录，而不是指定文件，并且在你声明的路径中无法获取人们可以读懂的文件。
{{< /hint >}}
<a name="partition-files"></a>

## 分区文件

 Flink 的文件系统分区支持标准的 Hive 格式。但是，它不要求分区预先注册到表目录中。分区是根据目录结构发现和推断的。 例如，根据下面的目录进行分区的表将被推断为包含 `datetime` 和 `hour` 分区。

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

文件系统表支持分区插入和覆盖插入。 请参考 [INSERT Statement]({{< ref "docs/dev/table/sql/insert" >}})。当你在分区表中覆盖插入时，只会覆盖相应的分区，而不会覆盖整个表。
<a name="file-formats"></a>

## 文件格式

文件系统连接器支持多种文件格式：

- CSV： [RFC-4180](https://tools.ietf.org/html/rfc4180) 。 未压缩。
- JSON：注意，文件系统连接器的 JSON 格式不是典型的 JSON 文件，而是未压缩的。[换行符分割的 JSON](http://jsonlines.org/) 。
- Avro：[Apache Avro](http://avro.apache.org) 。通过配置参数 `avro.codec` 支持压缩。
- Parquet：[Apache Parquet](http://parquet.apache.org) 。兼容 Hive 。
- Orc：[Apache Orc](http://orc.apache.org) 。兼容 Hive 。
- Debezium-JSON：[debezium-json]({{< ref "docs/connectors/table/formats/debezium" >}}) 。
- Canal-JSON：[canal-json]({{< ref "docs/connectors/table/formats/canal" >}}) 。
- Raw：[raw]({{< ref "docs/connectors/table/formats/raw" >}}) 。
<a name="source"></a>

## Source

文件系统连接器可用于将当个文件或整个目录的数据读取到单个表中。

当使用目录作为源路径时，对目录中的文件进行 **无序的读取** 。
<a name="directory-watching"></a>

### 目录监控

当运行模式配置为流模式时，文件系统连接器会自动监控输入目录。

你可以使用以下选项修改监控时间间隔。

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
        <td>设置监控新文件的时间间隔，并且必须大于0。 
        每个文件都由其路径唯一标识，一旦被发现，就会被处理一次。 
        已处理的文件在数据源的整个生命周期内保持某种状态，因此，它与数据源的状态一起保存在 Checkpoint 和 Savepoint 中。 
        更短的时间间隔意味着文件被更快地发现，但也意味着更频繁地遍历文件系统/对象存储。 
        如果未设置此配置选项，则提供的路径被扫描一次，因此数据源将是被绑定的。</td>
    </tr>
  </tbody>
</table>

<a name="available-metadata"></a>

### 可用元数据

以下连接器元数据可以作为表定义中对元数据进行访问。所有元数据都是只读的。

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
      <td>输入文件的完整路径。</td>
    </tr>
    <tr>
      <td><code>file.name</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>文件名，即距离文件跟路径最远的元素。</td>
    </tr>
    <tr>
      <td><code>file.size</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td>文件的字节数。</td>
    </tr>
    <tr>
      <td><code>file.modification-time</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>文件的修改时间。</td>
    </tr>
    </tbody>
</table>

扩展的 `CREATE TABLE` 示例演示了标识某个字段为元数据的语法：

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

文件系统连接器支持流写入，是基于 Flink 的 [文件系统]({{< ref "docs/connectors/datastream/filesystem" >}}) 写入文件的。使用行编码格式的包含 CSV 和 JSON 。 使用块编码格式的包含 Parquet、ORC 和 Avro 。

你可以直接写 SQL 语句，插入流数据到非分区的表中。
如果是分区表你可以配置分区相关的操作。请参考 [分区提交](#partition-commit) 了解更多详情。
<a name="rolling-policy"></a>

### 回滚策略

数据通过分区目录被分割成了多个文件。对于接收到该分区数据的接收器的每个 Subtask，每个分区将至少包含一个文件。正在进行中的文件将被关闭，并根据配置的滚动策略创建其他文件。策略根据大小回滚文件，超时配置指定了文件可以打开的最长时间。

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
        <td>回滚之前最大文件大小。</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td>回滚前文件可以保持打开状态的最长时间（默认30分钟，为了避免产生过多小文件）。
        检查频率是由配置 'sink.rolling-policy.check-interval' 参数进行控制的。</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td style="word-wrap: break-word;">1 min</td>
        <td>Duration</td>
        <td>基于回滚策略的检查时间间隔。控制一个文件基于参数 'sink.rolling-policy.rollover-interval' 判断是否回滚的检查频率。</td>
    </tr>
  </tbody>
</table>

**注意：** 对于块格式数据 (parquet、 orc、 avro)，回滚策略与 Checkpoint 间隔（挂起的文件在下一个 Checkpoint 完成）相结合，控制这些文件的大小和数量。

**注意：** 对于行格式数据 (csv、 json)，你可以在连接器配置文件中配置  `sink.rolling-policy.file-size` 或 `sink.rolling-policy.rollover-interval` 参数并且在 flink-conf.yaml 文件中配置 `execution.checkpointing.interval` 参数。
如果不想在文件系统中观察到数据出现之前等待很长时间。对于其他数据格式 (avro, orc)， 你可以仅在 flink-conf.yaml 文件中配置 `execution.checkpointing.interval` 参数即可。
<a name="file-compaction"></a>

### 文件压缩

输出文件算子支持文件压缩，它允许应用程序在不生成大量文件的情况下拥有较小的 Checkpoint 间隔。

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
        <td>不论是否在流式 Sink 中启用自动压缩。数据都将被写入临时文件。当 Checkpoint 完成后，将生成压缩的临时文件。压缩之前临时文件不可见。</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>MemorySize</td>
        <td>压缩的目标文件大小，默认值为回滚文件大小。</td>
    </tr>
  </tbody>
</table>

如果启用文件压缩，将根据目标文件大小将多个小文件合并为更大的文件。
在生产环境中运行文件压缩时，请注意以下几点：
- 仅压缩单个 Checkpoint 的文件，即生成文件数至少与 Checkpoint 数相同。
- 合并前的文件是不可见的，那么文件的可见时间可能是：Checkpoint 间隔时间 + 压缩时间。
- 如果压缩时间过长，将导致反压并且延长 Checkpoint 时间。

<a name="partition-commit"></a>

### 分区提交

写入分区之后，通常需要通知下游应用程序。例如，添加分区信息到 Hive 元数据中或者在目录中添加 `_SUCCESS` 文件。文件系统接收器包含一个分区提交功能，允许配置自定义策略。提交操作是基于 `触发器` 和 `策略` 的共同作用。

- 触发器：提交分区的时间可以通过 Watermark 和从分区提取的时间来确定，也可以通过处理时间来确定。
- 策略：如何提交分区，内置策略支持提交成功文件和元数据，你还可以实现自己的策略，例如触发配置单元分析来生成统计信息，或合并小文件，等等。

**注意：** 分区提交仅在动态分区插入情况下使用。
<a name="partition-commit-trigger"></a>

#### 分区提交触发器

当提交分区时需要提供分区提交触发器：

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
        <td>分区提交的触发器类型：'process-time'：基于机器时间，它既不要求分区时间提取也不要求 Watermark 生成。一旦当前系统时间大于 分区创建时间+延迟时间 的值立即提交分区。'partition-time'： 基于提取的分区时间，它要求 Watermark 生成。一旦 Watermark 时间大于 分区提取时间+延迟时间 的值立即提交分区。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td>分区提交的延迟时间。如果是按天分区，可以设置为 '1 d'，如果是按小时分区，可以设置为 '1 h'。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.watermark-time-zone</h5></td>
        <td style="word-wrap: break-word;">UTC</td>
        <td>String</td>
        <td>解析 Long 类型的 Watermark 值为时间戳时的所在时区，解析 Watermark 时间戳是为了和分区时间进行比较进而决定是否提交分区。这个选项仅当 `sink.partition-commit.trigger` 被设置为 'partition-time' 时有效。如果这个选项不能正确配置，例如，source rowtime 被定义为 TIMESTAMP_LTZ 类型的列，但是这个选项没有配置，那么用户可能会看到延迟几个小时后才提交的分区。 默认值为 'UTC'，意味着 Watermark 被定义在 TIMESTAMP 类型的列上或者没有被定义。如果 Watermark 被定义在 TIMESTAMP_LTZ 类型的列上， Watermark 时区必须是会话时区（session time zone）。该选项可以配置成全名，例如 'America/Los_Angeles'，也可以自定义时区，例如 'GMT-08:00'。</td>
    </tr>    
  </tbody>
</table>

触发器有两种类型：
- 第一种是分区处理时间。它既不要求额外的分区时间，也不需要 Watermark 生成。这种分区提交触发器基于分区创建时间和当前系统时间。
  这种触发器更具普遍性，但不是很精确。例如，数据延迟或故障将导致过早提交分区。
- 第二种提交分区触发器是根据在分区值提取的时间和 Watermark 共同触发。
  这种要求 Job 中有 Watermark 生成，并且根据时间进行分区，例如，按小时分区或按天分区。

如果你想让下游尽快看到分区，不论数据处理是否完成：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='0s' (默认值)
  一旦数据进入分区，将立即提交。注意：这个分区将被多次提交。

如果你想当数据处理完成时下游才看到分区，你的 Job 中有 Watermark 生成以及从分区值中提取的时间：
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1h' ('1h' 如果你的分区所依赖的分区类型是按小时分区)
  这是提交分区的最准确方法，它将尝试确保提交的分区数据尽可能完整。

如果你想当数据处理完成时下游才看到分区，但是没有 Watermark 生成或者不能从分区值中提取时间：
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='1h' ('1h' 如果你的分区所依赖的分区类型是按小时分区)
  能够比较精确的提交分区，但是数据延迟或者故障将导致过早提交分区。

迟到数据的处理: 当一条记录被写入已经提交的分区时，该记录被写入其分区，然后该分区的提交将被再次触发。
<a name="partition-time-extractor"></a>

#### 分区时间提取器

时间提取器从分区中提取时间。

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
        <td>时间提取器从分区中提取时间。支持 default 和 custom。 默认情况下，可以配置时间戳模式/格式。对于 custom，应该配置自定义的提取器类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td>实现 PartitionTimeExtractor 接口的提取器类。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td>允许用户使用分区字段来获取合法的时间戳模式的默认格式化方式。默认支持字段采用 'yyyy-MM-dd hh:mm:ss' 这种格式。如果时间提取来自单一分区字段 'dt'， 可以配置成：'$dt'。如果时间提取来自多个分区字段，例如 'year'、 'month'、 'day' 和 'hour'，可以配置成: '$year-$month-$day $hour:00:00'。如果时间提取来自两个分区字段 'dt' 和 'hour'，可以配置成： '$dt $hour:00:00'。</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-formatter</h5></td>
        <td style="word-wrap: break-word;">yyyy-MM-dd&nbsp;HH:mm:ss</td>
        <td>String</td>
        <td>转换分区时间字符串值为时间戳的格式化类型，分区时间字符串值通过 'partition.time-extractor.timestamp-pattern' 参数表达。例如，分区时间提取来自多个分区字段，比如 'year'、 'month' 和 'day'，你可以配置 'partition.time-extractor.timestamp-pattern' 参数为 '$year$month$day'，并且配置 `partition.time-extractor.timestamp-formatter` 参数为 'yyyyMMdd'。默认的格式是 'yyyy-MM-dd HH:mm:ss'。
            <br>这的时间戳格式化和 Java 的 <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a> 是通用的。
 				</td>
    </tr>
  </tbody>
</table>

默认情况下，提取器基于由分区字段组成的时间戳模式。你也可以指定一个实现接口 `PartitionTimeExtractor` 的完全自定义的分区提取器。

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

分区提交策略定义了提交分区时采取的操作。

- 第一种是元数据存储（metastore），仅 Hive 表支持的元数据存储策略，文件系统通过目录结构管理分区。
- 第二种是成功文件，它是一个即将在分区对应的目录中写入的空文件。

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
        <td>提交分区策略通知下游该分区已完成写，该分区可以被读取。metastore：向元数据存储添加分区。仅 Hive 支持 metastore 策略，文件系统通过目录结构管理分区。success-file： 在目录中增加 '_success' 文件。 两者可以同时设置：'metastore,success-file'。自定义: 通过策略类创建提交策略。也是支持多个设置的：'metastore,success-file'。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td style="word-wrap: break-word;">(无)</td>
        <td>String</td>
        <td>实现 PartitionCommitPolicy 接口的分区提交策略类。仅在自定义提交策略时生效。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td style="word-wrap: break-word;">_SUCCESS</td>
        <td>String</td>
        <td>分区提交策略添加的成功文件名，默认值为 '_SUCCESS'。</td>
    </tr>
  </tbody>
</table>

你可以扩展提交策略的实现，可以参考如下方式进行自定义提交策略的实现：

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

将文件写入外部文件系统（包括 Hive）的 Parallelism 可以通过相应的表选项进行配置，在流模式和批模式下都是支持的。默认情况下，Sink Parallelism 配置与上游链式算子的 Parallelism 相同。当配置的 Sink Parallelism 不同于上游算子的 Parallelism 时，写入文件操作和压缩文件操作（如果启用压缩）将使用该配置的 Sink Parallelism。


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
        <td>将文件写入外部文件系统的 Parallelism 。这个值应该大于0否则抛异常。</td>
    </tr>

  </tbody>
</table>

**注意：** 目前，当且仅当上游的 changelog 模式为 **INSERT-ONLY** 时，才支持配置 Sink Parallelism。否则，程序将会抛出异常。
<a name="full-example"></a>

## 完整示例

以下示例展示了如何使用文件系统连接器编写流式查询，将数据从 Kafka 写入文件系统，然后运行批式查询将数据读回。

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

如果 Watermark 被定义在 TIMESTAMP_LTZ 类型的列上并且使用 `partition-time` 模式进行提交，`sink.partition-commit.watermark-time-zone` 这个参数要求设置成会话时区，否则分区提交可能会延迟几个小时。
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
