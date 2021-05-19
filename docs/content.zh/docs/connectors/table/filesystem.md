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

# 文件系统 SQL 连接器 

该连接器提供了对 [Flink 文件系统抽象]({{< ref "docs/deployment/filesystems/overview" >}}) 支持的文件系统中的分区文件的访问.

文件系统连接器本身就被包括在 Flink 中，不需要任何额外的依赖。当从文件系统中读取或向文件系统写入记录时，需要指定相应的记录格式。

文件系统连接器支持对本地文件系统或分布式文件系统的读取和写入。 可以通过如下方式定义文件系统表:

```sql
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- 必选: 指定连接器类型
  'path' = 'file:///path/to/whatever',  -- 必选: 指向目录的路径
  'format' = '...',                     -- 必选: 文件系统连接器需要指定格式，请查阅 表格式 部分以获取更多细节
  'partition.default-name' = '...',     -- 可选: 动态分区模式下分区字段值是 null 或空字符串时，默认的分区名。
  'sink.shuffle-by-partition.enable' = '...',  -- 可选: 该选项开启了在 sink 阶段通过动态分区字段来 shuffle 数据，该功能可以大大减少文件系统 sink 的文件数，但可能会导致数据倾斜，默认值是 false.
  ...
)
```

{{< hint info >}}
需要确保包含以下依赖 [Flink File System specific dependencies]({{< ref "docs/deployment/filesystems/overview" >}}).
{{< /hint >}}

{{< hint info >}}
针对流的文件系统 sources 目前还在开发中。 将来，社区会不断添加对常见的流处理场景的支持, 比如对分区和目录的检测等。
{{< /hint >}}

{{< hint warning >}}
新版的文件系统连接器和旧版的文件系统连接器有很大不同：path 参数指定的是一个目录而不是一个文件，该目录下文件的格式也不是肉眼可读的。
{{< /hint >}}

## 分区文件

Flink 的文件系统连接器在对分区的支持上，使用了标准的 hive 格式。 不过，它不需要预先注册分区，而是基于目录结构自动做了分区发现。比如，以下目录结构的表， 会被自动推导为包含 `datetime` 和 `hour` 分区的分区表。

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

文件系统连接器支持分区新增插入和分区覆盖插入。 参见 [INSERT Statement]({{< ref "docs/dev/table/sql/insert" >}}). 当对分区表进行分区覆盖插入时，只有相应的分区会被覆盖，而不是整个表。

## 文件格式

文件系统连接器支持多种格式:

 - CSV: [RFC-4180](https://tools.ietf.org/html/rfc4180). 非压缩格式。
 - JSON: 注意文件系统连接器中的 JSON 不是传统的标准的 JSON 格式，而是非压缩的 [newline delimited JSON](http://jsonlines.org/).
 - Avro: [Apache Avro](http://avro.apache.org). 可以通过配置 `avro.codec` 支持压缩.
 - Parquet: [Apache Parquet](http://parquet.apache.org). 与 Hive 兼容.
 - Orc: [Apache Orc](http://orc.apache.org). 与 Hive 兼容.
 - Debezium-JSON: [debezium-json]({{< ref "docs/connectors/table/formats/debezium" >}}).
 - Canal-JSON: [canal-json]({{< ref "docs/connectors/table/formats/canal" >}}).
 - Raw: [raw]({{< ref "docs/connectors/table/formats/raw" >}}).

## 流式 Sink

文件系统连接器支持流式的写, 它基于 Flink 的 [Streaming File Sink]({{< ref "docs/connectors/datastream/streamfile_sink" >}})
将记录写入文件。按行编码的格式支持 csv 和 json。 按块编码的格式支持 parquet, orc 和 avro。

你可以直接编写 SQL，把流数据插入到非分区表。
如果是分区表，可以配置分区操作相关的参数，参见 [分区提交](#分区提交) 以查阅更多细节.

### 滚动策略

分区目录下的数据被分割到分区文件中。每个分区对应的sink的每个接受到了数据的子任务都至少会为该分区生成一个分区文件。 
根据可配置的滚动策略，当前正在写入的分区文件会被关闭，新的分区文件也会被生成。 
该策略基于大小，和指定的文件可被打开的最大 timeout 时长，来滚动分区文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.rolling-policy.file-size</h5></td>
        <td style="word-wrap: break-word;">128MB</td>
        <td>MemorySize</td>
        <td> 滚动前，分区文件最大大小.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td> 滚动前，分区文件处于打开状态的最大时长 (默认值是30分钟，以避免产生大量小文件）。 检查该选项的频率由参数 'sink.rolling-policy.check-interval' 控制。</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td style="word-wrap: break-word;">1 min</td>
        <td>Duration</td>
        <td> 基于时间的滚动策略的检查间隔。该参数控制了基于参数 'sink.rolling-policy.rollover-interval' 检查分区文件是否该被滚动的检查频率 .</td>
    </tr>
  </tbody>
</table>

**注意:** 对于 bulk 格式 (parquet, orc, avro), 滚动策略和检查点间隔控制了分区文件的大小和个数 (未完成的文件会在下个检查点完成）.

**注意:** 对于行格式 (csv, json), 如果想使得分区文件更快地在文件系统中可见，可以设置连接器参数 `sink.rolling-policy.file-size` 或 `sink.rolling-policy.rollover-interval` ，以及 flink-conf.yaml 中的 `execution.checkpointing.interval` 。 
对于其他格式 (avro, orc), 可以只设置 flink-conf.yaml 中的 `execution.checkpointing.interval` 。

### 文件合并

file sink 支持文件合并，以允许应用程序可以使用较小的检查点间隔而不产生大量文件。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>auto-compaction</h5></td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td> 在流式 sink 中是否开启自动合并功能。数据首先会被写入到临时文件，在检查点完成后，该检查点产生的临时文件会被合并。这些临时文件在合并前不可见.</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>MemorySize</td>
        <td> 合并目标文件大小，默认值是滚动文件大小.</td>
    </tr>
  </tbody>
</table>

启用该参数后，文件合并功能会根据设定的目标文件大小，合并多个小文件到大文件。
当在生产环境使用文件合并功能时，需要注意：
- 只有检查点内部的文件才会被合并，也就是说，至少会生成跟检查点个数一样多的文件。
- 合并前文件是可见的，所以文件的可见性是：检查点间隔 + 合并时长。
- 如果合并花费的时间很长，会对作业产生反压，延长检查点所需时间。

### 分区提交 
<a id="分区提交"></a>

分区数据写完毕后，经常需要通知下游应用。比如，在 Hive metastore 中新增分区或者在目录下新增 `_SUCCESS` 文件。 分区提交策略是可定制的，具体的分区提交行为是基于 `triggers` 和 `policies` 的组合.

- Trigger: 分区提交的时机，可以基于从分区中提取的时间对应的水印，或者基于处理时间。
- Policy: 分区提交策略，内置的策略包括提交 `_SUCCESS` 文件和 hive metastore， 也可以自己定制提交策略, 比如触发 hive 生成统计信息，合并小文件等。

**注意:** 分区提交只有在动态分区插入模式下才有效。

#### 分区提交触发器

通过配置分区提交的触发策略，来配置何时提交分区:

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.trigger</h5></td>
        <td style="word-wrap: break-word;">process-time</td>
        <td>String</td>
        <td>分区提交触发器类型。 
         'process-time': 基于机器时间，既不需要分区时间提取器也不需要水印生成器，一旦 ”当前系统时间“ 超过了 “分区创建系统时间” 和 'sink.partition-commit.delay' 之和，就提交分区；
         'partition-time': 基于从分区字段提取的时间，需要水印生成器，一旦 “水印” 超过了 ”从分区字段提取的时间“ 和 'sink.partition-commit.delay' 之和，就提交分区.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td>该延迟时间之前分区不会被提交。如果是按天的分区，应配置为 '1 d', 如果是按小时的分区，应配置为 '1 h'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.watermark-time-zone</h5></td>
        <td style="word-wrap: break-word;">UTC</td>
        <td>String</td>
        <td>解析 LONG 类型的水印到 TIMESTAMP 类型时所采用的时区，解析得到的水印的 TIMESTAMP 会被用来跟分区时间进行比较以判断分区是否该被提交。
            该参数只有在参数 `sink.partition-commit.trigger` 被设置为 'partition-time' 时才生效。
            如果该参数设置的不正确，比如在 TIMESTAMP_LTZ 列上定义了 source rowtime, 但没有设置该参数，则用户可能在若干个小时后才看到分区的提交。
            该参数的默认值是 'UTC', 代表水印是定义在 TIMESTAMP 列上或没有定义水印。 如果水印定义在 TIMESTAMP_LTZ 列上，则水印的时区是会话的时区。
            该参数的可选值要么是完整的时区名比如 'America/Los_Angeles'，要么是自定义的时区 id 比如 'GMT-08:00'.</td>
    </tr>        
  </tbody>
</table>

有两种类型的触发器:
- 第一种是根据分区的处理时间。 该触发器不需要分区时间提取，也不需要生成水印。通过分区创建时间和当前系统时间来触发分区提交。该触发器更通用但不是很精确。比如，数据的延迟或故障转移都会导致分区的提前提交。
- 第二种是根据从分区字段提取的时间以及水印。这需要你的作业支持生成水印，分区是根据时间来切割的，比如按小时或按天分区。

如果想让下游系统尽快感知到分区，而不管分区数据是否完整:
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='0s' (默认值)
一旦分区中有数据，分区立马就会被提交。注意：分区可能会被提交多次。

如果想让下游系统只有在分区数据完整时才感知到分区，且你的作业有水印生成的逻辑，也能从分区字段的值中提取到时间:
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1h' (根据分区类型指定，如果是按小时的分区可配置为 '1h')
该方式是最精确的提交分区的方式，该方式尽力确保提交的分区包含尽量完整的数据。

如果想让下游系统只有在数据完整时才感知到分区，但是没有水印，或者无法从分区字段的值中提取时间:
- 'sink.partition-commit.trigger'='process-time' (默认值)
- 'sink.partition-commit.delay'='1h' (根据分区类型指定，如果是按小时的分区可配置为 '1h')
该方式尽量精确地提交分区，但是数据延迟或故障转移会导致分区的提前提交。

延迟数据的处理：延迟的记录会被写入到已经提交的对应分区中，且会再次触发该分区的提交。

#### 分区时间提取器 

时间提取器定义了如何从分区字段值中提取时间.

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>partition.time-extractor.kind</h5></td>
        <td style="word-wrap: break-word;">default</td>
        <td>String</td>
        <td>从分区字段提取时间的时间提取器。支持默认值和定制。对于默认值，可以配置时间戳模式。对于定制，应指定提取器类.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>实现了接口 PartitionTimeExtractor 的提取器类.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td> 'default' 时间提取器允许用户从分区字段中提取合法的时间戳模式。默认支持从第一个字段按 'yyyy-mm-dd hh:mm:ss' 时间戳模式提取。
        如果需要从一个分区字段比如 ‘dt’ 提取时间戳，可以配置为: '$dt';
        如果需要从多个分区字段，比如 'year', 'month', 'day' 和 'hour'提取时间戳，可以配置为：'$year-$month-$day $hour:00:00';
        如果需要从两字分区字段，比如 'dt' 和 'hour' 提取时间戳，可以配置为：'$dt $hour:00:00'.</td>
    </tr>
  </tbody>
</table>

默认的提取器是基于由分区字段组合而成的时间戳模式。你也可以指定一个实现了 `PartitionTimeExtractor` 接口的自定义的提取器。

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

#### 分区提交策略 

分区提交策略指定了提交分区时的具体操作.

- 第一种是 metastore, 只有 hive 表支持该策略, 该策略下文件系统通过目录层次结构来管理分区.
- 第二种是 success 文件, 该策略下会在分区对应的目录下写入一个名为 `_SUCCESS` 的空文件. 

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.policy.kind</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>分区提交策略用来通知下游应用系统某个分区已经写完毕可以被读取了。
            metastore: 向 metastore 中增加分区，只有 hive 支持 metastore 策略，文件系统通过目录结构管理分区；
            success-file: 向目录下增加 '_success' 文件； 
            custom: 使用指定的类来创建提交策略；
            支持同时指定多个提交策略，如：'metastore,success-file'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td> 实现了 PartitionCommitPolicy 接口的分区提交策略。只有在 custom 提交策略下适用。</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td style="word-wrap: break-word;">_SUCCESS</td>
        <td>String</td>
        <td> 使用 success-file 分区提交策略时的文件名，默认值是 '_SUCCESS'.</td>
    </tr>
  </tbody>
</table>

你也可以实现自己的提交策略,如：

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

## Sink 并行度

向外部文件系统（包括 hive) 写文件时的并行度，在流处理模式和批处理模式下，都可以通过对应的 table 选项指定。默认情况下，该并行度跟上一个上游的 chained operator 的并行度一样。当配置了跟上一个上游的 chained operator 不一样的并行度时，写文件的算子和合并文件的算子（如果使用了的话）会使用指定的并行度。


<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 55%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.parallelism</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Integer</td>
        <td> 向外部文件系统写文件时的并行度。必须大于 0，否则会抛出异常.</td>
    </tr>
    
  </tbody>
</table>

**注意:** 当前，只有在上游的 changelog 模式是 **INSERT-ONLY** 时，才支持设置 sink 的并行度。否则的话，会抛出异常。

## 完整示例

如下示例演示了如何使用文件系统连接器编写流查询语句查询 kafka 中的数据并写入到文件系统中，以及通过批查询把结果数据读取出来.

```sql

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  log_ts TIMESTAMP(3),
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND -- 在 TIMESTAMP 列上定义水印
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

-- streaming sql, 插入数据到文件系统表中
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(log_ts, 'yyyy-MM-dd'),
    DATE_FORMAT(log_ts, 'HH') 
FROM kafka_table;

-- batch sql, 分区裁剪查询
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

如果水印是定义在 TIMESTAMP_LTZ 列上，且使用了 `partition-time` 来提交分区, 则参数 `sink.partition-commit.watermark-time-zone` 需要被设置为会话的时区，否则分区会在若干小时后才会被提交。  
```sql

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  ts BIGINT, -- epoch 毫秒时间
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 TIMESTAMP_LTZ 列上定义水印
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
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- 假定用户配置的时区是 'Asia/Shanghai'
  'sink.partition-commit.policy.kind'='success-file'
);

-- streaming sql, 插入数据到文件系统表中
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'),
    DATE_FORMAT(ts_ltz, 'HH') 
FROM kafka_table;

-- batch sql, 分区裁剪查询
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

{{< top >}}
