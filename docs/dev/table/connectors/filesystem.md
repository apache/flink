---
title: "FileSystem SQL Connector"
nav-title: FileSystem
nav-parent_id: sql-connectors
nav-pos: 7
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

This connector provides access to partitioned files in filesystems
supported by the [Flink FileSystem abstraction]({% link deployment/filesystems/index.md %}).

* This will be replaced by the TOC
{:toc}

The file system connector itself is included in Flink and does not require an additional dependency.
A corresponding format needs to be specified for reading and writing rows from and to a file system.

The file system connector allows for reading and writing from a local or distributed filesystem. A filesystem table can be defined as:

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- required: specify the connector
  'path' = 'file:///path/to/whatever',  -- required: path to a directory
  'format' = '...',                     -- required: file system connector requires to specify a format,
                                        -- Please refer to Table Formats
                                        -- section for more details
  'partition.default-name' = '...',     -- optional: default partition name in case the dynamic partition
                                        -- column value is null/empty string

  -- optional: the option to enable shuffle data by dynamic partition fields in sink phase, this can greatly
  -- reduce the number of file for filesystem sink but may lead data skew, the default value is false.
  'sink.shuffle-by-partition.enable' = '...',
  ...
)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> Make sure to include [Flink File System specific dependencies]({% link deployment/filesystems/index.md %}).

<span class="label label-danger">Attention</span> File system sources for streaming is still under development. In the future, the community will add support for common streaming use cases, i.e., partition and directory monitoring.

<span class="label label-danger">Attention</span> The behaviour of file system connector is much different from `previous legacy filesystem connector`:
the path parameter is specified for a directory not for a file and you can't get a human-readable file in the path that you declare.

## Partition Files

Flink's file system partition support uses the standard hive format. However, it does not require partitions to be pre-registered with a table catalog. Partitions are discovered and inferred based on directory structure. For example, a table partitioned based on the directory below would be inferred to contain `datetime` and `hour` partitions.

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

The file system table supports both partition inserting and overwrite inserting. See [INSERT Statement]({% link dev/table/sql/insert.md %}). When you insert overwrite to a partitioned table, only the corresponding partition will be overwritten, not the entire table.

## File Formats

The file system connector supports multiple formats:

 - CSV: [RFC-4180](https://tools.ietf.org/html/rfc4180). Uncompressed.
 - JSON: Note JSON format for file system connector is not a typical JSON file but uncompressed [newline delimited JSON](http://jsonlines.org/).
 - Avro: [Apache Avro](http://avro.apache.org). Support compression by configuring `avro.codec`.
 - Parquet: [Apache Parquet](http://parquet.apache.org). Compatible with Hive.
 - Orc: [Apache Orc](http://orc.apache.org). Compatible with Hive.
 - Debezium-JSON: [debezium-json]({% link dev/table/connectors/formats/debezium.md %}).
 - Canal-JSON: [canal-json]({% link dev/table/connectors/formats/canal.md %}).
 - Raw: [raw]({% link dev/table/connectors/formats/raw.md %}).

## Streaming Sink

The file system connector supports streaming writes, based on Flink's [Streaming File Sink]({% link dev/connectors/streamfile_sink.md %})
to write records to file. Row-encoded Formats are csv and json. Bulk-encoded Formats are parquet, orc and avro.

You can write SQL directly, insert the stream data into the non-partitioned table.
If it is a partitioned table, you can configure partition related operations. See [Partition Commit](filesystem.html#partition-commit) for details.

### Rolling Policy

Data within the partition directories are split into part files. Each partition will contain at least one part file for
each subtask of the sink that has received data for that partition. The in-progress part file will be closed and additional
part file will be created according to the configurable rolling policy. The policy rolls part files based on size,
a timeout that specifies the maximum duration for which a file can be open.

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
        <td>The maximum part file size before rolling.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td>The maximum time duration a part file can stay open before rolling (by default 30 min to avoid to many small files).
        The frequency at which this is checked is controlled by the 'sink.rolling-policy.check-interval' option.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td style="word-wrap: break-word;">1 min</td>
        <td>Duration</td>
        <td>The interval for checking time based rolling policies. This controls the frequency to check whether a part file should rollover based on 'sink.rolling-policy.rollover-interval'.</td>
    </tr>
  </tbody>
</table>

**NOTE:** For bulk formats (parquet, orc, avro), the rolling policy in combination with the checkpoint interval(pending files
become finished on the next checkpoint) control the size and number of these parts.

**NOTE:** For row formats (csv, json), you can set the parameter `sink.rolling-policy.file-size` or `sink.rolling-policy.rollover-interval` in the connector properties and parameter `execution.checkpointing.interval` in flink-conf.yaml together
if you don't want to wait a long period before observe the data exists in file system. For other formats (avro, orc), you can just set parameter `execution.checkpointing.interval` in flink-conf.yaml.

### File Compaction

The file sink supports file compactions, which allows applications to have smaller checkpoint intervals without generating a large number of files.

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
        <td>Whether to enable automatic compaction in streaming sink or not. The data will be written to temporary files. After the checkpoint is completed, the temporary files generated by a checkpoint will be compacted. The temporary files are invisible before compaction.</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>MemorySize</td>
        <td>The compaction target file size, the default value is the rolling file size.</td>
    </tr>
  </tbody>
</table>

If enabled, file compaction will merge multiple small files into larger files based on the target file size.
When running file compaction in production, please be aware that:
- Only files in a single checkpoint are compacted, that is, at least the same number of files as the number of checkpoints is generated.
- The file before merging is invisible, so the visibility of the file may be: checkpoint interval + compaction time.
- If the compaction takes too long, it will backpressure the job and extend the time period of checkpoint.

### Partition Commit

After writing a partition, it is often necessary to notify downstream applications. For example, add the partition to a Hive metastore or writing a `_SUCCESS` file in the directory. The file system sink contains a partition commit feature that allows configuring custom policies. Commit actions are based on a combination of `triggers` and `policies`.

- Trigger: The timing of the commit of the partition can be determined by the watermark with the time extracted from the partition, or by processing time.
- Policy: How to commit a partition, built-in policies support for the commit of success files and metastore, you can also implement your own policies, such as triggering hive's analysis to generate statistics, or merging small files, etc.

**NOTE:** Partition Commit only works in dynamic partition inserting.

#### Partition commit trigger

To define when to commit a partition, providing partition commit trigger:

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
        <td>Trigger type for partition commit: 'process-time': based on the time of the machine, it neither requires partition time extraction nor watermark generation. Commit partition once the 'current system time' passes 'partition creation system time' plus 'delay'. 'partition-time': based on the time that extracted from partition values, it requires watermark generation. Commit partition once the 'watermark' passes 'time extracted from partition values' plus 'delay'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td>The partition will not commit until the delay time. If it is a daily partition, should be '1 d', if it is a hourly partition, should be '1 h'.</td>
    </tr>
  </tbody>
</table>

There are two types of trigger:
- The first is partition processing time. It neither requires partition time extraction nor watermark
generation. The trigger of partition commit according to partition creation time and current system time. This trigger
is more universal, but not so precise. For example, data delay or failover will lead to premature partition commit.
- The second is the trigger of partition commit according to the time that extracted from partition values and watermark.
This requires that your job has watermark generation, and the partition is divided according to time, such as
hourly partition or daily partition.

If you want to let downstream see the partition as soon as possible, no matter whether its data is complete or not:
- 'sink.partition-commit.trigger'='process-time' (Default value)
- 'sink.partition-commit.delay'='0s' (Default value)
Once there is data in the partition, it will immediately commit. Note: the partition may be committed multiple times.

If you want to let downstream see the partition only when its data is complete, and your job has watermark generation, and you can extract the time from partition values:
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1h' ('1h' if your partition is hourly partition, depends on your partition type)
This is the most accurate way to commit partition, and it will try to ensure that the committed partitions are as data complete as possible.

If you want to let downstream see the partition only when its data is complete, but there is no watermark, or the time cannot be extracted from partition values:
- 'sink.partition-commit.trigger'='process-time' (Default value)
- 'sink.partition-commit.delay'='1h' ('1h' if your partition is hourly partition, depends on your partition type)
Try to commit partition accurately, but data delay or failover will lead to premature partition commit.

Late data processing: The record will be written into its partition when a record is supposed to be
written into a partition that has already been committed, and then the committing of this partition
will be triggered again.

#### Partition Time Extractor

Time extractors define extracting time from partition values.

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
        <td>Time extractor to extract time from partition values. Support default and custom. For default, can configure timestamp pattern. For custom, should configure extractor class.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The extractor class for implement PartitionTimeExtractor interface.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The 'default' construction way allows users to use partition fields to get a legal timestamp pattern. Default support 'yyyy-mm-dd hh:mm:ss' from first field. If timestamp should be extracted from a single partition field 'dt', can configure: '$dt'. If timestamp should be extracted from multiple partition fields, say 'year', 'month', 'day' and 'hour', can configure: '$year-$month-$day $hour:00:00'. If timestamp should be extracted from two partition fields 'dt' and 'hour', can configure: '$dt $hour:00:00'.</td>
    </tr>
  </tbody>
</table>

The default extractor is based on a timestamp pattern composed of your partition fields. You can also specify an implementation for fully custom partition extraction based on the `PartitionTimeExtractor` interface.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

public class HourPartTimeExtractor implements PartitionTimeExtractor {
    @Override
    public LocalDateTime extract(List<String> keys, List<String> values) {
        String dt = values.get(0);
        String hour = values.get(1);
		return Timestamp.valueOf(dt + " " + hour + ":00:00").toLocalDateTime();
	}
}

{% endhighlight %}
</div>
</div>

#### Partition Commit Policy

The partition commit policy defines what action is taken when partitions are committed.

- The first is metastore, only hive table supports metastore policy, file system manages partitions through directory structure.
- The second is the success file, which will write an empty file in the directory corresponding to the partition.

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
        <td>Policy to commit a partition is to notify the downstream application that the partition has finished writing, the partition is ready to be read. metastore: add partition to metastore. Only hive table supports metastore policy, file system manages partitions through directory structure. success-file: add '_success' file to directory. Both can be configured at the same time: 'metastore,success-file'. custom: use policy class to create a commit policy. Support to configure multiple policies: 'metastore,success-file'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The partition commit policy class for implement PartitionCommitPolicy interface. Only work in custom commit policy.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td style="word-wrap: break-word;">_SUCCESS</td>
        <td>String</td>
        <td>The file name for success-file partition commit policy, default is '_SUCCESS'.</td>
    </tr>
  </tbody>
</table>

You can extend the implementation of commit policy, The custom commit policy implementation like:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

public class AnalysisCommitPolicy implements PartitionCommitPolicy {
    private HiveShell hiveShell;
	@Override
	public void commit(Context context) throws Exception {
	    if (hiveShell == null) {
	        hiveShell = createHiveShell(context.catalogName());
	    }
	    hiveShell.execute(String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s = '%s') location '%s'",
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

{% endhighlight %}
</div>
</div>

## Full Example

The below shows how the file system connector can be used to write a streaming query to write data from Kafka into a file system and runs a batch query to read that data back out.

{% highlight sql %}

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

-- streaming sql, insert into file system table
INSERT INTO fs_table SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';

{% endhighlight %}

{% top %}
