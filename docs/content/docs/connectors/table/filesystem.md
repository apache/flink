---
title: FileSystem
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

# FileSystem SQL Connector

This connector provides access to partitioned files in filesystems
supported by the [Flink FileSystem abstraction]({{< ref "docs/deployment/filesystems/overview" >}}).

The file system connector itself is included in Flink and does not require an additional dependency.
The corresponding jar can be found in the Flink distribution inside the `/lib` directory.
A corresponding format needs to be specified for reading and writing rows from and to a file system.

The file system connector allows for reading and writing from a local or distributed filesystem. A filesystem table can be defined as:

```sql
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
```

{{< hint info >}}
Make sure to include [Flink File System specific dependencies]({{< ref "docs/deployment/filesystems/overview" >}}).
{{< /hint >}}

{{< hint warning >}}
The behaviour of file system connector is much different from `previous legacy filesystem connector`:
the path parameter is specified for a directory not for a file and you can't get a human-readable file in the path that you declare.
{{< /hint >}}

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

The file system table supports both partition inserting and overwrite inserting. See [INSERT Statement]({{< ref "docs/dev/table/sql/insert" >}}). When you insert overwrite to a partitioned table, only the corresponding partition will be overwritten, not the entire table.

## File Formats

The file system connector supports multiple formats:

 - CSV: [RFC-4180](https://tools.ietf.org/html/rfc4180). Uncompressed.
 - JSON: Note JSON format for file system connector is not a typical JSON file but uncompressed [newline delimited JSON](http://jsonlines.org/).
 - Avro: [Apache Avro](http://avro.apache.org). Support compression by configuring `avro.codec`.
 - Parquet: [Apache Parquet](http://parquet.apache.org). Compatible with Hive.
 - Orc: [Apache Orc](http://orc.apache.org). Compatible with Hive.
 - Debezium-JSON: [debezium-json]({{< ref "docs/connectors/table/formats/debezium" >}}).
 - Canal-JSON: [canal-json]({{< ref "docs/connectors/table/formats/canal" >}}).
 - Raw: [raw]({{< ref "docs/connectors/table/formats/raw" >}}).

## Source

The file system connector can be used to read single files or entire directories into a single table.

When using a directory as the source path, there is **no defined order of ingestion** for the files inside the directory.

### Directory watching

By default, the file system connector is bounded, that is it will scan the configured path once and then close itself.

You can enable continuous directory watching by configuring the option `source.monitor-interval`:

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
        <td><h5>source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Duration</td>
        <td>The interval in which the source checks for new files. The interval must be greater than 0. 
        Each file is uniquely identified by its path, and will be processed once, as soon as it's discovered. 
        The set of files already processed is kept in state during the whole lifecycle of the source, 
        so it's persisted in checkpoints and savepoints together with the source state. 
        Shorter intervals mean that files are discovered more quickly, 
        but also imply more frequent listing or directory traversal of the file system / object store. 
        If this config option is not set, the provided path will be scanned once, hence the source will be bounded.</td>
    </tr>
  </tbody>
</table>

### Available Metadata

The following connector metadata can be accessed as metadata columns in a table definition. All the metadata are read only.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 30%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>file.path</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Full path of the input file.</td>
    </tr>
    <tr>
      <td><code>file.name</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Name of the file, that is the farthest element from the root of the filepath.</td>
    </tr>
    <tr>
      <td><code>file.size</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td>Byte count of the file.</td>
    </tr>
    <tr>
      <td><code>file.modification-time</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>Modification time of the file.</td>
    </tr>
    </tbody>
</table>

The extended `CREATE TABLE` example demonstrates the syntax for exposing these metadata fields:

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

## Streaming Sink

The file system connector supports streaming writes, based on Flink's [FileSystem]({{< ref "docs/connectors/datastream/filesystem" >}}),
to write records to file. Row-encoded Formats are CSV and JSON. Bulk-encoded Formats are Parquet, ORC and Avro.

You can write SQL directly, insert the stream data into the non-partitioned table.
If it is a partitioned table, you can configure partition related operations. See [Partition Commit]({{< ref "docs/connectors/table/filesystem" >}}#partition-commit) for details.

### Rolling Policy

Data within the partition directories are split into part files. Each partition will contain at least one part file for
each subtask of the sink that has received data for that partition. The in-progress part file will be closed and additional
part file will be created according to the configurable rolling policy. The policy rolls part files based on size,
a timeout that specifies the maximum duration for which a file can be open.

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.rolling-policy.file-size</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">128 MB</td>
        <td>MemorySize</td>
        <td>The maximum part file size before rolling.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.rollover-interval</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">30 min</td>
        <td>Duration</td>
        <td>The maximum time duration a part file can stay open before rolling (by default 30 min to avoid to many small files).
        The frequency at which this is checked is controlled by the 'sink.rolling-policy.check-interval' option.</td>
    </tr>
    <tr>
        <td><h5>sink.rolling-policy.check-interval</h5></td>
        <td>optional</td>
        <td>yes</td>
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
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>auto-compaction</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td>Whether to enable automatic compaction in streaming sink or not. The data will be written to temporary files. After the checkpoint is completed, the temporary files generated by a checkpoint will be compacted. The temporary files are invisible before compaction.</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td>optional</td>
        <td>yes</td>
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
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.trigger</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">process-time</td>
        <td>String</td>
        <td>Trigger type for partition commit: 'process-time': based on the time of the machine, it neither requires partition time extraction nor watermark generation. Commit partition once the 'current system time' passes 'partition creation system time' plus 'delay'. 'partition-time': based on the time that extracted from partition values, it requires watermark generation. Commit partition once the 'watermark' passes 'time extracted from partition values' plus 'delay'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.delay</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">0 s</td>
        <td>Duration</td>
        <td>The partition will not commit until the delay time. If it is a daily partition, should be '1 d', if it is a hourly partition, should be '1 h'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.watermark-time-zone</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">UTC</td>
        <td>String</td>
        <td>The time zone to parse the long watermark value to TIMESTAMP value, the parsed watermark timestamp is used to compare with partition time to decide the partition should commit or not. This option is only take effect when `sink.partition-commit.trigger` is set to 'partition-time'. If this option is not configured correctly, e.g. source rowtime is defined on TIMESTAMP_LTZ column, but this config is not configured, then users may see the partition committed after a few hours. The default value is 'UTC', which means the watermark is defined on TIMESTAMP column or not defined. If the watermark is defined on TIMESTAMP_LTZ column, the time zone of watermark is the session time zone. The option value is either a full name such as 'America/Los_Angeles', or a custom timezone id such as 'GMT-08:00'.</td>
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
- 'sink.partition-commit.delay'='0 s' (Default value)
Once there is data in the partition, it will immediately commit. Note: the partition may be committed multiple times.

If you want to let downstream see the partition only when its data is complete, and your job has watermark generation, and you can extract the time from partition values:
- 'sink.partition-commit.trigger'='partition-time'
- 'sink.partition-commit.delay'='1 h' ('1 h' if your partition is hourly partition, depends on your partition type)
This is the most accurate way to commit partition, and it will try to ensure that the committed partitions are as data complete as possible.

If you want to let downstream see the partition only when its data is complete, but there is no watermark, or the time cannot be extracted from partition values:
- 'sink.partition-commit.trigger'='process-time' (Default value)
- 'sink.partition-commit.delay'='1 h' ('1 h' if your partition is hourly partition, depends on your partition type)
Try to commit partition accurately, but data delay or failover will lead to premature partition commit.

Late data processing: The record will be written into its partition when a record is supposed to be
written into a partition that has already been committed, and then the committing of this partition
will be triggered again.

#### Partition Time Extractor

Time extractors define extracting time from partition values.

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>partition.time-extractor.kind</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">default</td>
        <td>String</td>
        <td>Time extractor to extract time from partition values. Support default and custom. For default, can configure timestamp pattern\formatter. For custom, should configure extractor class.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.class</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The extractor class for implement PartitionTimeExtractor interface.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-pattern</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The 'default' construction way allows users to use partition fields to get a legal timestamp pattern. Default support 'yyyy-MM-dd hh:mm:ss' from first field. If timestamp should be extracted from a single partition field 'dt', can configure: '$dt'. If timestamp should be extracted from multiple partition fields, say 'year', 'month', 'day' and 'hour', can configure: '$year-$month-$day $hour:00:00'. If timestamp should be extracted from two partition fields 'dt' and 'hour', can configure: '$dt $hour:00:00'.</td>
    </tr>
    <tr>
        <td><h5>partition.time-extractor.timestamp-formatter</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">yyyy-MM-dd&nbsp;HH:mm:ss</td>
        <td>String</td>
        <td>The formatter that formats the partition timestamp string value to timestamp, the partition timestamp string value is expressed by 'partition.time-extractor.timestamp-pattern'. For example, the partition timestamp is extracted from multiple partition fields, say 'year', 'month' and 'day', you can configure 'partition.time-extractor.timestamp-pattern' to '$year$month$day', and configure `partition.time-extractor.timestamp-formatter` to 'yyyyMMdd'. By default the formatter is 'yyyy-MM-dd HH:mm:ss'.
            <br>The timestamp-formatter is compatible with Java's <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a>
 				</td>
    </tr>
  </tbody>
</table>

The default extractor is based on a timestamp pattern composed of your partition fields. You can also specify an implementation for fully custom partition extraction based on the `PartitionTimeExtractor` interface.

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

#### Partition Commit Policy

The partition commit policy defines what action is taken when partitions are committed.

- The first is metastore, only hive table supports metastore policy, file system manages partitions through directory structure.
- The second is the success file, which will write an empty file in the directory corresponding to the partition.

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.partition-commit.policy.kind</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>Policy to commit a partition is to notify the downstream application that the partition has finished writing, the partition is ready to be read. metastore: add partition to metastore. Only hive table supports metastore policy, file system manages partitions through directory structure. success-file: add '_success' file to directory. Both can be configured at the same time: 'metastore,success-file'. custom: use policy class to create a commit policy. Support to configure multiple policies: 'metastore,success-file'.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.policy.class</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>String</td>
        <td>The partition commit policy class for implement PartitionCommitPolicy interface. Only work in custom commit policy.</td>
    </tr>
    <tr>
        <td><h5>sink.partition-commit.success-file.name</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">_SUCCESS</td>
        <td>String</td>
        <td>The file name for success-file partition commit policy, default is '_SUCCESS'.</td>
    </tr>
  </tbody>
</table>

You can extend the implementation of commit policy, The custom commit policy implementation like:

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

## Sink Parallelism

The parallelism of writing files into external file system (including Hive) can be configured by the corresponding table option, which is supported both in streaming mode and in batch mode. By default, the parallelism is configured to being the same as the parallelism of its last upstream chained operator. When the parallelism which is different from the parallelism of the upstream parallelism is configured, the operator of writing files and the operator compacting files (if used) will apply the parallelism.


<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 8%">Forwarded</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 42%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>sink.parallelism</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Integer</td>
        <td>Parallelism of writing files into external file system. The value should greater than zero otherwise exception will be thrown.</td>
    </tr>

  </tbody>
</table>

**NOTE:** Currently, Configuring sink parallelism is supported if and only if the changelog mode of upstream is **INSERT-ONLY**. Otherwise, exception will be thrown.

## Full Example

The below examples show how the file system connector can be used to write a streaming query to write data from Kafka into a file system and runs a batch query to read that data back out.

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

-- streaming sql, insert into file system table
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(log_ts, 'yyyy-MM-dd'),
    DATE_FORMAT(log_ts, 'HH') 
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

If the watermark is defined on TIMESTAMP_LTZ column and used `partition-time` to commit, the `sink.partition-commit.watermark-time-zone` is required to set to the session time zone, otherwise the partition committed may happen after a few hours.  
```sql

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  ts BIGINT, -- time in epoch milliseconds
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- Define watermark on TIMESTAMP_LTZ column
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
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'
  'sink.partition-commit.policy.kind'='success-file'
);

-- streaming sql, insert into file system table
INSERT INTO fs_table 
SELECT 
    user_id, 
    order_amount, 
    DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'),
    DATE_FORMAT(ts_ltz, 'HH') 
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM fs_table WHERE dt='2020-05-20' and `hour`='12';
```

{{< top >}}
