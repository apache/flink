---
title: "Hive Read & Write"
weight: 4
type: docs
aliases:
  - /dev/table/connectors/hive/hive_read_write.html
  - /dev/table/hive/hive_streaming.html
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

# Hive Read & Write

Using the `HiveCatalog`, Apache Flink can be used for unified `BATCH` and `STREAM` processing of Apache 
Hive Tables. This means Flink can be used as a more performant alternative to Hive’s batch engine,
or to continuously read and write data into and out of Hive tables to power real-time data
warehousing applications. 

## Reading

Flink supports reading data from Hive in both `BATCH` and `STREAMING` modes. When run as a `BATCH`
application, Flink will execute its query over the state of the table at the point in time when the
query is executed. `STREAMING` reads will continuously monitor the table and incrementally fetch
new data as it is made available. Flink will read tables as bounded by default.

`STREAMING` reads support consuming both partitioned and non-partitioned tables. 
For partitioned tables, Flink will monitor the generation of new partitions, and read
them incrementally when available. For non-partitioned tables, Flink will monitor the generation
of new files in the folder and read new files incrementally.

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
        <td><h5>streaming-source.enable</h5></td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td>Enable streaming source or not. NOTES: Please make sure that each partition/file should be written atomically, otherwise the reader may get incomplete data.</td>
    </tr>
    <tr>
        <td><h5>streaming-source.partition.include</h5></td>
        <td style="word-wrap: break-word;">all</td>
        <td>String</td>
        <td>Option to set the partitions to read, the supported option are `all` and `latest`, the `all` means read all partitions; the `latest` means read latest partition in order of 'streaming-source.partition.order', the `latest` only works` when the streaming hive source table used as temporal table. By default the option is `all`.
            Flink supports temporal join the latest hive partition by enabling 'streaming-source.enable' and setting 'streaming-source.partition.include' to 'latest', at the same time, user can assign the partition compare order and data update interval by configuring following partition-related options.  
        </td>
    </tr>     
    <tr>
        <td><h5>streaming-source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">None</td>
        <td>Duration</td>
        <td>Time interval for consecutively monitoring partition/file.
            Notes: The default interval for hive streaming reading is '1 min', the default interval for hive streaming temporal join is '60 min', this is because there's one framework limitation that every TM will visit the Hive metaStore in current hive streaming temporal join implementation which may produce pressure to metaStore, this will improve in the future.</td>
    </tr>
    <tr>
        <td><h5>streaming-source.partition-order</h5></td>
        <td style="word-wrap: break-word;">partition-name</td>
        <td>String</td>
        <td>The partition order of streaming source, support create-time, partition-time and partition-name. create-time compares partition/file creation time, this is not the partition create time in Hive metaStore, but the folder/file modification time in filesystem, if the partition folder somehow gets updated, e.g. add new file into folder, it can affect how the data is consumed. partition-time compares the time extracted from partition name. partition-name compares partition name's alphabetical order. For non-partition table, this value should always be 'create-time'. By default the value is partition-name. The option is equality with deprecated option 'streaming-source.consume-order'.</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-start-offset</h5></td>
        <td style="word-wrap: break-word;">None</td>
        <td>String</td>
        <td>Start offset for streaming consuming. How to parse and compare offsets depends on your order. For create-time and partition-time, should be a timestamp string (yyyy-[m]m-[d]d [hh:mm:ss]). For partition-time, will use partition time extractor to extract time from partition.
         For partition-name, is the partition name string (e.g. pt_year=2020/pt_mon=10/pt_day=01).</td>
    </tr>
  </tbody>
</table>

[SQL Hints]({{< ref "docs/dev/table/sql/queries/hints" >}}) can be used to apply configurations to a Hive table
without changing its definition in the Hive metastore.

```sql

SELECT * 
FROM hive_table 
/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.consume-start-offset'='2020-05-20') */;

```

**Notes**

- Monitor strategy is to scan all directories/files currently in the location path. Many partitions may cause performance degradation.
- Streaming reads for non-partitioned tables requires that each file be written atomically into the target directory.
- Streaming reading for partitioned tables requires that each partition should be added atomically in the view of hive metastore. If not, new data added to an existing partition will be consumed.
- Streaming reads do not support watermark grammar in Flink DDL. These tables cannot be used for window operators.

### Reading Hive Views

Flink is able to read from Hive defined views, but some limitations apply:

1) The Hive catalog must be set as the current catalog before you can query the view. 
This can be done by either `tableEnv.useCatalog(...)` in Table API or `USE CATALOG ...` in SQL Client.

2) Hive and Flink SQL have different syntax, e.g. different reserved keywords and literals.
Make sure the view’s query is compatible with Flink grammar.

### Vectorized Optimization upon Read

Flink will automatically used vectorized reads of Hive tables when the following conditions are met:

- Format: ORC or Parquet.
- Columns without complex data type, like hive types: List, Map, Struct, Union.

This feature is enabled by default. 
It may be disabled with the following configuration. 

```bash
table.exec.hive.fallback-mapred-reader=true
```

### Source Parallelism Inference

By default, Flink will infer the optimal parallelism for its Hive readers 
based on the number of files, and number of blocks in each file.

Flink allows you to flexibly configure the policy of parallelism inference. You can configure the
following parameters in `TableConfig` (note that these parameters affect all sources of the job):

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
        <td><h5>table.exec.hive.infer-source-parallelism</h5></td>
        <td style="word-wrap: break-word;">true</td>
        <td>Boolean</td>
        <td>If is true, source parallelism is inferred according to splits number. If is false, parallelism of source are set by config.</td>
    </tr>
    <tr>
        <td><h5>table.exec.hive.infer-source-parallelism.max</h5></td>
        <td style="word-wrap: break-word;">1000</td>
        <td>Integer</td>
        <td>Sets max infer parallelism for source operator.</td>
    </tr>
  </tbody>
</table>

### Tuning Split Size While Reading Hive Table
While reading Hive table, the data files will be enumerated into splits, one of which is a portion of data consumed by the source.
Splits are granularity by which the source distributes the work and parallelize the data reading.
Users can do some performance tuning by tuning the split's size with the follow configurations.

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
        <td><h5>table.exec.hive.split-max-size</h5></td>
        <td style="word-wrap: break-word;">128mb</td>
        <td>MemorySize</td>
        <td>The maximum number of bytes (default is 128MB) to pack into a split while reading Hive table.</td>
    </tr>
    <tr>
        <td><h5>table.exec.hive.file-open-cost</h5></td>
        <td style="word-wrap: break-word;">4mb</td>
        <td>MemorySize</td>
        <td>The estimated cost (default is 4MB) to open a file. Used to enumerate Hive's files to splits.
            If the value is overestimated, Flink will tend to pack Hive's data into less splits, which will helpful when Hive's table contains many small files.
            If the value is underestimated, Flink will tend to pack Hive's data into more splits, which will help improve parallelism.
        </td>
    </tr>
  </tbody>
</table>
{{< hint warning >}}
**NOTE**:
- To tune the split's size, Flink will first get all files' size for all partitions.
  If there are too many partitions, it maybe time-consuming,
  then you can configure the job configuration `table.exec.hive.calculate-partition-size.thread-num` (3 by default) to a bigger value to enable more threads to speed up the process.
- Currently, these configurations for tuning split size only works for the Hive table stored as ORC format.
{{< /hint >}}

### Read Table Statistics

When the table statistic is not available from the Hive metastore, Flink will try to scan the table to get the statistic to generate a better execution plan. It may cost some time to get the statistic. To get it faster, you can use `table.exec.hive.read-statistics.thread-num` to configure how many threads to use to scan the table.
The default value is the number of available processors in the current system and the configured value should be bigger than 0.

### Load Partition Splits

Multi-thread is used to split hive's partitions. You can use `table.exec.hive.load-partition-splits.thread-num` to configure the thread number. The default value is 3 and the configured value should be bigger than 0.

### Read Partition With Subdirectory

In some case, you may create an external table referring another table, but the partition columns is a subset of the referred table.
For example, you have a partitioned table `fact_tz` with partition `day` and `hour`:

```sql
CREATE TABLE fact_tz(x int) PARTITIONED BY (day STRING, hour STRING);
```

And you have an external table `fact_daily` referring to table `fact_tz` with a coarse-grained partition `day`:

```sql
CREATE EXTERNAL TABLE fact_daily(x int) PARTITIONED BY (ds STRING) LOCATION '/path/to/fact_tz';
```

Then when reading the external table `fact_daily`, there will be sub-directories (`hour=1` to `hour=24`) in the partition directory of the table.

By default, you can add partition with sub-directories to the external table. Flink SQL can recursively scan all sub-directories and fetch all the data from all sub-directories.

```sql
ALTER TABLE fact_daily ADD PARTITION (ds='2022-07-07') location '/path/to/fact_tz/ds=2022-07-07';
```

You can set job configuration `table.exec.hive.read-partition-with-subdirectory.enabled` (`true` by default) to `false` to disallow Flink to read the sub-directories.
If the configuration is `false` and the directory does not contain files, rather consists of sub directories Flink blows up with the exception: `java.io.IOException: Not a file: /path/to/data/*`.

## Temporal Table Join

You can use a Hive table as a temporal table, and then a stream can correlate the Hive table by temporal join. 
Please see [temporal join]({{< ref "docs/dev/table/sql/queries/joins" >}}#temporal-joins) for more information about the temporal join.

Flink supports processing-time temporal join Hive Table, the processing-time temporal join always joins the latest version of temporal table.
Flink supports temporal join both partitioned table and Hive non-partitioned table, for partitioned table, Flink supports tracking the latest partition of Hive table automatically.

**NOTE**: Flink does not support event-time temporal join Hive table yet.

### Temporal Join The Latest Partition

For a partitioned table which is changing over time, we can read it out as an unbounded stream, the partition can be acted as a version of the temporal table if every partition contains complete data of a version,
the version of temporal table keeps the data of the partition.
 
Flink supports tracking the latest partition (version) of temporal table automatically in processing time temporal join, the latest partition (version) is defined by 'streaming-source.partition-order' option,
This is the most common user cases that use Hive table as dimension table in a Flink stream application job.

**NOTE:** This feature is only supported in Flink `STREAMING` Mode.

The following demo shows a classical business pipeline, the dimension table comes from Hive and it's updated once every day by a batch pipeline job or a Flink job, the kafka stream comes from real time online business data or log and need to join with the dimension table to enrich stream. 

```sql
-- Assume the data in hive table is updated per day, every day contains the latest and complete dimension data
SET table.sql-dialect=hive;
CREATE TABLE dimension_table (
  product_id STRING,
  product_name STRING,
  unit_price DECIMAL(10, 4),
  pv_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  update_time TIMESTAMP(3),
  update_user STRING,
  ...
) PARTITIONED BY (pt_year STRING, pt_month STRING, pt_day STRING) TBLPROPERTIES (
  -- using default partition-name order to load the latest partition every 12h (the most recommended and convenient way)
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '12 h',
  'streaming-source.partition-order' = 'partition-name',  -- option with default value, can be ignored.

  -- using partition file create-time order to load the latest partition every 12h
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.partition-order' = 'create-time',
  'streaming-source.monitor-interval' = '12 h'

  -- using partition-time order to load the latest partition every 12h
  'streaming-source.enable' = 'true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '12 h',
  'streaming-source.partition-order' = 'partition-time',
  'partition.time-extractor.kind' = 'default',
  'partition.time-extractor.timestamp-pattern' = '$pt_year-$pt_month-$pt_day 00:00:00' 
);

SET table.sql-dialect=default;
CREATE TABLE orders_table (
  order_id STRING,
  order_amount DOUBLE,
  product_id STRING,
  log_ts TIMESTAMP(3),
  proctime as PROCTIME()
) WITH (...);


-- streaming sql, kafka temporal join a hive dimension table. Flink will automatically reload data from the
-- configured latest partition in the interval of 'streaming-source.monitor-interval'.

SELECT * FROM orders_table AS o 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON o.product_id = dim.product_id;

```

### Temporal Join The Latest Table
 
For a Hive table, we can read it out as a bounded stream. In this case, the Hive table can only track its latest version at the time when we query.
The latest version of table keep all data of the Hive table. 

When performing the temporal join the latest Hive table, the Hive table will be cached in Slot memory and each record from the stream is joined against the table by key to decide whether a match is found. 
Using the latest Hive table as a temporal table does not require any additional configuration. Optionally, you can configure the TTL of the Hive table cache with the following property. After the cache expires, the Hive table will be scanned again to load the latest data.

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
        <td><h5>lookup.join.cache.ttl</h5></td>
        <td style="word-wrap: break-word;">60 min</td>
        <td>Duration</td>
        <td>The cache TTL (e.g. 10min) for the build table in lookup join. By default the TTL is 60 minutes. NOTES: The option only works when lookup bounded hive table source, if you're using streaming hive source as temporal table, please use 'streaming-source.monitor-interval' to configure the interval of data update.
       </td>
    </tr>
  </tbody>
</table>

The following demo shows load all data of hive table as a temporal table.

```sql
-- Assume the data in hive table is overwrite by batch pipeline.
SET table.sql-dialect=hive;
CREATE TABLE dimension_table (
  product_id STRING,
  product_name STRING,
  unit_price DECIMAL(10, 4),
  pv_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  update_time TIMESTAMP(3),
  update_user STRING,
  ...
) TBLPROPERTIES (
  'streaming-source.enable' = 'false',           -- option with default value, can be ignored.
  'streaming-source.partition.include' = 'all',  -- option with default value, can be ignored.
  'lookup.join.cache.ttl' = '12 h'
);

SET table.sql-dialect=default;
CREATE TABLE orders_table (
  order_id STRING,
  order_amount DOUBLE,
  product_id STRING,
  log_ts TIMESTAMP(3),
  proctime as PROCTIME()
) WITH (...);


-- streaming sql, kafka join a hive dimension table. Flink will reload all data from dimension_table after cache ttl is expired.

SELECT * FROM orders_table AS o 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON o.product_id = dim.product_id;

```
Note: 

1. Each joining subtask needs to keep its own cache of the Hive table. Please make sure the Hive table can fit into the memory of a TM task slot.
2. It is encouraged to set a relatively large value both for `streaming-source.monitor-interval`(latest partition as temporal table) or `lookup.join.cache.ttl`(all partitions as temporal table). Otherwise, Jobs are prone to performance issues as the table needs to be updated and reloaded too frequently.
3. Currently we simply load the whole Hive table whenever the cache needs refreshing. There's no way to differentiate
new data from the old.

## Writing

Flink supports writing data from Hive in both `BATCH` and `STREAMING` modes. When run as a `BATCH`
application, Flink will write to a Hive table only making those records visible when the Job finishes.
`BATCH` writes support both appending to and overwriting existing tables.

```sql
# ------ INSERT INTO will append to the table or partition, keeping the existing data intact ------ 
Flink SQL> INSERT INTO mytable SELECT 'Tom', 25;

# ------ INSERT OVERWRITE will overwrite any existing data in the table or partition ------ 
Flink SQL> INSERT OVERWRITE mytable SELECT 'Tom', 25;
```

Data can also be inserted into particular partitions. 

```sql
# ------ Insert with static partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;

# ------ Insert with dynamic partition ------ 
Flink SQL> INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';

# ------ Insert with static(my_type) and dynamic(my_date) partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
```

`STREAMING` writes continuously adding new data to Hive, committing records - making them 
visible - incrementally. Users control when/how to trigger commits with several properties. Insert
overwrite is not supported for streaming write.

The below examples show how the streaming sink can be used to write a streaming query to write data from Kafka into a Hive table with partition-commit,
and runs a batch query to read that data back out. 

Please see the [streaming sink]({{< ref "docs/connectors/table/filesystem" >}}#streaming-sink) for a full list of available configurations.

```sql

SET table.sql-dialect=hive;
CREATE TABLE hive_table (
  user_id STRING,
  order_amount DOUBLE
) PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (
  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.policy.kind'='metastore,success-file'
);

SET table.sql-dialect=default;
CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  log_ts TIMESTAMP(3),
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND -- Define watermark on TIMESTAMP column
) WITH (...);

-- streaming sql, insert into hive table
INSERT INTO TABLE hive_table 
SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH')
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM hive_table WHERE dt='2020-05-20' and hr='12';

```

If the watermark is defined on TIMESTAMP_LTZ column and used `partition-time` to commit, the `sink.partition-commit.watermark-time-zone` is required to set to the session time zone, otherwise the partition committed may happen after a few hours.  
```sql

SET table.sql-dialect=hive;
CREATE TABLE hive_table (
  user_id STRING,
  order_amount DOUBLE
) PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (
  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- Assume user configured time zone is 'Asia/Shanghai'
  'sink.partition-commit.policy.kind'='metastore,success-file'
);

SET table.sql-dialect=default;
CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  ts BIGINT, -- time in epoch milliseconds
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- Define watermark on TIMESTAMP_LTZ column
) WITH (...);

-- streaming sql, insert into hive table
INSERT INTO TABLE hive_table 
SELECT user_id, order_amount, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'), DATE_FORMAT(ts_ltz, 'HH')
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM hive_table WHERE dt='2020-05-20' and hr='12';

```

By default, for streaming writes, Flink only supports renaming committers, meaning the S3 filesystem
cannot support exactly-once streaming writes.
Exactly-once writes to S3 can be achieved by configuring the following parameter to false.
This will instruct the sink to use Flink's native writers but only works for
parquet and orc file types.
This configuration is set in the `TableConfig` and will affect all sinks of the job.

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
        <td><h5>table.exec.hive.fallback-mapred-writer</h5></td>
        <td style="word-wrap: break-word;">true</td>
        <td>Boolean</td>
        <td>If it is false, using flink native writer to write parquet and orc files; if it is true, using hadoop mapred record writer to write parquet and orc files.</td>
    </tr>
  </tbody>
</table>

### Dynamic Partition Writing
Different from static partition writing which requires users to specify the partition column value, dynamic partition writing allow
users not to specify partition column value. 
For example, for a partitioned table like:
```sql
CREATE TABLE fact_tz(x int) PARTITIONED BY (day STRING, hour STRING);
```
Users can use the follow SQL statement to write data to partitioned table `fact_tz`:
```sql
INSERT INTO TABLE fact_tz PARTITION (day, hour) select 1, '2022-8-8', '14';
```
It's a typical case for dynamic partition writing since user does not specify any partition column value in the SQL statement.

By default, if it's for dynamic partition writing, Flink will sort the data additionally by dynamic partition columns before writing into sink table.
That means the sink will receive all elements of one partition and then all elements of another partition.
Elements of different partitions will not be mixed. This is helpful for Hive sink to reduce the number of partition writers and improve writing performance by writing one partition at a time.
Otherwise, too many partition writers may cause the OutOfMemory exception.

To avoid the extra sorting, you can set job configuration `table.exec.hive.sink.sort-by-dynamic-partition.enable` (`true` by default) to `false`.
But with such a configuration, as said before, it may throw OutOfMemory exception if there are too many partitions fall into same sink node.

To relieve the problem of too many partition writers, if data is not skewed, you can add `DISTRIBUTED BY <partition_field>` in your SQL statement to shuffle the data with same partition into same node.

Also, you can manually add `SORTED BY <partition_field>` in your SQL statement to achieve the same purpose as `table.exec.hive.sink.sort-by-dynamic-partition.enable=true`.

**NOTE:** 
- The configuration `table.exec.hive.sink.sort-by-dynamic-partition.enable` only works in Flink `BATCH` mode.
- Currently, `DISTRIBUTED BY` and `SORTED BY` is only supported when using [Hive dialect]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/overview" >}})  in Flink `BATCH` mode.

### Auto Gather Statistic
By default, Flink will gather the statistic automatically and then committed to Hive metastore during writing Hive table.

But in some case, you may want to disable it as it may be time-consuming to gather the statistic.
Then, you can set the job configuration `table.exec.hive.sink.statistic-auto-gather.enable` (`true` by default) to `false`
to disable it.

If the Hive table is stored as Parquet or ORC format, `numFiles`/`totalSize`/`numRows`/`rawDataSize` can be gathered.
Otherwise, only `numFiles`/`totalSize` can be gathered.

To gather statistic `numRows`/`rawDataSize` for Parquet and ORC format, Flink will only read the file's footer to do fast gathering.
But it may still time-consuming when there are too many files, then you can configure the job configuration `table.exec.hive.sink.statistic-auto-gather.thread-num` (`3` by default) to 
use more threads to speed the gathering.

**NOTE:**
- Only `BATCH` mode supports to auto gather statistic, `STREAMING` mode doesn't support it yet.

### File Compaction

The Hive sink also supports file compactions, which allows applications to reduce the number of files generated while writing into Hive.

#### Stream Mode

In stream mode, the behavior is the same as `FileSystem` sink. Please refer to [File Compaction]({{< ref "docs/connectors/table/filesystem" >}}#file-compaction) for more details.

#### Batch Mode

When it's in batch mode and auto compaction is enabled, after finishing writing files, Flink will calculate the average size of written files for each partition. And if the average size is less than the
configured threshold, then Flink will try to compact these files to files with a target size. The following are the table's options for file compaction.

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
        <td>Whether to enable automatic compaction in Hive sink or not. The data will be written to temporary files first. The temporary files are invisible before compaction.</td>
    </tr>
    <tr>
        <td><h5>compaction.small-files.avg-size</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">16MB</td>
        <td>MemorySize</td>
        <td>The threshold for file compaction. If the average size of the files is less than this value, Flink will then compact these files. the default value is 16MB.</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>MemorySize</td>
        <td>The compaction target file size, the default value is the <a href="{{< ref "docs/connectors/table/filesystem" >}}#sink-rolling-policy-file-size">rolling file size</a>.</td>
    </tr>
    <tr>
        <td><h5>compaction.parallelism</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Integer</td>
        <td>
        The parallelism to compact files. If not set, it will use the <a href="{{< ref "docs/connectors/table/filesystem" >}}#sink-parallelism">sink parallelism</a>.
        When using <a href="{{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler">adaptive batch scheduler</a>, the parallelism of the compact operator deduced by the scheduler may be small, which will cause taking much time to finish compaction.
        In such a case, please remember to set this option to a bigger value manually.
        </td>
    </tr>
  </tbody>
</table>

## Formats

Flink's Hive integration has been tested against the following file formats:

- Text
- CSV
- SequenceFile
- ORC
- Parquet
