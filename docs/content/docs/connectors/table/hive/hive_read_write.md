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
            Notes: The default interval for hive streaming reading is '1 m', the default interval for hive streaming temporal join is '60 m', this is because there's one framework limitation that every TM will visit the Hive metaStore in current hive streaming temporal join implementation which may produce pressure to metaStore, this will improve in the future.</td>
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

[SQL Hints]({{< ref "docs/dev/table/sql/hints" >}}) can be used to apply configurations to a Hive table
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

## Temporal Table Join

You can use a Hive table as a temporal table, and then a stream can correlate the Hive table by temporal join. 
Please see [temporal join]({{< ref "docs/dev/table/concepts/joins" >}}#temporal-joins) for more information about the temporal join.

Flink supports processing-time temporal join Hive Table, the processing-time temporal join always joins the latest version of temporal table.
Flink supports temporal join both partitioned table and Hive non-partitioned table, for partitioned table, Flink supports tracking the latest partition of Hive table automatically.

**NOTE**: Flink does not support event-time temporal join Hive table yet.

### Temporal Join The Latest Partition

For a partitioned table which is changing over time, we can read it out as an unbounded stream, the partition can be acted as a version of the temporal table if every partition contains complete data of a version,
the version of temporal table keeps the data of the partition.
 
Flink support tracking the latest partition(version) of temporal table automatically in processing time temporal join, the latest partition(version) is defined by 'streaming-source.partition-order' option,
This is the most common user cases that use Hive table as dimension table in a Flink stream application job.

**NOTE:** This feature is only support in Flink `STREAMING` Mode.

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

SELECT * FROM orders_table AS order 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON order.product_id = dim.product_id;

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

SELECT * FROM orders_table AS order 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON order.product_id = dim.product_id;

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

The below shows how the streaming sink can be used to write a streaming query to write data from Kafka into a Hive table with partition-commit,
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
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
) WITH (...);

-- streaming sql, insert into hive table
INSERT INTO TABLE hive_table 
SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH')
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


## Formats

Flink's Hive integration has been tested against the following file formats:

- Text
- CSV
- SequenceFile
- ORC
- Parquet
