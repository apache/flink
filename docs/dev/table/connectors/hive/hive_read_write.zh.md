---
title: "Hive Read & Write"
nav-parent_id: hive_tableapi
nav-pos: 3
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

Using the `HiveCatalog`, Apache Flink can be used for unified `BATCH` and `STREAM` processing of Apache 
Hive Tables. This means Flink can be used as a more performant alternative to Hive’s batch engine,
or to continuously read and write data into and out of Hive tables to power real-time data
warehousing applications. 

<div class="alert alert-info">
   <b>IMPORTANT:</b> Reading and writing to and from Apache Hive is only supported by the Blink table planner.
</div>

* This will be replaced by the TOC
{:toc}

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
        <td><h5>streaming-source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">1 m</td>
        <td>Duration</td>
        <td>Time interval for consecutively monitoring partition/file.</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-order</h5></td>
        <td style="word-wrap: break-word;">create-time</td>
        <td>String</td>
        <td>The consume order of streaming source, support create-time and partition-time. create-time compare partition/file creation time, this is not the partition create time in Hive metaStore, but the folder/file modification time in filesystem; partition-time compare time represented by partition name, if the partition folder somehow gets updated, e.g. add new file into folder, it can affect how the data is consumed. For non-partition table, this value should always be 'create-time'.</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-start-offset</h5></td>
        <td style="word-wrap: break-word;">1970-00-00</td>
        <td>String</td>
        <td>Start offset for streaming consuming. How to parse and compare offsets depends on your order. For create-time and partition-time, should be a timestamp string (yyyy-[m]m-[d]d [hh:mm:ss]). For partition-time, will use partition time extractor to extract time from partition.</td>
    </tr>
  </tbody>
</table>

[SQL Hints]({% link dev/table/sql/hints.zh.md %}) can be used to apply configurations to a Hive table
without changing its definition in the Hive metastore.

{% highlight sql %}

SELECT * 
FROM hive_table 
/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.consume-start-offset'='2020-05-20') */;

{% endhighlight %}

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

### Temporal Table Join

You can use a Hive table as a temporal table and join streaming data with it. Please follow
the [example]({% link dev/table/streaming/temporal_tables.zh.md %}#temporal-table) to find
out how to join a temporal table.

When performing the join, the Hive table will be cached in Slot memory and each record from
the stream is joined against the table by key to decide whether a match is found. Using a Hive
table as a temporal table does not require any additional configuration. Optionally, you can
configure the TTL of the Hive table cache with the following property. After the cache expires,
the Hive table will be scanned again to load the latest data.

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
        <td>The cache TTL (e.g. 10min) for the build table in lookup join. By default the TTL is 60 minutes.</td>
    </tr>
  </tbody>
</table>

**Notes**:

-  Each joining subtask needs to keep its own cache of the Hive table. Please ensure the Hive table can fit into
the memory of a TM task slot.
- It is encouraged to set a relatively large value for `lookup.join.cache.ttl`. Otherwise, Jobs
are prone to performance issues as the table needs to be updated and reloaded too frequently. 
- Currently, Flink simply loads the whole Hive table whenever the cache needs to be refreshed.
There is no way to differentiate new data from old.

### Vectorized Optimization upon Read

Flink will automatically used vectorized reads of Hive tables when the following conditions are met:

- Format: ORC or Parquet.
- Columns without complex data type, like hive types: List, Map, Struct, Union.

This feature is enabled by default. 
It may be disabled with the following configuration. 

{% highlight bash %}
table.exec.hive.fallback-mapred-reader=true
{% endhighlight %}

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

## Writing

Flink supports writing data from Hive in both `BATCH` and `STREAMING` modes. When run as a `BATCH`
application, Flink will write to a Hive table only making those records visible when the Job finishes.
`BATCH` writes support both appending to and overwriting existing tables.

{% highlight sql %}
# ------ INSERT INTO will append to the table or partition, keeping the existing data intact ------ 
Flink SQL> INSERT INTO mytable SELECT 'Tom', 25;

# ------ INSERT OVERWRITE will overwrite any existing data in the table or partition ------ 
Flink SQL> INSERT OVERWRITE mytable SELECT 'Tom', 25;
{% endhighlight %}

Data can also be inserted into a particular partitions. 

{% highlight sql %}
# ------ Insert with static partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;

# ------ Insert with dynamic partition ------ 
Flink SQL> INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';

# ------ Insert with static(my_type) and dynamic(my_date) partition ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
{% endhighlight %}

`STREAMING` writes continuously adding new data to Hive, committing records - making them 
visible - incrementally. Users control when/how to trigger commits with several properties. Insert
overwrite is not supported for streaming write.

The below shows how the streaming sink can be used to write a streaming query to write data from Kafka into a Hive table with partition-commit,
and runs a batch query to read that data back out. 

Please see the [StreamingFileSink]({% link dev/connectors/streamfile_sink.zh.md %}) for 
a full list of available configurations. 

{% highlight sql %}

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

{% endhighlight %}


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

