---
title: "Hive Streaming"
nav-parent_id: hive_tableapi
nav-pos: 2
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

A typical hive job is scheduled periodically to execute, so there will be a large delay.

Flink supports to write, read and join the hive table in the form of streaming.

* This will be replaced by the TOC
{:toc}

There are three types of streaming:

- Writing streaming data into Hive table.
- Reading Hive table incrementally in the form of streaming.
- Streaming table join Hive table using [Temporal Table]({{ site.baseurl }}/dev/table/streaming/temporal_tables.html#temporal-table).

## Streaming Writing

The Hive table supports streaming writes, based on [Filesystem Streaming Sink]({{ site.baseurl }}/dev/table/connectors/filesystem.html#streaming-sink).

The Hive Streaming Sink re-use Filesystem Streaming Sink to integrate Hadoop OutputFormat/RecordWriter to streaming writing.
Hadoop RecordWriters are Bulk-encoded Formats, Bulk Formats rolls files on every checkpoint.

By default, now only have renaming committer, this means S3 filesystem can not supports exactly-once,
if you want to use Hive streaming sink in S3 filesystem, You can configure the following parameter to
false to use Flink native writers (only work for parquet and orc) in `TableConfig` (note that these
parameters affect all sinks of the job):

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

The below shows how the streaming sink can be used to write a streaming query to write data from Kafka into a Hive table with partition-commit,
and runs a batch query to read that data back out. 

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
INSERT INTO TABLE hive_table SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM hive_table WHERE dt='2020-05-20' and hr='12';

{% endhighlight %}

## Streaming Reading

To improve the real-time performance of hive reading, Flink support real-time Hive table stream read:

- Partition table, monitor the generation of partition, and read the new partition incrementally.
- Non-partition table, monitor the generation of new files in the folder, and read new files incrementally.

You can even use the 10 minute level partition strategy, and use Flink's Hive streaming reading and
Hive streaming writing to greatly improve the real-time performance of Hive data warehouse to quasi
real-time minute level.

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

Note:

- Monitor strategy is to scan all directories/files in location path now. If there are too many partitions, there will be performance problems.
- Streaming reading for non-partitioned requires that each file should be put atomically into the target directory.
- Streaming reading for partitioned requires that each partition should be add atomically in the view of hive metastore. This means that new data added to an existing partition won't be consumed.
- Streaming reading not support watermark grammar in Flink DDL. So it can not be used for window operators.

The below shows how to read Hive table incrementally. 

{% highlight sql %}

SELECT * FROM hive_table /*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.consume-start-offset'='2020-05-20') */;

{% endhighlight %}

## Hive Table As Temporal Tables

You can use a Hive table as temporal table and join streaming data with it. Please follow
the [example]({{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table) to find out how to join a
temporal table.

When performing the join, the Hive table will be cached in TM memory and each record from the stream
is looked up in the Hive table to decide whether a match is found. You don't need any extra settings to use a Hive table
as temporal table. But optionally, you can configure the TTL of the Hive table cache with the following
property. After the cache expires, the Hive table will be scanned again to load the latest data.

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

**Note**:
1. Each joining subtask needs to keep its own cache of the Hive table. Please make sure the Hive table can fit into
the memory of a TM task slot.
2. You should set a relatively large value for `lookup.join.cache.ttl`. You'll probably have performance issue if
your Hive table needs to be updated and reloaded too frequently.
3. Currently we simply load the whole Hive table whenever the cache needs refreshing. There's no way to differentiate
new data from the old.
