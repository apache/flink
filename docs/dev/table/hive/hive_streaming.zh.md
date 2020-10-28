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

典型的 Hive 作业是周期性地调度执行，所以会有很大的延迟。

Flink 支持流式的写入、读取和关联 Hive 表。

* This will be replaced by the TOC
{:toc}

有三种类型的流：  

- 写入流式数据到 Hive 表。  
- 以流的形式增量读取 Hive 表。
- 以时态表 [Temporal Table]({{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table) 的方式实现流表关联 Hive 表。

## 流式写

Hive 支持基于 [Filesystem Streaming Sink]({{ site.baseurl }}/zh/dev/table/connectors/filesystem.html#streaming-sink) 的流式写入。  

Hive Streaming Sink 复用 Filesystem Streaming Sink 将 Hadoop OutputFormat/RecordWriter 集成到流式写入。  
Hadoop RecordWriters 是批量编码格式，批量格式在每个检查点滚动文件。

默认情况下，现在只有重命名提交者(committer)，这意味着 S3 文件系统不支持精确一次语义，如果您想在 S3 文件系统中使用 Hive streaming sink，可以将下面的参数配置为 false，以便在 ‘TableConfig’ 中使用 Flink native sink(仅适用于 parquet 和 orc 格式)(请注意，这些参数会影响作业的所有 sink)：
<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>table.exec.hive.fallback-mapred-writer</h5></td>
        <td style="word-wrap: break-word;">true</td>
        <td>Boolean</td>
        <td>如果为 false, 使用 flink native writer 写入 parquet 和 orc 文件;如果为 true,使用 hadoop mapred record writer 写入 parquet and orc 文件.</td>
    </tr>
  </tbody>
</table>

下面展示了如何使用 streaming sink 编写一个流查询，将 Kafka 中的数据写入带有分区提交的 Hive 表，并运行一个批查询读取返回的数据。

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

## 流式读

为了提高 Hive 读取的实时性能，Flink 支持实时的 Hive 表流，如下所示:  

- 分区表，监控分区的生成，并以增量方式读取新分区。  
- 非分区表，监控目录中新文件的生成，并以增量方式读取新文件。

你甚至可以使用10分钟级别的分区策略，并使用 Flink 的 Hive 流式读和 Hive 流式写，从而大大提高 Hive 数据仓库的实时性能，达到准实时分钟级别。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>streaming-source.enable</h5></td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td>是否启用流式 source，注意：请确保每个分区/文件应该以原子方式写入，否则读取器可能会得到不完整的数据。</td>
    </tr>
    <tr>
        <td><h5>streaming-source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">1 m</td>
        <td>Duration</td>
        <td>连续监控分区/文件的时间间隔</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-order</h5></td>
        <td style="word-wrap: break-word;">create-time</td>
        <td>String</td>
        <td>流式 source 消费顺序，支持 create-time（创建时间）和 partition-time（分区时间）。create-time 表示分区/文件的创建时间，这不是 Hive metaStore 中的分区创建时间，而是文件系统中的文件夹/文件修改时间; partition-time 表示分区名称代表的时间，如果分区文件夹以某种方式得到更新，例如添加新文件到文件夹中，它会影响数据的消费方式。对于非分区表，此值应始终为“ create-time”。</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-start-offset</h5></td>
        <td style="word-wrap: break-word;">1970-00-00</td>
        <td>String</td>
        <td>流式消费的起始偏移量，如何解析和比较偏移量取决于你的指令。对于 create-time 和 partition-time，应该是一个时间戳字符串(yyyy-[m]m-[d]d [hh:mm:ss])。对于 partition-time，将使用分区时间提取器从分区提取时间。</td>
    </tr>
  </tbody>
</table>
  
注意:

- 监控策略是扫描位置路径中的所有目录/文件。如果分区太多，就会出现性能问题。
- 对于非分区的流式读取需要将每个文件原子性地放入目标目录。
- 对已分区的流式读取要求每个分区都应该原子性地添加到 Hive metastore 视图中。这意味着添加到现有分区的新数据将不会被使用。
- 流式阅读不支持 Flink DDL 中的水印语法，因此不能用于窗口操作。

下面展示了如何以增量方式读取 Hive 表。

{% highlight sql %}

SELECT * FROM hive_table /*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.consume-start-offset'='2020-05-20') */;

{% endhighlight %}

# Hive表作为 Temporal Table

你可以使用一个 Hive 表作为 temporal table，并用来关联流数据。请遵循[示例]({{ site.baseurl }}/zh/dev/table/streaming/temporal_tables.html#temporal-table)来查明如何加入 temporal 表。

在执行关联时，Hive 表将缓存在 TM 内存中，来自流的每个记录都将在 Hive 表中查找，以决定是否找到匹配项。您不需要任何额外的设置就可以将 Hive 表用作 temporal table。但是也可以使用以下属性配置 Hive 表缓存的 TTL。缓存过期后，将再次扫描 Hive 表以加载最新数据。

<table class="table table-bordered">
  <thead>
    <tr>
        <th class="text-left" style="width: 20%">Key</th>
        <th class="text-left" style="width: 15%">默认值</th>
        <th class="text-left" style="width: 10%">类型</th>
        <th class="text-left" style="width: 55%">描述</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><h5>lookup.join.cache.ttl</h5></td>
        <td style="word-wrap: break-word;">60 min</td>
        <td>Duration</td>
        <td>查找关联中构建的表的缓存 TTL(例如10分钟)，默认情况下，TTL 为60分钟。</td>
    </tr>
  </tbody>
</table>


**注意**:
1. 每个连接子任务都需要保留自己的 Hive 表缓存，请确保 Hive 表可以放入 TM 任务槽的内存中。
2. 你应当为 lookup.join.cache.ttl 设置一个相对较大的值，如果你的 Hive 表需要频繁地更新和重新加载，那么可能会出现性能问题。
3. 目前，只要缓存需要刷新，我们就加载整个 Hive 表，没有办法区分新数据和旧数据。
