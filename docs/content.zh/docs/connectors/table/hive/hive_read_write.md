---
title: "Hive Read & Write"
weight: 4
type: docs
aliases:
  - /zh/dev/table/connectors/hive/hive_read_write.html
  - /zh/dev/table/hive/hive_streaming.html
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

# Hive 读 & 写

通过使用 `HiveCatalog`，Apache Flink 可以对 Apache Hive 表做统一的批和流处理。这意味着 Flink 可以成为 Hive 批处理引擎的一个性能更好的选择，或者连续读写 Hive 表中的数据以支持实时数据仓库应用。

## 读

Flink 支持以批和流两种模式从 Hive 表中读取数据。批读的时候，Flink 会基于执行查询时表的状态进行查询。流读时将持续监控表，并在表中新数据可用时进行增量获取，默认情况下，Flink 将以批模式读取数据。

流读支持消费分区表和非分区表。对于分区表，Flink 会监控新分区的生成，并且在数据可用的情况下增量获取数据。对于非分区表，Flink 将监控文件夹中新文件的生成，并增量地读取新文件。

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
        <td><h5>streaming-source.enable</h5></td>
        <td style="word-wrap: break-word;">false</td>
        <td>Boolean</td>
        <td>是否启动流读。注意：请确保每个分区/文件都应该原子地写入，否则读取不到完整的数据。</td>
    </tr>
    <tr>
        <td><h5>streaming-source.partition.include</h5></td>
        <td style="word-wrap: break-word;">all</td>
        <td>String</td>
        <td>选择读取的分区，可选项为 `all` 和 `latest`，`all` 读取所有分区；`latest` 读取按照 'streaming-source.partition.order' 排序后的最新分区，`latest` 仅在流模式的 Hive 源表作为时态表时有效。默认的选项是 `all`。在开启 'streaming-source.enable' 并设置 'streaming-source.partition.include' 为 'latest' 时，Flink 支持 temporal join 最新的 Hive 分区，同时，用户可以通过配置分区相关的选项来配置分区比较顺序和数据更新时间间隔。 
        </td>
    </tr>     
    <tr>
        <td><h5>streaming-source.monitor-interval</h5></td>
        <td style="word-wrap: break-word;">None</td>
        <td>Duration</td>
        <td>连续监控分区/文件的时间间隔。
            注意: 默认情况下，流式读 Hive 的间隔为 '1 min'，但流读 Hive 的 temporal join 的默认时间间隔是 '60 min'，这是因为当前流读 Hive 的 temporal join 实现上有一个框架限制，即每个 TM 都要访问 Hive metastore，这可能会对 metastore 产生压力，这个问题将在未来得到改善。</td>
    </tr>
    <tr>
        <td><h5>streaming-source.partition-order</h5></td>
        <td style="word-wrap: break-word;">partition-name</td>
        <td>String</td>
        <td>streaming source 分区排序，支持 create-time， partition-time 和 partition-name。 create-time 比较分区/文件创建时间， 这不是 Hive metastore 中创建分区的时间，而是文件夹/文件在文件系统的修改时间，如果分区文件夹以某种方式更新，比如添加在文件夹里新增了一个文件，它会影响到数据的使用。partition-time 从分区名称中抽取时间进行比较。partition-name 会比较分区名称的字典顺序。对于非分区的表，总是会比较 'create-time'。对于分区表默认值是 'partition-name'。该选项与已经弃用的 'streaming-source.consume-order' 的选项相同</td>
    </tr>
    <tr>
        <td><h5>streaming-source.consume-start-offset</h5></td>
        <td style="word-wrap: break-word;">None</td>
        <td>String</td>
        <td>流模式起始消费偏移量。如何解析和比较偏移量取决于你指定的顺序。对于 create-time 和 partition-time，会比较时间戳 (yyyy-[m]m-[d]d [hh:mm:ss])。对于 partition-time，将使用分区时间提取器从分区名字中提取的时间。
         对于 partition-name，是字符串类型的分区名称(比如 pt_year=2020/pt_mon=10/pt_day=01)。</td>
    </tr>
  </tbody>
</table>

[SQL Hints]({{< ref "docs/dev/table/sql/queries/hints" >}}) 可以在不改变 Hive metastore 的情况下配置 Hive table 的属性。

```sql

SELECT * 
FROM hive_table 
/*+ OPTIONS('streaming-source.enable'='true', 'streaming-source.consume-start-offset'='2020-05-20') */;

```

**注意**

- 监控策略是扫描当前位置路径中的所有目录/文件，分区太多可能导致性能下降。
- 流读非分区表时要求每个文件应原子地写入目标目录。
- 流读分区表要求每个分区应该被原子地添加进 Hive metastore 中。如果不是的话，只有添加到现有分区的新数据会被消费。 
- 流读 Hive 表不支持 Flink DDL 的 watermark 语法。这些表不能被用于窗口算子。

### 读取 Hive Views

Flink 能够读取 Hive 中已经定义的视图。但是也有一些限制：

1) Hive catalog 必须设置成当前的 catalog 才能查询视图。在 Table API 中使用 `tableEnv.useCatalog(...)`，或者在 SQL 客户端使用 `USE CATALOG ...` 来改变当前 catalog。

2) Hive 和 Flink SQL 的语法不同, 比如不同的关键字和字面值。请确保对视图的查询语法与 Flink 语法兼容。

### 读取时的向量化优化

当满足以下条件时，Flink 会自动对 Hive 表进行向量化读取:

- 格式：ORC 或者 Parquet。
- 没有复杂类型的列，比如 Hive 列类型：List、Map、Struct、Union。

该特性默认开启，可以使用以下配置禁用它。

```bash
table.exec.hive.fallback-mapred-reader=true
```

### Source 并发推断

默认情况下，Flink 会基于文件的数量，以及每个文件中块的数量推断出读取 Hive 的最佳并行度。

Flink 允许你灵活的配置并发推断策略。你可以在 `TableConfig` 中配置以下参数(注意这些参数会影响当前作业的所有 source)：

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
        <td><h5>table.exec.hive.infer-source-parallelism</h5></td>
        <td style="word-wrap: break-word;">true</td>
        <td>Boolean</td>
        <td>如果是 true，会根据 split 的数量推断 source 的并发度。如果是 false，source 的并发度由配置决定。</td>
    </tr>
    <tr>
        <td><h5>table.exec.hive.infer-source-parallelism.max</h5></td>
        <td style="word-wrap: break-word;">1000</td>
        <td>Integer</td>
        <td>设置 source operator 推断的最大并发度。</td>
    </tr>
  </tbody>
</table>

### 读 Hive 表时调整数据分片（Split） 大小
读 Hive 表时, 数据文件将会被切分为若干个分片（split）, 每一个分片是要读取的数据的一部分。
分片是 Flink 进行任务分配和数据并行读取的基本粒度。
用户可以通过下面的参数来调整每个分片的大小来做一定的读性能调优。

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
        <td>读 Hive 表时，每个分片最大可以包含的字节数 (默认是 128MB) 
    </tr>
    <tr>
        <td><h5>table.exec.hive.file-open-cost</h5></td>
        <td style="word-wrap: break-word;">4mb</td>
        <td>MemorySize</td>
        <td> 打开一个文件预估的开销，以字节为单位，默认是 4MB。
             如果这个值比较大，Flink 则将会倾向于将 Hive 表切分为更少的分片，这在 Hive 表中包含大量小文件的时候很有用。
             反之，Flink 将会倾向于将 Hive 表切分为更多的分片，这有利于提升数据读取的并行度。
        </td>
    </tr>
  </tbody>
</table>

{{< hint warning >}}
**注意：**
- 为了调整数据分片的大小， Flink 首先将计算得到所有分区下的所有文件的大小。
  但是这在分区数量很多的情况下会比较耗时，你可以配置作业参数 `table.exec.hive.calculate-partition-size.thread-num`（默认为3）为一个更大的值使用更多的线程来进行加速。
- 目前上述参数仅适用于 ORC 格式的 Hive 表。
{{< /hint >}}

### 读取表统计信息

当hive metastore 中没有表的统计信息时，Flink 会尝试扫描表来获取统计信息从而生成合适的执行计划。此过程可以会比较耗时，你可以使用`table.exec.hive.read-statistics.thread-num`去配置使用多少个线程去扫描表，默认值是当前系统可用处理器数，配置的值应该大于0。

### 加载分区切片

Flink 使用多个线程并发将 Hive 分区切分成多个 split 进行读取。你可以使用 `table.exec.hive.load-partition-splits.thread-num` 去配置线程数。默认值是3，你配置的值应该大于0。

### 读取带有子目录的分区

在某些情况下，你或许会创建一个引用其他表的外部表，但是该表的分区列是另一张表分区字段的子集。
比如，你创建了一个分区表 `fact_tz`，分区字段是 `day` 和 `hour`：

```sql
CREATE TABLE fact_tz(x int) PARTITIONED BY (day STRING, hour STRING);
```

然后你基于 `fact_tz` 表创建了一个外部表 `fact_daily`，并使用了一个粗粒度的分区字段 `day`：

```sql
CREATE EXTERNAL TABLE fact_daily(x int) PARTITIONED BY (ds STRING) LOCATION '/path/to/fact_tz';
```

当读取外部表 `fact_daily` 时，该表的分区目录下存在子目录(`hour=1` 到 `hour=24`)。

默认情况下，可以将带有子目录的分区添加到外部表中。Flink SQL 会递归扫描所有的子目录，并获取所有子目录中数据。

```sql
ALTER TABLE fact_daily ADD PARTITION (ds='2022-07-07') location '/path/to/fact_tz/ds=2022-07-07';
```

你可以设置作业属性 `table.exec.hive.read-partition-with-subdirectory.enabled` (默认为 `true`) 为 `false` 以禁止 Flink 读取子目录。
如果你设置成 `false` 并且分区目录下不包含任何子目录，Flink 会抛出 `java.io.IOException: Not a file: /path/to/data/*` 异常。

## 时态表 Join

你可以使用 Hive 表作为时态表，然后一个数据流就可以使用 temporal join 关联 Hive 表。
请参照 [temporal join]({{< ref "docs/dev/table/sql/queries/joins" >}}#temporal-joins) 获取更多关于 temporal join 的信息。

Flink 支持 processing-time temporal join Hive 表，processing-time temporal join 总是关联最新版本的时态表。
Flink 支持 temporal join Hive 的分区表和非分区表，对于分区表，Flink 支持自动跟踪 Hive 表的最新分区。

**注意**: Flink 还不支持 event-time temporal join Hive 表。

### Temporal Join 最新的分区

对于随时变化的分区表，我们可以把它看作是一个无界流进行读取，如果每个分区包含完整数据，则分区可以作为时态表的一个版本，时态表的版本保存分区的数据。
 
Flink 支持在使用 processing time temporal join 时自动追踪最新的分区（版本），通过 `streaming-source.partition-order` 定义最新的分区（版本）。
用户最常使用的案例就是在 Flink 流作业中使用 Hive 表作为维度表。

**注意:** 该特性仅支持 Flink 流模式。

下面的案例演示了经典的业务 pipeline，使用 Hive 中的表作为维度表，它们由每天一次的批任务或者 Flink 任务来更新。
Kafka 数据流来自实时在线业务数据或者日志，该流需要关联维度表以丰富数据流。

```sql
-- 假设 Hive 表中的数据每天更新, 每天包含最新和完整的维度数据
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
  'streaming-source.partition-order' = 'partition-name',  -- 有默认的配置项，可以不填。

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


-- streaming sql, kafka temporal join Hive 维度表. Flink 将在 'streaming-source.monitor-interval' 的间隔内自动加载最新分区的数据。

SELECT * FROM orders_table AS o 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON o.product_id = dim.product_id;

```

### Temporal Join 最新的表
 
对于 Hive 表，我们可以把它看作是一个无界流进行读取，在这个案例中，当我们查询时只能去追踪最新的版本。
最新版本的表保留了 Hive 表的所有数据。

当 temporal join 最新的 Hive 表，Hive 表会缓存到 Slot 内存中，并且数据流中的每条记录通过 key 去关联表找到对应的匹配项。
使用最新的 Hive 表作为时态表不需要额外的配置。作为可选项，您可以使用以下配置项配置 Hive 表缓存的 TTL。当缓存失效，Hive 表会重新扫描并加载最新的数据。

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
        <td><h5>lookup.join.cache.ttl</h5></td>
        <td style="word-wrap: break-word;">60 min</td>
        <td>Duration</td>
        <td>在 lookup join 时构建表缓存的 TTL (例如 10min)。默认的 TTL 是60分钟。注意: 该选项仅在 lookup 表为有界的 Hive 表时有效，如果你使用流式的 Hive 表作为时态表，请使用 'streaming-source.monitor-interval' 去配置数据更新的间隔。
       </td>
    </tr>
  </tbody>
</table>

下面的案例演示加载 Hive 表的所有数据作为时态表。

```sql
-- 假设 Hive 表中的数据被批处理 pipeline 覆盖。
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
  'streaming-source.enable' = 'false',           -- 有默认的配置项，可以不填。
  'streaming-source.partition.include' = 'all',  -- 有默认的配置项，可以不填。
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


-- streaming sql, kafka join Hive 维度表. 当缓存失效时 Flink 会加载维度表的所有数据。

SELECT * FROM orders_table AS o 
JOIN dimension_table FOR SYSTEM_TIME AS OF o.proctime AS dim
ON o.product_id = dim.product_id;

```
注意: 

1. 每个参与 join 的 subtask 需要在他们的缓存中保留 Hive 表。请确保 Hive 表可以放到 TM task slot 中。
2. 建议把这两个选项配置成较大的值 `streaming-source.monitor-interval`(最新的分区作为时态表) 和 `lookup.join.cache.ttl`(所有的分区作为时态表)。否则，任务会频繁更新和加载表，容易出现性能问题。
3. 目前，缓存刷新的时候会重新加载整个 Hive 表，所以没有办法区分数据是新数据还是旧数据。

## 写

Flink 支持批和流两种模式往 Hive 中写入数据，当作为批程序，只有当作业完成时，Flink 写入 Hive 表的数据才能被看见。批模式写入支持追加到现有的表或者覆盖现有的表。

```sql
# ------ INSERT INTO 将追加到表或者分区，保证数据的完整性 ------ 
Flink SQL> INSERT INTO mytable SELECT 'Tom', 25;

# ------ INSERT OVERWRITE 将覆盖表或者分区中所有已经存在的数据 ------ 
Flink SQL> INSERT OVERWRITE mytable SELECT 'Tom', 25;
```

还可以将数据插入到特定的分区中。

```sql
# ------ 插入静态分区 ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1', my_date='2019-08-08') SELECT 'Tom', 25;

# ------ 插入动态分区 ------ 
Flink SQL> INSERT OVERWRITE myparttable SELECT 'Tom', 25, 'type_1', '2019-08-08';

# ------ 插入静态(my_type)和动态(my_date)分区 ------ 
Flink SQL> INSERT OVERWRITE myparttable PARTITION (my_type='type_1') SELECT 'Tom', 25, '2019-08-08';
```

流写会不断的往 Hive 中添加新数据，提交记录使它们可见。用户可以通过几个属性控制如何触发提交。流写不支持 `Insert overwrite` 。

下面的案例演示如何流式地从 Kafka 写入 Hive 表并执行分区提交，然后运行一个批处理查询将数据读出来。

请参阅 [streaming sink]({{< ref "docs/connectors/table/filesystem" >}}#streaming-sink) 获取可用配置的完整列表。

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
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND -- 在 TIMESTAMP 列声明 watermark。
) WITH (...);

-- streaming sql, insert into hive table
INSERT INTO TABLE hive_table 
SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH')
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM hive_table WHERE dt='2020-05-20' and hr='12';

```

如果在 TIMESTAMP_LTZ 列定义了 watermark 并且使用 `partition-time` 提交，需要对 `sink.partition-commit.watermark-time-zone` 设置会话时区，否则分区提交会发生在几个小时后。
```sql

SET table.sql-dialect=hive;
CREATE TABLE hive_table (
  user_id STRING,
  order_amount DOUBLE
) PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (
  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 h',
  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai', -- 假设用户配置的时区是 'Asia/Shanghai'。
  'sink.partition-commit.policy.kind'='metastore,success-file'
);

SET table.sql-dialect=default;
CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  ts BIGINT, -- time in epoch milliseconds
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 TIMESTAMP_LTZ 列声明 watermark。
) WITH (...);

-- streaming sql, insert into hive table
INSERT INTO TABLE hive_table 
SELECT user_id, order_amount, DATE_FORMAT(ts_ltz, 'yyyy-MM-dd'), DATE_FORMAT(ts_ltz, 'HH')
FROM kafka_table;

-- batch sql, select with partition pruning
SELECT * FROM hive_table WHERE dt='2020-05-20' and hr='12';

```

默认情况下，对于流，Flink 仅支持重命名 committers，对于 S3 文件系统不支持流写的 exactly-once 语义。
通过将以下参数设置为 false，可以实现 exactly-once 写入 S3。
这会调用 Flink 原生的 writer ，但是仅针对 parquet 和 orc 文件类型有效。
这个配置项可以在 `TableConfig` 中配置，该配置项对作业的所有 sink 都生效。

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
        <td><h5>table.exec.hive.fallback-mapred-writer</h5></td>
        <td style="word-wrap: break-word;">true</td>
        <td>Boolean</td>
        <td>如果是 false，使用 Flink 原生的 writer 去写 parquet 和 orc 文件；如果是 true，使用 hadoop mapred record writer 去写 parquet 和 orc 文件。</td>
    </tr>
  </tbody>
</table>

### 动态分区的写入
不同于静态分区的写入总是需要用户指定分区列的值，动态分区允许用户在写入数据的时候不指定分区列的值。
比如，有这样一个分区表：
```sql
CREATE TABLE fact_tz(x int) PARTITIONED BY (day STRING, hour STRING);
```
用户可以使用如下的 SQL 语句向该分区表写入数据：
```sql
INSERT INTO TABLE fact_tz PARTITION (day, hour) select 1, '2022-8-8', '14';
```
在该 SQL 语句中，用户没有指定分区列的值，这就是一个典型的动态分区写入的例子。

默认情况下, 如果是动态分区的写入, 在实际写入目标表之前，Flink 将额外对数据按照动态分区列进行排序。
这就意味着 sink 节点收到的数据都是按分区排序的，即首先收到一个分区的数据，然后收到另一个分区的数据，不同分区的数据不会混在一起。
这样 Hive sink 节点就可以一次只维护一个分区的 writer，否则，Hive sink 需要维护收到的数据对应的所有分区的 writer，如果分区的 writer 过多的话，则可能会导致内存溢出（OutOfMemory）异常。

为了避免额外的排序，你可以将作业的配置项 `table.exec.hive.sink.sort-by-dynamic-partition.enable`（默认是 `true`）设置为 `false`。
但是这种配置下，如之前所述，如果单个 sink 节点收到的动态分区数过多的话，则有可能会出现内存溢出的异常。 

如果数据倾斜不严重的话，你可以在 SQL 语句中添加 `DISTRIBUTED BY <partition_field>` 将相同分区的数据分布到到相同的 sink 节点上来缓解单个 sink 节点的分区 writer 过多的问题。

此外，你也可以在 SQL 语句中添加 `DISTRIBUTED BY <partition_field>` 来达到将 `table.exec.hive.sink.sort-by-dynamic-partition.enable` 设置为 `false` 的效果。

**注意：**
- 该配置项 `table.exec.hive.sink.sort-by-dynamic-partition.enable` 只在批模式下生效。
- 目前，只有在 Flink 批模式下使用了 [Hive 方言]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/overview" >}})，才可以使用 `DISTRIBUTED BY` 和 `SORTED BY`。

### 自动收集统计信息
在使用 Flink 写入 Hive 表的时候，Flink 将默认自动收集写入数据的统计信息然后将其提交至 Hive metastore 中。
但在某些情况下，你可能不想自动收集统计信息，因为收集这些统计信息可能会花费一定的时间。 
为了避免 Flink 自动收集统计信息，你可以设置作业参数 `table.exec.hive.sink.statistic-auto-gather.enable` (默认是 `true`) 为 `false`。

如果写入的 Hive 表是以 Parquet 或者 ORC 格式存储的时候，`numFiles`/`totalSize`/`numRows`/`rawDataSize` 这些统计信息可以被 Flink 收集到。
否则, 只有 `numFiles`/`totalSize` 可以被收集到。

对于 Parquet 或者 ORC 格式的表，为了快速收集到统计信息 `numRows`/`rawDataSize`， Flink 只会读取文件的 footer。但是在文件数量很多的情况下，这可能也会比较耗时，你可以通过
设置作业参数 `table.exec.hive.sink.statistic-auto-gather.thread-num`（默认是 `3`）为一个更大的值来加快统计信息的收集。

**注意：**
- 只有批模式才支持自动收集统计信息，流模式目前还不支持自动收集统计信息。

### 文件合并

在使用 Flink 写 Hive 表的时候，Flink 也支持自动对小文件进行合并以减少小文件的数量。

#### Stream Mode

流模式下，合并小文件的行为与写 `文件系统` 一样，更多细节请参考 [文件合并]({{< ref "docs/connectors/table/filesystem" >}}#file-compaction)。

#### Batch Mode

在批模式，并且自动合并小文件已经开启的情况下，在结束写 Hive 表后，Flink 会计算每个分区下文件的平均大小，如果文件的平均大小小于用户指定的一个阈值，Flink 则会将这些文件合并成指定大小的文件。下面是文件合并涉及到的参数：

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
        <td>是否开启自动合并，数据将会首先被写入临时文件，合并结束后，文件才可见。</td>
    </tr>
    <tr>
        <td><h5>compaction.small-files.avg-size</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">16MB</td>
        <td>MemorySize</td>
        <td>合并文件的阈值，当文件的平均大小小于该阈值，Flink 将对这些文件进行合并。默认值是 16MB。</td>
    </tr>
    <tr>
        <td><h5>compaction.file-size</h5></td>
        <td>optional</td>
        <td>yes</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>MemorySize</td>
        <td>合并文件的目标大小，即期望将文件合并成多大的文件，默认值是 <a href="{{< ref "docs/connectors/table/filesystem" >}}#sink-rolling-policy-file-size">rolling file</a>的大小。</td>
    </tr>
    <tr>
        <td><h5>compaction.parallelism</h5></td>
        <td>optional</td>
        <td>no</td>
        <td style="word-wrap: break-word;">(none)</td>
        <td>Integer</td>
        <td>
        合并文件的并行度。 如果没有设置，它将使用 <a href="{{< ref "docs/connectors/table/filesystem" >}}#sink-parallelism">sink parallelism</a> 作为并行度。
        当使用了 <a href="{{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler">adaptive batch scheduler</a>, 该并行度可能会很小，导致花费很多时间进行文件合并。
        在这种情况下, 你可以手动设置该值为一个更大的值。
        </td>
    </tr>
  </tbody>
</table>

## 格式

Flink 对 Hive 的集成已经在如下的文件格式进行了测试：

- Text
- CSV
- SequenceFile
- ORC
- Parquet
