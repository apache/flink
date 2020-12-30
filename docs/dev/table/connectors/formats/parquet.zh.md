---
title: "Parquet 格式"
nav-title: Parquet
nav-parent_id: sql-formats
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[Apache Parquet](https://parquet.apache.org/) 格式允许读写 Parquet 数据.

依赖
------------

{% assign connector = site.data.sql-connectors['parquet'] %}
{% include sql-connector-download-table.zh.html
    connector=connector
%}

如何创建基于 Parquet 格式的表
----------------

以下为用 Filesystem 连接器和 Parquet 格式创建表的示例，

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3),
  dt STRING
) PARTITIONED BY (dt) WITH (
 'connector' = 'filesystem',
 'path' = '/tmp/user_behavior',
 'format' = 'parquet'
)
{% endhighlight %}
</div>
</div>

Format 参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 8%">是否必须</th>
        <th class="text-center" style="width: 7%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定使用的格式，此处应为"parquet"。</td>
    </tr>
    <tr>
      <td><h5>parquet.utc-timezone</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>使用 UTC 时区或本地时区在纪元时间和 LocalDateTime 之间进行转换。Hive 0.x/1.x/2.x 使用本地时区，但 Hive 3.x 使用 UTC 时区。</td>
    </tr>
    </tbody>
</table>

Parquet 格式也支持 [ParquetOutputFormat](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.10.0/org/apache/parquet/hadoop/ParquetOutputFormat.html) 的配置。
例如, 可以配置 `parquet.compression=GZIP` 来开启 gzip 压缩。

数据类型映射
----------------

目前，Parquet 格式类型映射与 Apache Hive 兼容，但与 Apache Spark 有所不同：

- Timestamp：不论精度，映射 timestamp 类型至 int96。
- Decimal：根据精度，映射 decimal 类型至固定长度字节的数组。

下表列举了 Flink 中的数据类型与 JSON 中的数据类型的映射关系。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink 数据类型</th>
        <th class="text-center">Parquet 类型</th>
        <th class="text-center">Parquet 逻辑类型</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>BINARY</td>
      <td>UTF8</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>BINARY</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>FIXED_LEN_BYTE_ARRAY</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>INT32</td>
      <td>INT_8</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>INT32</td>
      <td>INT_16</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT32</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>INT64</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>INT32</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>INT32</td>
      <td>TIME_MILLIS</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>INT96</td>
      <td></td>
    </tr>
    </tbody>
</table>

<span class="label label-danger">注意</span> 暂不支持复合数据类型（Array、Map 与 Row）。
