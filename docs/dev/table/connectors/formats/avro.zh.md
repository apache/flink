---
title: "Avro Format"
nav-title: Avro
nav-parent_id: sql-formats
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[Apache Avro](https://avro.apache.org/) format 允许基于 Avro schema 读取和写入 Avro 数据。目前，Avro schema 从 table schema 推导而来。

依赖
------------

{% assign connector = site.data.sql-connectors['avro'] %} 
{% include sql-connector-download-table.zh.html 
    connector=connector
%}

如何使用 Avro format 创建表
----------------

这是使用 Kafka 连接器和 Avro format 创建表的示例。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'avro'
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
        <th class="text-center" style="width: 10%">是否必选</th>
        <th class="text-center" style="width: 10%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必要</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定使用什么 format，这里应该是 <code>'avro'</code>。</td>
    </tr>
    <tr>
      <td><h5>avro.codec</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>仅用于 <a href="{% link dev/table/connectors/filesystem.zh.md %}">filesystem</a>，avro 压缩编解码器。默认不压缩。目前支持：deflate、snappy、bzip2、xz。</td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前，Avro schema 通常是从 table schema 中推导而来。尚不支持显式定义 Avro schema。因此，下表列出了从 Flink 类型到 Avro 类型的类型映射。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL 类型</th>
        <th class="text-left">Avro 类型</th>
        <th class="text-left">Avro 逻辑类型</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>string</td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>fixed</code></td>
      <td><code>decimal</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>long</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int</code></td>
      <td><code>date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>int</code></td>
      <td><code>time-millis</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>long</code></td>
      <td><code>timestamp-millis</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MAP</code><br>
      (key 必须是 string/char/varchar 类型)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MULTISET</code><br>
      (元素必须是 string/char/varchar 类型)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>record</code></td>
      <td></td>
    </tr>
    </tbody>
</table>

除了上面列出的类型，Flink 支持读取/写入 nullable 的类型。Flink 将 nullable 的类型映射到 Avro `union(something, null)`，其中 `something` 是从 Flink 类型转换的 Avro 类型。

您可以参考 [Avro 规范](https://avro.apache.org/docs/current/spec.html) 获取更多有关 Avro 类型的信息。
