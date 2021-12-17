---
title: Raw
weight: 10
type: docs
aliases:
  - /zh/dev/table/connectors/formats/raw.html
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

# Raw Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

Raw format 允许读写原始（基于字节）值作为单个列。

注意: 这种格式将 `null` 值编码成 `byte[]` 类型的 `null`。这样在 `upsert-kafka` 中使用时可能会有限制，因为 `upsert-kafka` 将 `null` 值视为 墓碑消息（在键上删除）。因此，如果该字段可能具有 `null` 值，我们建议避免使用 `upsert-kafka` 连接器和 `raw` format 作为 `value.format`。

Raw format 连接器是内置的。

示例
----------------

例如，你可能在 Kafka 中具有原始日志数据，并希望使用 Flink SQL 读取和分析此类数据。

```
47.29.201.179 - - [28/Feb/2019:13:17:10 +0000] "GET /?p=1 HTTP/2.0" 200 5316 "https://domain.com/?p=1" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36" "2.75"
```

下面的代码创建了一张表，使用 `raw` format 以 UTF-8 编码的形式从中读取（也可以写入）底层的 Kafka topic 作为匿名字符串值：

```sql
CREATE TABLE nginx_log (
  log STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'nginx_log',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'format' = 'raw'
)
```

然后，你可以将原始数据读取为纯字符串，之后使用用户自定义函数将其分为多个字段进行进一步分析。例如 示例中的 `my_split`。

```sql
SELECT t.hostname, t.datetime, t.url, t.browser, ...
FROM(
  SELECT my_split(log) as t FROM nginx_log
);
```

相对应的，你也可以将一个 STRING 类型的列以 UTF-8 编码的匿名字符串值写入 Kafka topic。

Format 参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 8%">是否必选</th>
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
      <td>指定要使用的格式, 这里应该是 'raw'。</td>
    </tr>
    <tr>
      <td><h5>raw.charset</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">UTF-8</td>
      <td>String</td>
      <td>指定字符集来编码文本字符串。</td>
    </tr>
    <tr>
      <td><h5>raw.endianness</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">big-endian</td>
      <td>String</td>
      <td>指定字节序来编码数字值的字节。有效值为'big-endian'和'little-endian'。
      更多细节可查阅 <a href="https://zh.wikipedia.org/wiki/字节序">字节序</a>。</td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

下表详细说明了这种格式支持的 SQL 类型，包括用于编码和解码的序列化类和反序列化类的详细信息。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL 类型</th>
        <th class="text-left">值</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td>UTF-8（默认）编码的文本字符串。<br>
       编码字符集可以通过 'raw.charset' 进行配置。</td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY / BYTES</code></td>
      <td>字节序列本身。</td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td>表示布尔值的单个字节，0表示 false, 1 表示 true。</td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td>有符号数字值的单个字节。</td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td>采用big-endian（默认）编码的两个字节。<br>
       字节序可以通过 'raw.endianness' 配置。</td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td>采用 big-endian （默认）编码的四个字节。<br>
       字节序可以通过 'raw.endianness' 配置。</td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td>采用 big-endian （默认）编码的八个字节。<br>
       字节序可以通过 'raw.endianness' 配置。</td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>采用 IEEE 754 格式和 big-endian （默认）编码的四个字节。<br>
       字节序可以通过 'raw.endianness' 配置。</td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td>采用 IEEE 754 格式和 big-endian （默认）编码的八个字节。<br>
       字节序可以通过 'raw.endianness' 配置。</td>
    </tr>
    <tr>
      <td><code>RAW</code></td>
      <td>通过 RAW 类型的底层 TypeSerializer 序列化的字节序列。</td>
    </tr>
    </tbody>
</table>

