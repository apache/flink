---
title: CSV
weight: 2
type: docs
aliases:
  - /zh/dev/table/connectors/formats/csv.html
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

# CSV Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

[CSV](https://zh.wikipedia.org/wiki/%E9%80%97%E5%8F%B7%E5%88%86%E9%9A%94%E5%80%BC) Format 允许我们基于 CSV schema 进行解析和生成 CSV 数据。 目前 CSV schema 是基于 table schema 推断而来的。

依赖
------------

{{< sql_download_table "csv" >}}

如何创建使用 CSV 格式的表
----------------


以下是一个使用 Kafka 连接器和 CSV 格式创建表的示例。

```sql
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
 'format' = 'csv',
 'csv.ignore-parse-errors' = 'true',
 'csv.allow-comments' = 'true'
)
```

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
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的格式，这里应该是 <code>'csv'</code>。</td>
    </tr>
    <tr>
      <td><h5>csv.field-delimiter</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>,</code></td>
      <td>String</td>
      <td>字段分隔符 (默认<code>','</code>)，必须为单字符。你可以使用反斜杠字符指定一些特殊字符，例如 <code>'\t'</code> 代表制表符。
      你也可以通过 unicode 编码在纯 SQL 文本中指定一些特殊字符，例如 <code>'csv.field-delimiter' = U&'\0001'</code> 代表 <code>0x01</code> 字符。
      </td>
    </tr>
    <tr>
      <td><h5>csv.disable-quote-character</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否禁止对引用的值使用引号 (默认是 false)。 如果禁止，选项 <code>'csv.quote-character'</code> 不能设置。</td>
    </tr>
    <tr>
      <td><h5>csv.quote-character</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>"</code></td>
      <td>String</td>
      <td>用于围住字段值的引号字符 (默认<code>"</code>)。</td>
    </tr>
    <tr>
      <td><h5>csv.allow-comments</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否允许忽略注释行（默认不允许），注释行以 <code>'#'</code> 作为起始字符。
      如果允许注释行，请确保 <code>csv.ignore-parse-errors</code> 也开启了从而允许空行。 
      </td>
    </tr>
    <tr>
      <td><h5>csv.ignore-parse-errors</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
    <td>当解析异常时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。如果忽略字段的解析异常，则会将该字段值设置为<code>null</code>。</td>
    </tr>
    <tr>
      <td><h5>csv.array-element-delimiter</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>;</code></td>
      <td>String</td>
      <td>分隔数组和行元素的字符串(默认<code>';'</code>)。</td>
    </tr>
    <tr>
      <td><h5>csv.escape-character</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>转义字符(默认关闭)。</td>
    </tr>
    <tr>
      <td><h5>csv.null-literal</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定识别成 null 值的字符串（默认禁用）。在输入端会将该字符串转为 null 值，在输出端会将 null 值转成该字符串。</td>
    </tr>
    <tr>
      <td><h5>csv.write-bigdecimal-in-scientific-notation</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>设置将 Bigdecimal 类型的数据表示为科学计数法（默认为true，即需要转为科学计数法），例如一个BigDecimal的值为100000，设置true，结果为 '1E+5'；设置为false，结果为 100000。注意：只有当值不等于0且是10的倍数才会转为科学计数法。</td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前 CSV 的 schema 都是从 table schema 推断而来的。显式地定义 CSV schema 暂不支持。
Flink 的 CSV Format 数据使用 [jackson databind API](https://github.com/FasterXML/jackson-databind) 去解析 CSV 字符串。

下面的表格列出了flink数据和CSV数据的对应关系。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL 类型</th>
        <th class="text-left">CSV 类型</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>INTERVAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>
